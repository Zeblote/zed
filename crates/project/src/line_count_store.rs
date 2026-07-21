use crate::{FileLineCount, ProjectLineCount, ProjectLineCountFingerprint};
use anyhow::{Context as _, Result, anyhow};
use collections::{HashMap, HashSet};
use fs::{Fs, MTime, Metadata};
use futures::StreamExt as _;
use parking_lot::Mutex;
use sqlez::{connection::Connection, statement::Statement};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};
use util::{ResultExt as _, rel_path::RelPath};
use worktree::Snapshot;

const CACHE_RETENTION_SECONDS: i64 = 90 * 24 * 60 * 60;
const ACCESS_REFRESH_SECONDS: i64 = 24 * 60 * 60;
const LINE_COUNT_CONCURRENCY: usize = 8;
static LINE_COUNT_SEMAPHORE: LazyLock<async_lock::Semaphore> =
    LazyLock::new(|| async_lock::Semaphore::new(LINE_COUNT_CONCURRENCY));

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileFingerprint {
    mtime: MTime,
    size: u64,
    inode: u64,
}

impl FileFingerprint {
    fn from_metadata(metadata: Metadata) -> Option<Self> {
        if metadata.is_dir || metadata.is_fifo {
            return None;
        }
        metadata.mtime.to_seconds_and_nanos_for_persistence()?;
        Some(Self {
            mtime: metadata.mtime,
            size: metadata.len,
            inode: metadata.inode,
        })
    }

    fn as_project_fingerprint(self) -> ProjectLineCountFingerprint {
        ProjectLineCountFingerprint {
            mtime: self.mtime,
            size: self.size,
            inode: self.inode,
        }
    }
}

#[derive(Clone, Debug)]
pub struct LineCountCandidate {
    path: Arc<RelPath>,
    abs_path: PathBuf,
    fingerprint: Option<FileFingerprint>,
}

impl LineCountCandidate {
    pub fn from_snapshot(snapshot: &Snapshot, path: Arc<RelPath>) -> Option<Self> {
        let entry = snapshot.entry_for_path(&path)?;
        if !entry.is_file() || entry.is_fifo {
            return None;
        }

        let fingerprint = entry.mtime.and_then(|mtime| {
            mtime.to_seconds_and_nanos_for_persistence()?;
            Some(FileFingerprint {
                mtime,
                size: entry.size,
                inode: entry.inode,
            })
        });

        Some(Self {
            abs_path: snapshot.absolutize(&path),
            path,
            fingerprint,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CachedLineCount {
    fingerprint: FileFingerprint,
    count: FileLineCount,
}

struct LoadedWorktree {
    entries: HashMap<Arc<RelPath>, CachedLineCount>,
    last_accessed: i64,
    data_version: i64,
}

struct LineCountStoreInner {
    connection: Connection,
    worktrees: HashMap<Arc<Path>, LoadedWorktree>,
}

struct LineCountStore {
    inner: Mutex<LineCountStoreInner>,
}

impl LineCountStore {
    fn open(database_path: &Path) -> Result<Self> {
        let parent = database_path
            .parent()
            .context("line count cache database has no parent directory")?;
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating line count cache directory {parent:?}"))?;

        let connection = Connection::open_file(database_path.to_string_lossy().as_ref());
        anyhow::ensure!(
            connection.persistent(),
            "failed to open persistent line count cache at {database_path:?}"
        );
        connection.exec("PRAGMA journal_mode=WAL")?()?;
        connection.exec("PRAGMA synchronous=NORMAL")?()?;
        connection.exec("PRAGMA busy_timeout=5000")?()?;
        connection.exec("PRAGMA foreign_keys=ON")?()?;
        connection.exec(
            "CREATE TABLE IF NOT EXISTS line_count_worktrees (
                    root BLOB PRIMARY KEY NOT NULL,
                    last_accessed INTEGER NOT NULL
                ) STRICT",
        )?()?;
        connection.exec(
            "CREATE TABLE IF NOT EXISTS line_count_files (
                    root BLOB NOT NULL,
                    path TEXT NOT NULL,
                    mtime_seconds INTEGER NOT NULL,
                    mtime_nanos INTEGER NOT NULL,
                    size INTEGER NOT NULL,
                    inode INTEGER NOT NULL,
                    line_count INTEGER,
                    is_binary INTEGER NOT NULL,
                    PRIMARY KEY(root, path),
                    FOREIGN KEY(root) REFERENCES line_count_worktrees(root) ON DELETE CASCADE,
                    CHECK((is_binary = 1 AND line_count IS NULL) OR
                          (is_binary = 0 AND line_count IS NOT NULL))
                ) STRICT",
        )?()?;

        let cutoff = unix_timestamp().saturating_sub(CACHE_RETENTION_SECONDS);
        connection
            .exec_bound::<i64>("DELETE FROM line_count_worktrees WHERE last_accessed < ?")?(
            cutoff,
        )?;

        Ok(Self {
            inner: Mutex::new(LineCountStoreInner {
                connection,
                worktrees: HashMap::default(),
            }),
        })
    }

    fn lookup(
        &self,
        worktree_root: Arc<Path>,
        candidates: &[LineCountCandidate],
    ) -> Result<HashMap<Arc<RelPath>, FileLineCount>> {
        let mut inner = self.inner.lock();
        load_worktree(&mut inner, worktree_root.clone())?;

        let entries = &inner
            .worktrees
            .get(&worktree_root)
            .context("line count worktree was not loaded")?
            .entries;
        Ok(candidates
            .iter()
            .filter_map(|candidate| {
                let fingerprint = candidate.fingerprint?;
                let cached = entries.get(&candidate.path)?;
                (cached.fingerprint == fingerprint)
                    .then_some((candidate.path.clone(), cached.count))
            })
            .collect())
    }

    fn store(
        &self,
        worktree_root: Arc<Path>,
        counts: &[(LineCountCandidate, FileLineCount)],
    ) -> Result<()> {
        if counts.is_empty() {
            return Ok(());
        }

        let mut inner = self.inner.lock();
        load_worktree(&mut inner, worktree_root.clone())?;
        run_transaction(&inner.connection, |connection| {
            let mut statement = Statement::prepare(
                connection,
                "INSERT INTO line_count_files(
                    root, path, mtime_seconds, mtime_nanos, size, inode, line_count, is_binary
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(root, path) DO UPDATE SET
                    mtime_seconds = excluded.mtime_seconds,
                    mtime_nanos = excluded.mtime_nanos,
                    size = excluded.size,
                    inode = excluded.inode,
                    line_count = excluded.line_count,
                    is_binary = excluded.is_binary",
            )?;
            for (candidate, count) in counts {
                let Some(fingerprint) = candidate.fingerprint else {
                    continue;
                };
                let (mtime_seconds, mtime_nanos) = fingerprint
                    .mtime
                    .to_seconds_and_nanos_for_persistence()
                    .context("line count fingerprint mtime predates the Unix epoch")?;
                let line_count = match count {
                    FileLineCount::Text(lines) => Some(*lines),
                    FileLineCount::Binary => None,
                };
                let is_binary = matches!(count, FileLineCount::Binary);
                let mut next_index = statement.bind(&worktree_root, 1)?;
                next_index = statement.bind(&candidate.path.as_unix_str(), next_index)?;
                next_index = statement.bind(&mtime_seconds, next_index)?;
                next_index = statement.bind(&mtime_nanos, next_index)?;
                next_index = statement.bind(&fingerprint.size, next_index)?;
                next_index = statement.bind(&fingerprint.inode, next_index)?;
                next_index = statement.bind(&line_count, next_index)?;
                statement.bind(&is_binary, next_index)?;
                statement.exec()?;
            }
            Ok(())
        })?;

        let entries = &mut inner
            .worktrees
            .get_mut(&worktree_root)
            .context("line count worktree was not loaded")?
            .entries;
        for (candidate, count) in counts {
            if let Some(fingerprint) = candidate.fingerprint {
                entries.insert(
                    candidate.path.clone(),
                    CachedLineCount {
                        fingerprint,
                        count: *count,
                    },
                );
            }
        }
        Ok(())
    }

    fn reconcile(
        &self,
        worktree_root: Arc<Path>,
        valid_paths: HashSet<Arc<RelPath>>,
    ) -> Result<()> {
        let mut inner = self.inner.lock();
        load_worktree(&mut inner, worktree_root.clone())?;
        let stale_paths = inner
            .worktrees
            .get(&worktree_root)
            .context("line count worktree was not loaded")?
            .entries
            .keys()
            .filter(|path| !valid_paths.contains(*path))
            .cloned()
            .collect::<Vec<_>>();
        if stale_paths.is_empty() {
            return Ok(());
        }

        run_transaction(&inner.connection, |connection| {
            let mut statement = Statement::prepare(
                connection,
                "DELETE FROM line_count_files WHERE root = ? AND path = ?",
            )?;
            for path in &stale_paths {
                let next_index = statement.bind(&worktree_root, 1)?;
                statement.bind(&path.as_unix_str(), next_index)?;
                statement.exec()?;
            }
            Ok(())
        })?;
        let entries = &mut inner
            .worktrees
            .get_mut(&worktree_root)
            .context("line count worktree was not loaded")?
            .entries;
        for path in stale_paths {
            entries.remove(&path);
        }
        Ok(())
    }
}

fn load_worktree(inner: &mut LineCountStoreInner, worktree_root: Arc<Path>) -> Result<()> {
    let now = unix_timestamp();
    let data_version = inner.connection.select_row::<i64>("PRAGMA data_version")?()?
        .context("SQLite did not return a data version")?;
    let must_reload = inner
        .worktrees
        .get(&worktree_root)
        .is_none_or(|worktree| worktree.data_version != data_version);
    let must_refresh_access = inner.worktrees.get(&worktree_root).is_none_or(|worktree| {
        now.saturating_sub(worktree.last_accessed) >= ACCESS_REFRESH_SECONDS
    });

    if must_refresh_access {
        inner.connection.exec_bound::<(Arc<Path>, i64)>(
            "INSERT INTO line_count_worktrees(root, last_accessed) VALUES (?, ?)
             ON CONFLICT(root) DO UPDATE SET last_accessed = excluded.last_accessed",
        )?((worktree_root.clone(), now))?;
    }

    if must_reload {
        let rows = inner
            .connection
            .select_bound::<Arc<Path>, (String, i64, u32, u64, u64, Option<u64>, bool)>(
                "SELECT path, mtime_seconds, mtime_nanos, size, inode, line_count, is_binary
                 FROM line_count_files WHERE root = ?",
            )?(worktree_root.clone())?;
        let mut entries = HashMap::default();
        for (path, mtime_seconds, mtime_nanos, size, inode, line_count, is_binary) in rows {
            let path: Arc<RelPath> = RelPath::from_unix_str(&path)?.into();
            let mtime_seconds = u64::try_from(mtime_seconds)
                .context("line count cache contains a negative mtime")?;
            let count = if is_binary {
                FileLineCount::Binary
            } else {
                FileLineCount::Text(line_count.context("text line count cache entry has no count")?)
            };
            entries.insert(
                path,
                CachedLineCount {
                    fingerprint: FileFingerprint {
                        mtime: MTime::from_seconds_and_nanos(mtime_seconds, mtime_nanos),
                        size,
                        inode,
                    },
                    count,
                },
            );
        }
        inner.worktrees.insert(
            worktree_root,
            LoadedWorktree {
                entries,
                last_accessed: now,
                data_version,
            },
        );
    } else if must_refresh_access && let Some(worktree) = inner.worktrees.get_mut(&worktree_root) {
        worktree.last_accessed = now;
    }
    Ok(())
}

fn run_transaction(
    connection: &Connection,
    operation: impl FnOnce(&Connection) -> Result<()>,
) -> Result<()> {
    connection.exec("BEGIN IMMEDIATE")?()?;
    match operation(connection) {
        Ok(()) => match connection.exec("COMMIT")?() {
            Ok(()) => Ok(()),
            Err(error) => {
                if let Err(rollback_error) = connection.exec("ROLLBACK")?() {
                    log::error!(
                        "failed to roll back line count cache transaction: {rollback_error:#}"
                    );
                }
                Err(error).context("committing line count cache transaction")
            }
        },
        Err(error) => {
            if let Err(rollback_error) = connection.exec("ROLLBACK")?() {
                log::error!("failed to roll back line count cache transaction: {rollback_error:#}");
            }
            Err(error)
        }
    }
}

fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
        .unwrap_or(0)
}

fn global_line_count_store() -> Result<&'static LineCountStore> {
    static STORE: OnceLock<Result<LineCountStore, String>> = OnceLock::new();
    match STORE.get_or_init(|| {
        LineCountStore::open(&paths::temp_dir().join("line_counts").join("v2.sqlite3"))
            .map_err(|error| format!("{error:#}"))
    }) {
        Ok(store) => Ok(store),
        Err(error) => Err(anyhow!(error.clone())),
    }
}

pub async fn count_lines(
    fs: Arc<dyn Fs>,
    worktree_root: Arc<Path>,
    candidates: Vec<LineCountCandidate>,
    valid_paths: Option<Vec<Arc<RelPath>>>,
) -> Vec<ProjectLineCount> {
    let worktree_root = match fs.canonicalize(&worktree_root).await {
        Ok(worktree_root) => Arc::from(worktree_root),
        Err(error) => {
            log::error!(
                "failed to canonicalize line count cache root {worktree_root:?}: {error:#}"
            );
            worktree_root
        }
    };
    if let Some(valid_paths) = valid_paths
        && let Ok(store) = global_line_count_store()
        && let Err(error) =
            store.reconcile(worktree_root.clone(), valid_paths.into_iter().collect())
    {
        log::error!("failed to reconcile line count cache: {error:#}");
    }

    let cached_counts = match global_line_count_store() {
        Ok(store) => store
            .lookup(worktree_root.clone(), &candidates)
            .log_err()
            .unwrap_or_default(),
        Err(error) => {
            log::error!("failed to initialize line count cache: {error:#}");
            HashMap::default()
        }
    };

    let mut results = Vec::with_capacity(candidates.len());
    let mut misses = Vec::new();
    for candidate in candidates {
        if let Some(count) = cached_counts.get(&candidate.path) {
            results.push(ProjectLineCount {
                path: candidate.path,
                count: Some(*count),
                fingerprint: candidate
                    .fingerprint
                    .map(FileFingerprint::as_project_fingerprint),
            });
        } else {
            misses.push(candidate);
        }
    }

    let counted = futures::stream::iter(misses)
        .map(|candidate| {
            let fs = fs.clone();
            async move {
                let _permit = LINE_COUNT_SEMAPHORE.acquire().await;
                let before = fs
                    .metadata(&candidate.abs_path)
                    .await
                    .log_err()
                    .flatten()
                    .and_then(FileFingerprint::from_metadata);
                let Some(before) = before else {
                    return (candidate, None, None);
                };
                if candidate.fingerprint.is_some() && Some(before) != candidate.fingerprint {
                    return (candidate, None, Some(before));
                }
                let count = worktree::count_file_lines(fs.as_ref(), &candidate.abs_path)
                    .await
                    .log_err();
                let after = fs
                    .metadata(&candidate.abs_path)
                    .await
                    .log_err()
                    .flatten()
                    .and_then(FileFingerprint::from_metadata);
                if Some(before) != after {
                    return (candidate, None, after);
                }
                (candidate, count, Some(before))
            }
        })
        .buffer_unordered(LINE_COUNT_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

    let cacheable = counted
        .iter()
        .filter_map(|(candidate, count, fingerprint)| {
            let count = (*count)?;
            let fingerprint = (*fingerprint)?;
            candidate.fingerprint?;
            Some((
                LineCountCandidate {
                    fingerprint: Some(fingerprint),
                    ..candidate.clone()
                },
                count,
            ))
        })
        .collect::<Vec<_>>();
    if let Ok(store) = global_line_count_store()
        && let Err(error) = store.store(worktree_root, &cacheable)
    {
        log::error!("failed to persist line counts: {error:#}");
    }

    results.extend(
        counted
            .into_iter()
            .map(|(candidate, count, fingerprint)| ProjectLineCount {
                path: candidate.path,
                count,
                fingerprint: fingerprint.map(FileFingerprint::as_project_fingerprint),
            }),
    );
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persists_counts_and_invalidates_changed_fingerprints() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let database_path = temp_dir.path().join("line-counts.sqlite3");
        let worktree_root: Arc<Path> = temp_dir.path().join("worktree").into();
        let path: Arc<RelPath> = RelPath::from_unix_str("src/main.rs")?.into();
        let candidate = LineCountCandidate {
            path: path.clone(),
            abs_path: worktree_root.join("src/main.rs"),
            fingerprint: Some(FileFingerprint {
                mtime: MTime::from_seconds_and_nanos(100, 200),
                size: 300,
                inode: 400,
            }),
        };

        {
            let store = LineCountStore::open(&database_path)?;
            assert!(
                store
                    .lookup(worktree_root.clone(), std::slice::from_ref(&candidate))?
                    .is_empty()
            );
            store.store(
                worktree_root.clone(),
                &[(candidate.clone(), FileLineCount::Text(42))],
            )?;
        }

        let store = LineCountStore::open(&database_path)?;
        assert_eq!(
            store
                .lookup(worktree_root.clone(), std::slice::from_ref(&candidate))?
                .get(&path),
            Some(&FileLineCount::Text(42))
        );

        let changed_candidate = LineCountCandidate {
            fingerprint: Some(FileFingerprint {
                size: 301,
                ..candidate
                    .fingerprint
                    .context("candidate has no fingerprint")?
            }),
            ..candidate.clone()
        };
        assert!(
            store
                .lookup(worktree_root, &[changed_candidate])?
                .is_empty()
        );
        store.reconcile(temp_dir.path().join("worktree").into(), HashSet::default())?;
        assert!(
            store
                .lookup(
                    temp_dir.path().join("worktree").into(),
                    std::slice::from_ref(&candidate),
                )?
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn persists_binary_classification() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let database_path = temp_dir.path().join("line-counts.sqlite3");
        let worktree_root: Arc<Path> = temp_dir.path().join("worktree").into();
        let path: Arc<RelPath> = RelPath::from_unix_str("image.dat")?.into();
        let candidate = LineCountCandidate {
            path: path.clone(),
            abs_path: worktree_root.join("image.dat"),
            fingerprint: Some(FileFingerprint {
                mtime: MTime::from_seconds_and_nanos(100, 200),
                size: 16,
                inode: 2,
            }),
        };
        let store = LineCountStore::open(&database_path)?;
        store.lookup(worktree_root.clone(), std::slice::from_ref(&candidate))?;
        store.store(
            worktree_root.clone(),
            &[(candidate.clone(), FileLineCount::Binary)],
        )?;

        let reopened = LineCountStore::open(&database_path)?;
        assert_eq!(
            reopened.lookup(worktree_root, &[candidate])?.get(&path),
            Some(&FileLineCount::Binary)
        );
        Ok(())
    }
}
