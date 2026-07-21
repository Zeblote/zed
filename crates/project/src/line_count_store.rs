use crate::{FileLineCount, ProjectLineCount, ProjectLineCountFingerprint};
use anyhow::{Context as _, Result, anyhow};
use fs::Fs;
use futures::{SinkExt, StreamExt, channel::mpsc, stream::BoxStream};
use gpui::BackgroundExecutor;
use parking_lot::Mutex;
use sqlez::{connection::Connection, statement::Statement};
use std::{
    path::Path,
    sync::{Arc, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};
use util::{ResultExt as _, rel_path::RelPath};
use worktree::Snapshot;

const RESULT_CHUNK_SIZE: usize = 128;
const READ_CONCURRENCY: usize = 16;

struct LineCountStore(Mutex<Connection>);

impl LineCountStore {
    fn open(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path.parent().context("cache path has no parent")?)?;
        let connection = Connection::open_file(path.to_string_lossy().as_ref());
        anyhow::ensure!(connection.persistent(), "could not open line count cache");
        connection.exec(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA cache_size=-8192;
             CREATE TABLE IF NOT EXISTS counts (
                 root BLOB NOT NULL, path TEXT NOT NULL,
                 seconds INTEGER NOT NULL, nanos INTEGER NOT NULL,
                 size INTEGER NOT NULL, inode INTEGER NOT NULL,
                 lines INTEGER, updated_at INTEGER NOT NULL,
                 PRIMARY KEY(root, path)
             ) STRICT, WITHOUT ROWID",
        )?()?;
        connection.exec_bound::<i64>("DELETE FROM counts WHERE updated_at < ?")?(
            timestamp().saturating_sub(90 * 24 * 60 * 60),
        )?;
        Ok(Self(Mutex::new(connection)))
    }

    fn lookup(&self, root: &Arc<Path>, entries: &mut [ProjectLineCount]) -> Result<()> {
        let connection = self.0.lock();
        connection.exec("BEGIN")?()?;
        let rollback = util::defer(|| {
            connection
                .exec("ROLLBACK")
                .and_then(|mut query| query())
                .log_err();
        });
        let mut statement = Statement::prepare(
            &connection,
            "SELECT lines FROM counts
             WHERE root = ? AND path = ? AND seconds = ? AND nanos = ? AND size = ? AND inode = ?",
        )?;
        for entry in entries {
            let Some(fingerprint) = entry.fingerprint else {
                continue;
            };
            let Some((seconds, nanos)) = fingerprint.mtime.to_seconds_and_nanos_for_persistence()
            else {
                continue;
            };
            statement.bind(
                &(
                    root.clone(),
                    entry.path.as_unix_str(),
                    seconds,
                    nanos,
                    fingerprint.size,
                    fingerprint.inode,
                ),
                1,
            )?;
            if let Some(lines) = statement.maybe_row::<Option<u64>>()? {
                entry.count = Some(lines.map_or(FileLineCount::Binary, FileLineCount::Text));
            }
        }
        connection.exec("COMMIT")?()?;
        rollback.abort();
        Ok(())
    }

    fn store(&self, root: &Arc<Path>, entries: &[ProjectLineCount]) -> Result<()> {
        let connection = self.0.lock();
        connection.exec("BEGIN IMMEDIATE")?()?;
        let result = (|| {
            let mut statement = Statement::prepare(
                &connection,
                "INSERT INTO counts VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(root, path) DO UPDATE SET
                 seconds=excluded.seconds, nanos=excluded.nanos, size=excluded.size,
                 inode=excluded.inode, lines=excluded.lines, updated_at=excluded.updated_at",
            )?;
            let now = timestamp();
            for entry in entries {
                let (Some(count), Some(fingerprint)) = (entry.count, entry.fingerprint) else {
                    continue;
                };
                let Some((seconds, nanos)) =
                    fingerprint.mtime.to_seconds_and_nanos_for_persistence()
                else {
                    continue;
                };
                let lines = match count {
                    FileLineCount::Text(lines) => Some(lines),
                    FileLineCount::Binary => None,
                };
                statement.bind(
                    &(
                        root.clone(),
                        entry.path.as_unix_str(),
                        seconds,
                        nanos,
                        fingerprint.size,
                        fingerprint.inode,
                        lines,
                        now,
                    ),
                    1,
                )?;
                statement.exec()?;
            }
            connection.exec("COMMIT")?()?;
            Ok(())
        })();
        if result.is_err() {
            connection.exec("ROLLBACK")?().log_err();
        }
        result
    }
}

fn timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |time| time.as_secs() as i64)
}

fn store() -> Result<&'static LineCountStore> {
    static STORE: OnceLock<Result<LineCountStore, String>> = OnceLock::new();
    STORE
        .get_or_init(|| {
            // v2 could persist incorrect UTF-16 counts with otherwise valid fingerprints.
            LineCountStore::open(&paths::temp_dir().join("line_counts/v3.sqlite3"))
                .map_err(|error| format!("{error:#}"))
        })
        .as_ref()
        .map_err(|error| anyhow!(error.clone()))
}

pub fn count_lines(
    fs: Arc<dyn Fs>,
    snapshot: Snapshot,
    paths: Vec<Arc<RelPath>>,
    executor: &BackgroundExecutor,
) -> BoxStream<'static, Result<Vec<ProjectLineCount>>> {
    let (mut sender, receiver) = mpsc::channel(1);
    let task = executor.spawn(async move {
        let result: Result<()> = async {
            let root: Arc<Path> = fs.canonicalize(snapshot.abs_path()).await?.into();
            let (cached, missing) = smol::unblock({
                let root = root.clone();
                move || {
                    let mut entries = paths
                        .into_iter()
                        .map(|path| {
                            let fingerprint = snapshot
                                .entry_for_path(&path)
                                .filter(|entry| entry.is_file() && !entry.is_fifo)
                                .and_then(|entry| {
                                    Some(ProjectLineCountFingerprint {
                                        mtime: entry.mtime?,
                                        size: entry.size,
                                        inode: entry.inode,
                                    })
                                });
                            ProjectLineCount {
                                path,
                                count: None,
                                fingerprint,
                            }
                        })
                        .collect::<Vec<_>>();
                    match store() {
                        Ok(store) => {
                            store.lookup(&root, &mut entries).log_err();
                        }
                        Err(error) => log::error!("line count cache unavailable: {error:#}"),
                    }
                    entries.into_iter().partition::<Vec<_>, _>(|entry| {
                        entry.count.is_some() || entry.fingerprint.is_none()
                    })
                }
            })
            .await;
            for chunk in cached.chunks(RESULT_CHUNK_SIZE) {
                if sender.send(Ok(chunk.to_vec())).await.is_err() {
                    return Ok(());
                }
            }
            let mut completed = futures::stream::iter(missing)
                .map(|mut entry| {
                    let fs = fs.clone();
                    let root = root.clone();
                    async move {
                        let path = root.join(entry.path.as_unix_str());
                        match worktree::count_file_lines(fs.as_ref(), &path).await {
                            Ok(count) => {
                                let after = fs.metadata(&path).await.log_err().flatten();
                                if let (Some(expected), Some(after)) = (entry.fingerprint, after) {
                                    if expected.mtime == after.mtime
                                        && expected.size == after.len
                                        && expected.inode == after.inode
                                    {
                                        entry.count = Some(count);
                                    }
                                }
                            }
                            Err(error) => {
                                log::debug!("could not count lines in {path:?}: {error:#}")
                            }
                        }
                        entry
                    }
                })
                .buffer_unordered(READ_CONCURRENCY)
                .ready_chunks(RESULT_CHUNK_SIZE);
            let mut pending_writes = Vec::with_capacity(RESULT_CHUNK_SIZE);
            while let Some(entries) = completed.next().await {
                if sender.send(Ok(entries.clone())).await.is_err() {
                    return Ok(());
                }
                pending_writes.extend(entries.into_iter().filter(|entry| entry.count.is_some()));
                if pending_writes.len() < RESULT_CHUNK_SIZE {
                    continue;
                }
                let root = root.clone();
                let entries = std::mem::take(&mut pending_writes);
                smol::unblock(move || {
                    if let Ok(store) = store() {
                        store.store(&root, &entries).log_err();
                    }
                })
                .await;
            }
            if !pending_writes.is_empty() {
                let root = root.clone();
                smol::unblock(move || {
                    if let Ok(store) = store() {
                        store.store(&root, &pending_writes).log_err();
                    }
                })
                .await;
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            if sender.send(Err(error)).await.is_err() {
                return;
            }
        }
    });
    receiver
        .map(move |result| {
            let _keep_worker_alive = &task;
            result
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs::MTime;

    #[test]
    fn cache_reopens_and_rejects_changed_fingerprints() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("counts.sqlite3");
        let root: Arc<Path> = directory.path().into();
        let mut entry = ProjectLineCount {
            path: RelPath::from_unix_str("file.rs")?.into(),
            fingerprint: Some(ProjectLineCountFingerprint {
                mtime: MTime::from_seconds_and_nanos(1, 2),
                size: 30,
                inode: 40,
            }),
            count: Some(FileLineCount::Text(3)),
        };
        LineCountStore::open(&path)?.store(&root, &[entry.clone()])?;
        entry.count = None;
        let store = LineCountStore::open(&path)?;
        store.lookup(&root, std::slice::from_mut(&mut entry))?;
        assert_eq!(entry.count, Some(FileLineCount::Text(3)));
        entry
            .fingerprint
            .as_mut()
            .context("missing fingerprint")?
            .size += 1;
        entry.count = None;
        store.lookup(&root, std::slice::from_mut(&mut entry))?;
        assert_eq!(entry.count, None);
        Ok(())
    }
}
