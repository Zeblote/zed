use collections::HashMap;
use fs::MTime;
use project::{
    Entry, EntryKind, FileLineCount, ProjectLineCount, ProjectLineCountFingerprint, WorktreeId,
};
use std::sync::Arc;
use sum_tree::{Bias, ContextLessSummary, Dimension, KeyedItem, SumTree};
use util::rel_path::RelPath;
use worktree::{PathKey, PathProgress, PathSummary, PathTarget};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EntryFingerprint {
    mtime: Option<MTime>,
    size: u64,
    inode: u64,
}

impl EntryFingerprint {
    fn for_entry(entry: &Entry) -> Self {
        Self {
            mtime: entry.mtime,
            size: entry.size,
            inode: entry.inode,
        }
    }

    fn matches(self, fingerprint: ProjectLineCountFingerprint) -> bool {
        self.size == fingerprint.size
            && self.inode == fingerprint.inode
            && self.mtime.is_none_or(|mtime| mtime == fingerprint.mtime)
    }
}

#[derive(Clone, Debug)]
pub struct LineCountRequest {
    pub worktree_id: WorktreeId,
    pub path: Arc<RelPath>,
    pub fingerprint: EntryFingerprint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LineCountState {
    Pending,
    Text(u64),
    Binary,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Default)]
struct LineCountAggregate {
    lines: u64,
    pending_files: u32,
}

impl LineCountAggregate {
    fn add(&mut self, other: &Self) {
        self.lines = self.lines.saturating_add(other.lines);
        self.pending_files = self.pending_files.saturating_add(other.pending_files);
    }

    fn resolved_line_count(self) -> Option<u64> {
        (self.pending_files == 0).then_some(self.lines)
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct LineCountSummary {
    all: LineCountAggregate,
    non_ignored: LineCountAggregate,
    non_hidden: LineCountAggregate,
    visible: LineCountAggregate,
}

impl ContextLessSummary for LineCountSummary {
    fn zero() -> Self {
        Self::default()
    }

    fn add_summary(&mut self, summary: &Self) {
        self.all.add(&summary.all);
        self.non_ignored.add(&summary.non_ignored);
        self.non_hidden.add(&summary.non_hidden);
        self.visible.add(&summary.visible);
    }
}

impl<'a> Dimension<'a, PathSummary<LineCountSummary>> for LineCountSummary {
    fn zero(_: ()) -> Self {
        Self::default()
    }

    fn add_summary(&mut self, summary: &'a PathSummary<LineCountSummary>, _: ()) {
        ContextLessSummary::add_summary(self, &summary.item_summary);
    }
}

#[derive(Clone, Debug)]
struct LineCountEntry {
    path: Arc<RelPath>,
    fingerprint: Option<EntryFingerprint>,
    state: LineCountState,
    is_ignored: bool,
    is_hidden: bool,
}

impl LineCountEntry {
    fn pending_file(entry: &Entry) -> Self {
        Self {
            path: entry.path.clone(),
            fingerprint: Some(EntryFingerprint::for_entry(entry)),
            state: LineCountState::Pending,
            is_ignored: entry.is_ignored,
            is_hidden: entry.is_hidden,
        }
    }

    fn pending_directory(entry: &Entry) -> Self {
        Self {
            path: entry.path.clone(),
            fingerprint: None,
            state: LineCountState::Pending,
            is_ignored: entry.is_ignored,
            is_hidden: entry.is_hidden,
        }
    }

    fn aggregate(&self) -> LineCountAggregate {
        match self.state {
            LineCountState::Pending => LineCountAggregate {
                pending_files: 1,
                ..Default::default()
            },
            LineCountState::Text(lines) => LineCountAggregate {
                lines,
                ..Default::default()
            },
            LineCountState::Binary | LineCountState::Unavailable => LineCountAggregate::default(),
        }
    }
}

impl sum_tree::Item for LineCountEntry {
    type Summary = PathSummary<LineCountSummary>;

    fn summary(&self, _: ()) -> Self::Summary {
        let aggregate = self.aggregate();
        PathSummary {
            max_path: self.path.clone(),
            item_summary: LineCountSummary {
                all: aggregate,
                non_ignored: if self.is_ignored {
                    Default::default()
                } else {
                    aggregate
                },
                non_hidden: if self.is_hidden {
                    Default::default()
                } else {
                    aggregate
                },
                visible: if self.is_ignored || self.is_hidden {
                    Default::default()
                } else {
                    aggregate
                },
            },
        }
    }
}

impl KeyedItem for LineCountEntry {
    type Key = PathKey;

    fn key(&self) -> Self::Key {
        PathKey(self.path.clone())
    }
}

#[derive(Default)]
pub struct LineCountCache {
    worktrees: HashMap<WorktreeId, SumTree<LineCountEntry>>,
}

impl LineCountCache {
    pub fn clear(&mut self) {
        self.worktrees.clear();
    }

    pub fn remove_worktree(&mut self, worktree_id: WorktreeId) {
        self.worktrees.remove(&worktree_id);
    }

    pub fn reset_worktree<'a>(
        &mut self,
        worktree_id: WorktreeId,
        entries: impl IntoIterator<Item = &'a Entry>,
    ) -> Vec<LineCountRequest> {
        let mut line_count_entries = entries
            .into_iter()
            .filter_map(|entry| {
                if entry.is_file() && !entry.is_fifo {
                    Some(LineCountEntry::pending_file(entry))
                } else if entry.kind == EntryKind::PendingDir {
                    Some(LineCountEntry::pending_directory(entry))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        line_count_entries.sort_unstable_by(|left, right| left.path.cmp(&right.path));
        let requests = line_count_entries
            .iter()
            .filter_map(|entry| {
                Some(LineCountRequest {
                    worktree_id,
                    path: entry.path.clone(),
                    fingerprint: entry.fingerprint?,
                })
            })
            .collect();
        self.worktrees
            .insert(worktree_id, SumTree::from_iter(line_count_entries, ()));
        requests
    }

    pub fn mark_pending(
        &mut self,
        worktree_id: WorktreeId,
        entry: &Entry,
    ) -> Option<LineCountRequest> {
        if entry.kind == EntryKind::PendingDir {
            self.worktrees
                .entry(worktree_id)
                .or_insert_with(|| SumTree::new(()))
                .insert_or_replace(LineCountEntry::pending_directory(entry), ());
            return None;
        }
        if !entry.is_file() || entry.is_fifo {
            self.remove(worktree_id, &entry.path);
            return None;
        }
        let pending = LineCountEntry::pending_file(entry);
        let request = LineCountRequest {
            worktree_id,
            path: pending.path.clone(),
            fingerprint: pending.fingerprint?,
        };
        let entries = self
            .worktrees
            .entry(worktree_id)
            .or_insert_with(|| SumTree::new(()));
        if let Some(existing) = entries.get(&PathKey(pending.path.clone()), ())
            && existing.fingerprint == pending.fingerprint
            && matches!(
                existing.state,
                LineCountState::Text(_) | LineCountState::Binary
            )
        {
            let state = existing.state;
            entries.insert_or_replace(LineCountEntry { state, ..pending }, ());
            return None;
        }
        entries.insert_or_replace(pending, ());
        Some(request)
    }

    pub fn remove(&mut self, worktree_id: WorktreeId, path: &Arc<RelPath>) {
        if let Some(entries) = self.worktrees.get_mut(&worktree_id) {
            entries.remove(&PathKey(path.clone()), ());
        }
    }

    pub fn remove_subtree(&mut self, worktree_id: WorktreeId, path: &Arc<RelPath>) {
        let Some(entries) = self.worktrees.get_mut(&worktree_id) else {
            return;
        };
        let mut cursor = entries.cursor::<PathProgress>(());
        let mut retained = cursor.slice(&PathTarget::Path(path), Bias::Left);
        cursor.slice(&PathTarget::Successor(path), Bias::Left);
        retained.append(cursor.suffix(), ());
        drop(cursor);
        *entries = retained;
    }

    pub fn apply(&mut self, request: &LineCountRequest, result: Option<&ProjectLineCount>) -> bool {
        let Some(entries) = self.worktrees.get_mut(&request.worktree_id) else {
            return false;
        };
        let key = PathKey(request.path.clone());
        let Some(current) = entries.get(&key, ()).cloned() else {
            return false;
        };
        if current.fingerprint != Some(request.fingerprint) {
            return false;
        }
        // Any result that doesn't match the requested fingerprint (or is
        // missing entirely) becomes `Unavailable` rather than staying
        // `Pending`, so that one unreadable file cannot suppress the counts of
        // its ancestor directories forever. If the mismatch was caused by the
        // file changing on disk, the corresponding worktree update re-marks the
        // entry as pending with its new fingerprint.
        let state = match result {
            Some(result) => match result.fingerprint {
                Some(fingerprint) if request.fingerprint.matches(fingerprint) => match result.count
                {
                    Some(FileLineCount::Text(lines)) => LineCountState::Text(lines),
                    Some(FileLineCount::Binary) => LineCountState::Binary,
                    None => LineCountState::Unavailable,
                },
                _ => LineCountState::Unavailable,
            },
            None => LineCountState::Unavailable,
        };
        if current.state == state {
            return false;
        }
        entries.insert_or_replace(LineCountEntry { state, ..current }, ());
        true
    }

    pub fn line_count(
        &self,
        worktree_id: WorktreeId,
        entry: &Entry,
        hide_ignored: bool,
        hide_hidden: bool,
    ) -> Option<u64> {
        let entries = self.worktrees.get(&worktree_id)?;
        if entry.is_file() {
            let cached = entries.get(&PathKey(entry.path.clone()), ())?;
            return match cached.state {
                LineCountState::Text(lines) => Some(lines),
                _ => None,
            };
        }

        let mut cursor = entries.cursor::<PathProgress>(());
        cursor.seek(&PathTarget::Path(&entry.path), Bias::Left);
        let summary: LineCountSummary =
            cursor.summary(&PathTarget::Successor(&entry.path), Bias::Left);
        match (hide_ignored, hide_hidden) {
            (false, false) => summary.all,
            (true, false) => summary.non_ignored,
            (false, true) => summary.non_hidden,
            (true, true) => summary.visible,
        }
        .resolved_line_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{Context as _, Result};
    use project::EntryKind;

    fn entry(path: &str, kind: EntryKind, is_ignored: bool, is_hidden: bool) -> Result<Entry> {
        Ok(Entry {
            id: project::ProjectEntryId::from_usize(path.len() + 1),
            kind,
            path: RelPath::from_unix_str(path)?.into(),
            inode: path.len() as u64,
            mtime: Some(MTime::from_seconds_and_nanos(path.len() as u64 + 1, 0)),
            canonical_path: None,
            is_ignored,
            is_hidden,
            is_always_included: false,
            is_external: false,
            is_private: false,
            size: path.len() as u64,
            char_bag: Default::default(),
            is_fifo: false,
        })
    }

    #[test]
    fn aggregates_directory_line_counts() -> Result<()> {
        let worktree_id = WorktreeId::from_usize(1);
        let root = entry("", EntryKind::Dir, false, false)?;
        let source = entry("src", EntryKind::Dir, false, false)?;
        let entries = [
            entry("visible.rs", EntryKind::File, false, false)?,
            entry("ignored.rs", EntryKind::File, true, false)?,
            entry("hidden.rs", EntryKind::File, false, true)?,
            entry("src/nested.rs", EntryKind::File, false, false)?,
            entry("unloaded", EntryKind::UnloadedDir, false, false)?,
            entry("scanning", EntryKind::PendingDir, false, false)?,
        ];
        let mut cache = LineCountCache::default();
        let requests = cache.reset_worktree(worktree_id, entries.iter());
        assert_eq!(cache.line_count(worktree_id, &root, false, false), None);

        for request in &requests {
            let lines = match request.path.as_unix_str() {
                "visible.rs" => 10,
                "ignored.rs" => 20,
                "hidden.rs" => 30,
                "src/nested.rs" => 5,
                path => anyhow::bail!("unexpected path {path}"),
            };
            let fingerprint = request.fingerprint;
            assert!(cache.apply(
                request,
                Some(&ProjectLineCount {
                    path: request.path.clone(),
                    count: Some(FileLineCount::Text(lines)),
                    fingerprint: Some(ProjectLineCountFingerprint {
                        mtime: fingerprint.mtime.context("missing mtime")?,
                        size: fingerprint.size,
                        inode: fingerprint.inode,
                    }),
                })
            ));
        }

        assert_eq!(cache.line_count(worktree_id, &root, false, false), None);
        assert!(
            cache
                .mark_pending(
                    worktree_id,
                    &entry("scanning", EntryKind::Dir, false, false)?,
                )
                .is_none()
        );
        assert_eq!(cache.line_count(worktree_id, &root, false, false), Some(65));
        assert_eq!(cache.line_count(worktree_id, &root, true, false), Some(45));
        assert_eq!(cache.line_count(worktree_id, &root, false, true), Some(35));
        assert_eq!(cache.line_count(worktree_id, &root, true, true), Some(15));
        assert_eq!(
            cache.line_count(worktree_id, &source, false, false),
            Some(5)
        );
        Ok(())
    }

    #[test]
    fn unreadable_files_do_not_block_directory_totals() -> Result<()> {
        let worktree_id = WorktreeId::from_usize(1);
        let root = entry("", EntryKind::Dir, false, false)?;
        let entries = [
            entry("readable.rs", EntryKind::File, false, false)?,
            entry("unreadable.rs", EntryKind::File, false, false)?,
        ];
        let mut cache = LineCountCache::default();
        let requests = cache.reset_worktree(worktree_id, entries.iter());
        assert_eq!(cache.line_count(worktree_id, &root, false, false), None);

        for request in &requests {
            let result = match request.path.as_unix_str() {
                "readable.rs" => Some(ProjectLineCount {
                    path: request.path.clone(),
                    count: Some(FileLineCount::Text(7)),
                    fingerprint: Some(ProjectLineCountFingerprint {
                        mtime: request.fingerprint.mtime.context("missing mtime")?,
                        size: request.fingerprint.size,
                        inode: request.fingerprint.inode,
                    }),
                }),
                _ => None,
            };
            assert!(cache.apply(request, result.as_ref()));
        }

        assert_eq!(cache.line_count(worktree_id, &root, false, false), Some(7));
        assert_eq!(
            cache.line_count(
                worktree_id,
                &entry("unreadable.rs", EntryKind::File, false, false)?,
                false,
                false,
            ),
            None
        );
        Ok(())
    }

    #[test]
    fn ignores_stale_results() -> Result<()> {
        let worktree_id = WorktreeId::from_usize(1);
        let mut file = entry("file.rs", EntryKind::File, false, false)?;
        let mut cache = LineCountCache::default();
        let old_request = cache
            .reset_worktree(worktree_id, [&file])
            .pop()
            .context("missing request")?;
        file.size += 1;
        cache
            .mark_pending(worktree_id, &file)
            .context("missing updated request")?;

        assert!(!cache.apply(
            &old_request,
            Some(&ProjectLineCount {
                path: old_request.path.clone(),
                count: Some(FileLineCount::Text(10)),
                fingerprint: Some(ProjectLineCountFingerprint {
                    mtime: old_request.fingerprint.mtime.context("missing mtime")?,
                    size: old_request.fingerprint.size,
                    inode: old_request.fingerprint.inode,
                }),
            })
        ));
        assert_eq!(cache.line_count(worktree_id, &file, false, false), None);
        Ok(())
    }
}
