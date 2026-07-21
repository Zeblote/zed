use collections::{HashMap, HashSet};
use fs::MTime;
use project::{
    Entry, EntryKind, FileLineCount, ProjectLineCount, ProjectLineCountFingerprint, WorktreeId,
};
use std::{collections::BTreeMap, sync::Arc};
use sum_tree::{Bias, ContextLessSummary, Dimension, Edit, KeyedItem, SumTree};
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
    pub path: Arc<RelPath>,
    pub fingerprint: EntryFingerprint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LineCountState {
    Pending,
    Unscanned,
    Text(u64),
    Binary,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Default)]
struct LineCountAggregate {
    lines: u64,
    incomplete_entries: u32,
}

impl LineCountAggregate {
    fn add(&mut self, other: &Self) {
        self.lines = self.lines.saturating_add(other.lines);
        self.incomplete_entries = self
            .incomplete_entries
            .saturating_add(other.incomplete_entries);
    }

    fn resolved_line_count(self) -> Option<LineCountTotal> {
        let is_partial = self.incomplete_entries != 0;
        if is_partial && self.lines == 0 {
            return None;
        }
        Some(LineCountTotal {
            lines: self.lines,
            is_partial,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LineCountTotal {
    pub lines: u64,
    pub is_partial: bool,
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

fn is_unscanned_directory(entry: &Entry) -> bool {
    matches!(entry.kind, EntryKind::PendingDir | EntryKind::UnloadedDir)
}

// Any result that doesn't match the requested fingerprint (or is missing
// entirely) becomes `Unavailable` rather than staying `Pending`, so that one
// unreadable file cannot suppress the counts of its ancestor directories
// forever. If the mismatch was caused by the file changing on disk, the
// corresponding worktree update re-marks the entry as pending with its new
// fingerprint.
fn resolved_state(request: &LineCountRequest, result: Option<&ProjectLineCount>) -> LineCountState {
    match result {
        Some(result) => match result.fingerprint {
            Some(fingerprint) if request.fingerprint.matches(fingerprint) => match result.count {
                Some(FileLineCount::Text(lines)) => LineCountState::Text(lines),
                Some(FileLineCount::Binary) => LineCountState::Binary,
                None => LineCountState::Unavailable,
            },
            _ => LineCountState::Unavailable,
        },
        None => LineCountState::Unavailable,
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
            state: if entry.kind == EntryKind::UnloadedDir {
                LineCountState::Unscanned
            } else {
                LineCountState::Pending
            },
            is_ignored: entry.is_ignored,
            is_hidden: entry.is_hidden,
        }
    }

    fn aggregate(&self) -> LineCountAggregate {
        match self.state {
            LineCountState::Pending | LineCountState::Unscanned | LineCountState::Unavailable => {
                LineCountAggregate {
                    incomplete_entries: 1,
                    ..Default::default()
                }
            }
            LineCountState::Text(lines) => LineCountAggregate {
                lines,
                ..Default::default()
            },
            LineCountState::Binary => LineCountAggregate::default(),
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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LineCountFilter {
    included: HashSet<String>,
    excluded: HashSet<String>,
}

impl LineCountFilter {
    pub fn new(included: &str, excluded: &str) -> Self {
        Self {
            included: parse_extensions(included),
            excluded: parse_extensions(excluded),
        }
    }

    fn includes(&self, path: &RelPath) -> bool {
        if self.included.is_empty() && self.excluded.is_empty() {
            return true;
        }
        let extension = path.extension();
        if extension.is_some_and(|extension| contains_extension(&self.excluded, extension)) {
            return false;
        }
        self.included.is_empty()
            || extension.is_some_and(|extension| contains_extension(&self.included, extension))
    }
}

fn parse_extensions(value: &str) -> HashSet<String> {
    value
        .split(',')
        .map(|extension| {
            extension
                .trim()
                .trim_start_matches('.')
                .to_ascii_lowercase()
        })
        .filter(|extension| !extension.is_empty())
        .collect()
}

fn contains_extension(extensions: &HashSet<String>, extension: &str) -> bool {
    extensions.contains(extension) || extensions.contains(extension.to_ascii_lowercase().as_str())
}

#[derive(Clone, Default)]
pub struct LineCountCache {
    worktrees: HashMap<WorktreeId, SumTree<LineCountEntry>>,
    filter: LineCountFilter,
}

impl LineCountCache {
    pub fn new(filter: LineCountFilter) -> Self {
        Self {
            worktrees: HashMap::default(),
            filter,
        }
    }

    fn counts_file(&self, entry: &Entry) -> bool {
        entry.is_file() && !entry.is_fifo && self.filter.includes(&entry.path)
    }

    pub fn remove_worktree(&mut self, worktree_id: WorktreeId) {
        self.worktrees.remove(&worktree_id);
    }

    pub fn reset_worktree<'a>(
        &mut self,
        worktree_id: WorktreeId,
        entries: impl IntoIterator<Item = &'a Entry>,
    ) -> Vec<LineCountRequest> {
        let mut requests = Vec::new();
        let previous = self.worktrees.get(&worktree_id);
        let entries = entries
            .into_iter()
            .filter_map(|entry| {
                if self.counts_file(entry) {
                    let mut pending = LineCountEntry::pending_file(entry);
                    let reusable = previous
                        .and_then(|tree| tree.get(&PathKey(entry.path.clone()), ()))
                        .filter(|old| old.fingerprint == pending.fingerprint);
                    if let Some(old) = reusable {
                        pending.state = old.state;
                    } else if let Some(fingerprint) = pending.fingerprint {
                        requests.push(LineCountRequest {
                            path: entry.path.clone(),
                            fingerprint,
                        });
                    }
                    Some(pending)
                } else if is_unscanned_directory(entry) {
                    Some(LineCountEntry::pending_directory(entry))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        self.worktrees
            .insert(worktree_id, SumTree::from_iter(entries, ()));
        requests
    }

    fn apply_edits(&mut self, worktree_id: WorktreeId, edits: Vec<Edit<LineCountEntry>>) {
        if edits.is_empty() {
            return;
        }
        self.worktrees
            .entry(worktree_id)
            .or_insert_with(|| SumTree::new(()))
            .edit(edits, ());
    }

    pub fn mark_pending_many<'a>(
        &mut self,
        worktree_id: WorktreeId,
        entries: impl IntoIterator<Item = &'a Entry>,
    ) -> Vec<LineCountRequest> {
        let mut edits = Vec::new();
        let mut requests = Vec::new();
        let existing = self.worktrees.get(&worktree_id);
        for entry in entries {
            if is_unscanned_directory(entry) {
                edits.push(Edit::Insert(LineCountEntry::pending_directory(entry)));
                continue;
            }
            if !self.counts_file(entry) {
                edits.push(Edit::Remove(PathKey(entry.path.clone())));
                continue;
            }
            let pending = LineCountEntry::pending_file(entry);
            let reusable = existing
                .and_then(|entries| entries.get(&PathKey(pending.path.clone()), ()))
                .filter(|current| current.fingerprint == pending.fingerprint)
                .filter(|current| {
                    matches!(
                        current.state,
                        LineCountState::Pending | LineCountState::Text(_) | LineCountState::Binary
                    )
                })
                .map(|current| current.state);
            match (reusable, pending.fingerprint) {
                (Some(state), _) => edits.push(Edit::Insert(LineCountEntry { state, ..pending })),
                (None, Some(fingerprint)) => {
                    requests.push(LineCountRequest {
                        path: pending.path.clone(),
                        fingerprint,
                    });
                    edits.push(Edit::Insert(pending));
                }
                (None, None) => edits.push(Edit::Insert(pending)),
            }
        }
        self.apply_edits(worktree_id, edits);
        requests
    }

    pub fn apply_many<'a>(
        &mut self,
        worktree_id: WorktreeId,
        results: impl IntoIterator<Item = (&'a LineCountRequest, Option<&'a ProjectLineCount>)>,
    ) -> bool {
        let Some(entries) = self.worktrees.get(&worktree_id) else {
            return false;
        };
        let mut edits = Vec::new();
        for (request, result) in results {
            let Some(current) = entries.get(&PathKey(request.path.clone()), ()) else {
                continue;
            };
            if current.fingerprint != Some(request.fingerprint)
                || current.state != LineCountState::Pending
            {
                continue;
            }
            let state = resolved_state(request, result);
            if current.state == state {
                continue;
            }
            edits.push(Edit::Insert(LineCountEntry {
                state,
                ..current.clone()
            }));
        }
        let changed = !edits.is_empty();
        self.apply_edits(worktree_id, edits);
        changed
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

    pub fn line_count(
        &self,
        worktree_id: WorktreeId,
        entry: &Entry,
        hide_ignored: bool,
        hide_hidden: bool,
    ) -> Option<LineCountTotal> {
        let entries = self.worktrees.get(&worktree_id)?;
        if entry.is_file() {
            let cached = entries.get(&PathKey(entry.path.clone()), ())?;
            return match cached.state {
                LineCountState::Text(lines) => Some(LineCountTotal {
                    lines,
                    is_partial: false,
                }),
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

pub struct LineCountUpdate {
    snapshot: worktree::Snapshot,
    changes: Option<Vec<project::UpdatedEntriesSet>>,
    changed_entries: usize,
}

impl LineCountUpdate {
    pub fn new(snapshot: worktree::Snapshot) -> Self {
        Self {
            snapshot,
            changes: None,
            changed_entries: 0,
        }
    }

    pub fn incremental(snapshot: worktree::Snapshot, changes: &project::UpdatedEntriesSet) -> Self {
        let mut update = Self {
            snapshot: snapshot.clone(),
            changes: Some(Vec::new()),
            changed_entries: 0,
        };
        update.merge(snapshot, changes);
        update
    }

    pub fn merge(&mut self, snapshot: worktree::Snapshot, changes: &project::UpdatedEntriesSet) {
        self.snapshot = snapshot;
        self.changed_entries = self.changed_entries.saturating_add(changes.len());
        // Bound event retention during checkouts. A snapshot is cheap and supersedes the queued deltas.
        if self.changed_entries > 32768 {
            self.changes = None;
        } else if let Some(pending) = &mut self.changes {
            pending.push(changes.clone());
        }
    }
}

pub struct LineCountBatch {
    pub id: usize,
    pub worktree_id: WorktreeId,
    pub paths: Vec<Arc<RelPath>>,
}

pub struct LineCountWorker {
    pub cache: LineCountCache,
    pending: BTreeMap<(WorktreeId, Arc<RelPath>), LineCountRequest>,
    active: HashMap<usize, (WorktreeId, HashMap<Arc<RelPath>, LineCountRequest>)>,
    next_batch: usize,
}

impl LineCountWorker {
    pub fn new(filter: LineCountFilter) -> Self {
        Self {
            cache: LineCountCache::new(filter),
            pending: BTreeMap::new(),
            active: HashMap::default(),
            next_batch: 0,
        }
    }

    pub fn update(&mut self, updates: HashMap<WorktreeId, Option<LineCountUpdate>>) -> Vec<usize> {
        let mut cancelled = Vec::new();
        for (worktree_id, update) in updates {
            let Some(update) = update else {
                self.cache.remove_worktree(worktree_id);
                self.pending.retain(|(id, _), _| *id != worktree_id);
                self.active.retain(|id, (worktree, _)| {
                    if *worktree == worktree_id {
                        cancelled.push(*id);
                        false
                    } else {
                        true
                    }
                });
                continue;
            };
            let requests = if let Some(changes) = update.changes {
                let paths = changes
                    .iter()
                    .flat_map(|changes| changes.iter().map(|(path, _, _)| path.clone()))
                    .collect::<collections::BTreeSet<_>>();
                let mut entries = Vec::new();
                for path in paths {
                    if let Some(entry) = update.snapshot.entry_for_path(&path) {
                        entries.push(entry);
                    } else {
                        self.cache.remove_subtree(worktree_id, &path);
                        self.pending.remove(&(worktree_id, path));
                    }
                }
                self.cache.mark_pending_many(worktree_id, entries)
            } else {
                let requests = self
                    .cache
                    .reset_worktree(worktree_id, update.snapshot.entries(true, 0));
                self.pending.retain(|(id, path), _| {
                    *id != worktree_id || update.snapshot.entry_for_path(path).is_some()
                });
                requests
            };
            for request in requests {
                self.pending
                    .insert((worktree_id, request.path.clone()), request);
            }
        }
        cancelled
    }

    pub fn complete(&mut self, batch_id: usize, results: Option<Vec<ProjectLineCount>>) {
        if let Some(results) = results {
            let Some((worktree_id, requests)) = self.active.get_mut(&batch_id) else {
                return;
            };
            let paired = results
                .into_iter()
                .filter_map(|result| {
                    requests
                        .remove(&result.path)
                        .map(|request| (request, result))
                })
                .collect::<Vec<_>>();
            self.cache.apply_many(
                *worktree_id,
                paired
                    .iter()
                    .map(|(request, result)| (request, Some(result))),
            );
        } else if let Some((worktree_id, requests)) = self.active.remove(&batch_id) {
            self.cache.apply_many(
                worktree_id,
                requests.values().map(|request| (request, None)),
            );
        }
    }

    pub fn batches(&mut self) -> Vec<LineCountBatch> {
        let mut batches = Vec::new();
        while self.active.len() < 4 {
            let Some(((worktree_id, _), _)) = self.pending.first_key_value() else {
                break;
            };
            let worktree_id = *worktree_id;
            let mut requests = HashMap::default();
            while requests.len() < 2048
                && self
                    .pending
                    .first_key_value()
                    .is_some_and(|((id, _), _)| *id == worktree_id)
            {
                let Some((_, request)) = self.pending.pop_first() else {
                    break;
                };
                let current = self
                    .cache
                    .worktrees
                    .get(&worktree_id)
                    .and_then(|tree| tree.get(&PathKey(request.path.clone()), ()));
                if current.is_some_and(|entry| {
                    entry.state == LineCountState::Pending
                        && entry.fingerprint == Some(request.fingerprint)
                }) {
                    requests.insert(request.path.clone(), request);
                }
            }
            if requests.is_empty() {
                continue;
            }
            let id = self.next_batch;
            self.next_batch += 1;
            let paths = requests.keys().cloned().collect();
            self.active.insert(id, (worktree_id, requests));
            batches.push(LineCountBatch {
                id,
                worktree_id,
                paths,
            });
        }
        batches
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{Context as _, Result};
    use project::EntryKind;

    fn exact(lines: u64) -> LineCountTotal {
        LineCountTotal {
            lines,
            is_partial: false,
        }
    }

    fn partial(lines: u64) -> LineCountTotal {
        LineCountTotal {
            lines,
            is_partial: true,
        }
    }

    fn text_result(request: &LineCountRequest, lines: u64) -> Result<ProjectLineCount> {
        Ok(ProjectLineCount {
            path: request.path.clone(),
            count: Some(FileLineCount::Text(lines)),
            fingerprint: Some(ProjectLineCountFingerprint {
                mtime: request.fingerprint.mtime.context("missing mtime")?,
                size: request.fingerprint.size,
                inode: request.fingerprint.inode,
            }),
        })
    }

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
            entry("hidden.rs", EntryKind::File, false, true)?,
            entry("ignored.rs", EntryKind::File, true, false)?,
            entry("scanning", EntryKind::PendingDir, false, false)?,
            entry("src/nested.rs", EntryKind::File, false, false)?,
            entry("unloaded", EntryKind::UnloadedDir, false, false)?,
            entry("visible.rs", EntryKind::File, false, false)?,
        ];
        let mut cache = LineCountCache::default();
        let requests = cache.reset_worktree(worktree_id, entries.iter());
        assert_eq!(cache.line_count(worktree_id, &root, false, false), None);

        let mut counted = Vec::new();
        for request in &requests {
            let lines = match request.path.as_unix_str() {
                "visible.rs" => 10,
                "ignored.rs" => 20,
                "hidden.rs" => 30,
                "src/nested.rs" => 5,
                path => anyhow::bail!("unexpected path {path}"),
            };
            counted.push(text_result(request, lines)?);
        }
        assert!(cache.apply_many(worktree_id, requests.iter().zip(counted.iter().map(Some))));

        assert_eq!(
            cache.line_count(worktree_id, &root, false, false),
            Some(partial(65))
        );
        assert!(
            cache
                .mark_pending_many(
                    worktree_id,
                    [&entry("scanning", EntryKind::Dir, false, false)?],
                )
                .is_empty()
        );
        assert_eq!(
            cache.line_count(worktree_id, &root, false, false),
            Some(partial(65))
        );
        assert!(
            cache
                .mark_pending_many(
                    worktree_id,
                    [&entry("unloaded", EntryKind::Dir, false, false)?],
                )
                .is_empty()
        );
        assert_eq!(
            cache.line_count(worktree_id, &root, false, false),
            Some(exact(65))
        );
        assert_eq!(
            cache.line_count(worktree_id, &root, true, false),
            Some(exact(45))
        );
        assert_eq!(
            cache.line_count(worktree_id, &root, false, true),
            Some(exact(35))
        );
        assert_eq!(
            cache.line_count(worktree_id, &root, true, true),
            Some(exact(15))
        );
        assert_eq!(
            cache.line_count(worktree_id, &source, false, false),
            Some(exact(5))
        );
        Ok(())
    }

    #[test]
    fn extension_filter_selects_counted_files() -> Result<()> {
        let worktree_id = WorktreeId::from_usize(1);
        let root = entry("", EntryKind::Dir, false, false)?;
        let entries = [
            entry("Makefile", EntryKind::File, false, false)?,
            entry("data.JSON", EntryKind::File, false, false)?,
            entry("main.rs", EntryKind::File, false, false)?,
            entry("notes.md", EntryKind::File, false, false)?,
        ];

        let mut cache = LineCountCache::new(LineCountFilter::new("rs, .md , json", "json"));
        let requests = cache.reset_worktree(worktree_id, entries.iter());
        let counted = requests
            .iter()
            .map(|request| request.path.as_unix_str())
            .collect::<Vec<_>>();
        // `Makefile` has no extension, and `json` is excluded despite also being
        // included and differing in case.
        assert_eq!(counted, ["main.rs", "notes.md"]);

        let results = requests
            .iter()
            .map(|request| text_result(request, 3))
            .collect::<Result<Vec<_>>>()?;
        assert!(cache.apply_many(worktree_id, requests.iter().zip(results.iter().map(Some))));
        assert_eq!(
            cache.line_count(worktree_id, &root, false, false),
            Some(exact(6))
        );

        let mut cache = LineCountCache::new(LineCountFilter::default());
        assert_eq!(cache.reset_worktree(worktree_id, entries.iter()).len(), 4);
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

        let mut results = Vec::new();
        for request in &requests {
            let result = match request.path.as_unix_str() {
                "readable.rs" => Some(text_result(request, 7)?),
                _ => None,
            };
            results.push(result);
        }
        assert!(cache.apply_many(
            worktree_id,
            requests.iter().zip(results.iter().map(Option::as_ref))
        ));

        assert_eq!(
            cache.line_count(worktree_id, &root, false, false),
            Some(partial(7))
        );
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
    fn deduplicates_pending_requests_and_preserves_completed_results() -> Result<()> {
        let worktree_id = WorktreeId::from_usize(1);
        let file = entry("file.rs", EntryKind::File, false, false)?;
        let mut cache = LineCountCache::default();
        let requests = cache.reset_worktree(worktree_id, [&file]);
        assert_eq!(requests.len(), 1);
        assert!(cache.mark_pending_many(worktree_id, [&file]).is_empty());
        assert!(cache.reset_worktree(worktree_id, [&file]).is_empty());
        let request = requests.first().context("missing request")?;
        let result = text_result(request, 4)?;
        assert!(cache.apply_many(worktree_id, [(request, Some(&result))]));
        assert!(!cache.apply_many(worktree_id, [(request, None)]));
        assert_eq!(
            cache.line_count(worktree_id, &file, false, false),
            Some(exact(4))
        );
        Ok(())
    }

    #[test]
    fn streams_results_and_reuses_finished_slots_before_slow_batches() -> Result<()> {
        let worktree_id = WorktreeId::from_usize(1);
        let entries = (0..10000)
            .map(|index| entry(&format!("{index:05}.rs"), EntryKind::File, false, false))
            .collect::<Result<Vec<_>>>()?;
        let mut worker = LineCountWorker::new(LineCountFilter::default());
        for request in worker.cache.reset_worktree(worktree_id, entries.iter()) {
            worker
                .pending
                .insert((worktree_id, request.path.clone()), request);
        }
        let batches = worker.batches();
        assert_eq!(batches.len(), 4);
        assert!(worker.batches().is_empty());
        let first = batches.first().context("missing batch")?;
        let request = worker
            .active
            .get(&first.id)
            .and_then(|(_, requests)| requests.values().next())
            .context("missing request")?
            .clone();
        let result = text_result(&request, 7)?;
        worker.complete(first.id, Some(vec![result]));
        let file = entries
            .iter()
            .find(|entry| entry.path == request.path)
            .context("missing file")?;
        assert_eq!(
            worker.cache.line_count(worktree_id, file, false, false),
            Some(exact(7))
        );
        let last = batches.last().context("missing batch")?;
        worker.complete(last.id, None);
        assert_eq!(worker.batches().len(), 1);
        assert!(worker.active.contains_key(&first.id));

        let cancelled = worker.update(HashMap::from_iter([(worktree_id, None)]));
        assert_eq!(cancelled.len(), 4);
        assert!(worker.batches().is_empty());
        assert_eq!(
            worker.cache.line_count(worktree_id, file, false, false),
            None
        );
        for request in worker.cache.reset_worktree(worktree_id, [file]) {
            worker
                .pending
                .insert((worktree_id, request.path.clone()), request);
        }
        let empty_snapshot = worktree::Snapshot::new(
            worktree_id,
            RelPath::from_unix_str("root")?.into(),
            std::path::Path::new("/root").into(),
            util::paths::PathStyle::Unix,
        );
        worker.update(HashMap::from_iter([(
            worktree_id,
            Some(LineCountUpdate::new(empty_snapshot)),
        )]));
        assert!(worker.pending.is_empty());
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
        assert_eq!(cache.mark_pending_many(worktree_id, [&file]).len(), 1);

        let stale = text_result(&old_request, 10)?;
        assert!(!cache.apply_many(worktree_id, [(&old_request, Some(&stale))]));
        assert_eq!(cache.line_count(worktree_id, &file, false, false), None);
        Ok(())
    }
}
