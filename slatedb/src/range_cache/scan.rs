//! Direct scan path for cached ranges.
//!
//! [`CachedScan`] merges cursors over the base file, visible write buffers, and the range's delta
//! view at one sequence number. It skips tombstones, resolves merge operands via the normal stack
//! for that single key, and copies small page spans so returned rows never keep file mappings
//! alive. Adjacent ranges are scanned sequentially at the same sequence number.

use std::collections::VecDeque;
use std::io;
use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::ops::RangeBounds;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use super::buffer::{Cursor, Kind, Version};
use super::delta::{past_end, to_row, Delta};
use super::file::RowSpan;
use super::view::DeltaView;
use super::Layers;
use crate::bytes_range::BytesRange;
use crate::error::SlateDBError;
use crate::iter::RowEntryIterator;
use crate::merge_operator::MergeOperatorType;
use crate::range_cache::file::RangeFileCursor;
use crate::types::{RowEntry, ValueDeletable};

/// Buffer `i` of `layers`, in the order of [`Layers::buffers`]: 0 is the active buffer.
fn buffer_of(layers: &Layers, i: usize) -> &Delta {
    match i {
        0 => &layers.active,
        _ => &layers.sealed[i - 1],
    }
}

/// A cursor over the versions of a buffer that are visible at one sequence number. It is a
/// position in the buffer: the scan reads it through the layers that it holds, so the cursor
/// takes no reference of its own and reads the buffer in place.
struct DeltaCursor {
    /// The buffer, in the order of [`Layers::buffers`].
    index: usize,
    pos: Cursor,
    seq: u64,
    /// `pos` is at the newest visible version of its key, which the scan has not used yet.
    peeked: bool,
    /// The cursor passed the end of the part.
    done: bool,
}

impl DeltaCursor {
    fn new(index: usize, delta: &Delta, start: Bound<&[u8]>, seq: u64) -> Self {
        Self {
            index,
            pos: delta.buffer().seek(start),
            seq,
            peeked: false,
            done: false,
        }
    }

    /// Moves to the newest visible version of the next key, unless the cursor is at one. A buffer
    /// holds the versions of a key newest first, so the first version at or below `seq` is the
    /// newest visible one.
    fn fill(&mut self, delta: &Delta, end: &Bound<Bytes>) {
        let buffer = delta.buffer();
        while !self.peeked && !self.done {
            match buffer.version(&self.pos) {
                None => self.done = true,
                Some(version) if past_end(end, version.key) => self.done = true,
                Some(version) if version.seq <= self.seq => self.peeked = true,
                Some(_) => buffer.advance(&mut self.pos),
            }
        }
    }

    /// The version that `fill` found.
    fn version<'a>(&self, delta: &'a Delta) -> Option<Version<'a>> {
        if self.peeked {
            delta.buffer().version(&self.pos)
        } else {
            None
        }
    }

    fn key<'a>(&self, delta: &'a Delta) -> Option<&'a [u8]> {
        self.version(delta).map(|version| version.key)
    }

    /// Leaves `key` if the cursor is at one of its versions, visible or not.
    fn skip_key(&mut self, delta: &Delta, key: &[u8]) {
        if self.done {
            return;
        }
        let buffer = delta.buffer();
        let mut moved = false;
        while buffer.version(&self.pos).is_some_and(|v| v.key == key) {
            buffer.advance(&mut self.pos);
            moved = true;
        }
        if moved {
            self.peeked = false;
        }
    }

    /// Moves forward to the first key at or after `key`. It never moves backwards.
    fn seek(&mut self, delta: &Delta, key: &[u8]) {
        if self.done {
            return;
        }
        let buffer = delta.buffer();
        if buffer.version(&self.pos).is_some_and(|v| v.key < key) {
            self.pos = buffer.seek(Included(key));
            self.peeked = false;
        }
    }
}

/// How many page copies a view cursor keeps: so many runs can interleave in a scan before a copy
/// that still holds rows for it is replaced.
const PAGE_COPIES: usize = 4;
/// How far a view cursor looks ahead in the view for the rows of a page that it copies, at most.
const LOOKAHEAD: usize = 64;
/// How far a view cursor looks ahead for its first copy of a run. It doubles with each next copy
/// of the run.
const FIRST_LOOKAHEAD: usize = 4;

/// Bytes of one page of a run that a view cursor copied: the file from offset `at`.
struct PageCopy {
    slot: u8,
    at: u64,
    bytes: Bytes,
    /// The cursor's count of uses when it last used this copy. The least recently used copy is
    /// the one replaced, unless the run has a copy.
    used: u64,
    /// How far the cursor looked ahead for this copy.
    lookahead: usize,
}

impl PageCopy {
    /// True if the copy holds `[start, end)` of the file of `slot`, or, without an end, the rest
    /// of the page from `start` (positional reads copy pages to their end).
    fn holds(&self, slot: u8, start: u64, end: Option<u64>) -> bool {
        let copy_end = self.at + self.bytes.len() as u64;
        self.slot == slot && self.at <= start && end.map_or(start < copy_end, |e| e <= copy_end)
    }
}

/// The key of a view cursor's version: a span of a mapped run file, or a slice of a copy
/// (positional reads, whose files the cursor cannot borrow).
enum ViewKey {
    None,
    Mapped { slot: u8, at: usize, len: usize },
    Copied(Bytes),
}

/// Cursor over all runs of a range via its delta view, caching small copied page spans.
struct ViewCursor {
    /// The entry of the view.
    pos: usize,
    seq: u64,
    /// `pos` is at the newest visible version of its key, which the scan has not used yet.
    peeked: bool,
    /// The cursor passed the end of the part, or of the view.
    done: bool,
    /// The row and the key of the peeked version.
    row: Option<RowSpan>,
    key: ViewKey,
    copies: [Option<PageCopy>; PAGE_COPIES],
    uses: u64,
}

/// The view of `layers`. A view cursor exists only for layers with a view.
fn view_of(layers: &Layers) -> &DeltaView {
    layers
        .view
        .as_deref()
        .expect("a view cursor on layers without a view")
}

impl ViewCursor {
    fn new(view: &DeltaView, start: Bound<&[u8]>, seq: u64) -> io::Result<Self> {
        Ok(Self {
            pos: view.seek(start)?,
            seq,
            peeked: false,
            done: false,
            row: None,
            key: ViewKey::None,
            copies: Default::default(),
            uses: 0,
        })
    }

    /// Returns a cached page copy covering `[start, end)` for entry `i`'s run, copying ahead up
    /// to [`DeltaView::page_mates`] (or page end for positional reads) on a miss.
    fn copy_for(&mut self, view: &DeltaView, i: usize, end: Option<u64>) -> io::Result<&PageCopy> {
        self.uses += 1;
        let (slot, start) = (view.slot_of(i), view.offset(i));
        let found = self
            .copies
            .iter()
            .position(|c| c.as_ref().is_some_and(|c| c.holds(slot, start, end)));
        let at = match found {
            Some(at) => at,
            None => {
                let file = &view.run_in(slot).file;
                let page_end = file.page_end(start)?;
                let own = self
                    .copies
                    .iter()
                    .position(|c| c.as_ref().is_some_and(|c| c.slot == slot));
                let lookahead = match own.and_then(|at| self.copies[at].as_ref()) {
                    Some(copy) => (copy.lookahead * 2).min(LOOKAHEAD),
                    None => FIRST_LOOKAHEAD,
                };
                let copy_end = match (file.mapped(), end) {
                    (Some(_), Some(end)) => {
                        match view.page_mates(i, page_end, lookahead, PAGE_COPIES - 1) {
                            last if last == i => end,
                            last => view.row(last)?.end.max(end),
                        }
                    }
                    _ => page_end,
                };
                let bytes = file.copy_at(start, (copy_end - start) as usize)?;
                let at = own.unwrap_or_else(|| {
                    (0..PAGE_COPIES)
                        .min_by_key(|&c| self.copies[c].as_ref().map_or(0, |c| c.used))
                        .unwrap_or(0)
                });
                self.copies[at] = Some(PageCopy {
                    slot,
                    at: start,
                    bytes,
                    used: 0,
                    lookahead,
                });
                at
            }
        };
        let copy = self.copies[at]
            .as_mut()
            .ok_or_else(|| io::Error::other("range cache view cursor lost a copy"))?;
        copy.used = self.uses;
        Ok(copy)
    }

    /// Moves to the newest visible version of the next key, unless the cursor is at one. Versions
    /// of runs whose interval is above `seq` are skipped without a read; the first remaining one at
    /// or below `seq` is the newest visible version of its key.
    fn fill(&mut self, view: &DeltaView, end: &Bound<Bytes>) -> io::Result<()> {
        while !self.peeked && !self.done {
            if self.pos >= view.len() {
                self.done = true;
                break;
            }
            let run = view.run_of(self.pos);
            if run.lo >= self.seq {
                self.pos += 1;
                continue;
            }
            let (row, key) = match run.file.mapped() {
                Some(map) => {
                    let row = run.file.row_at(view.offset(self.pos))?;
                    let (at, len) = row.key;
                    let at = usize::try_from(at).map_err(|_| io::Error::other("key offset"))?;
                    if map.len() < at + len {
                        return Err(io::Error::other("range cache view: key past the file"));
                    }
                    let slot = view.slot_of(self.pos);
                    (row, ViewKey::Mapped { slot, at, len })
                }
                None => {
                    let offset = view.offset(self.pos);
                    let copy = self.copy_for(view, self.pos, None)?;
                    let row = RowSpan::parse(&copy.bytes, copy.at, offset)?;
                    let key = row.key_in(&copy.bytes, copy.at, &run.file)?;
                    (row, ViewKey::Copied(key))
                }
            };
            if run.hi > self.seq && row.seq > self.seq {
                self.pos += 1;
                continue;
            }
            self.row = Some(row);
            self.key = key;
            if self.key(view).is_none_or(|key| past_end(end, key)) {
                self.done = true;
                self.row = None;
                self.key = ViewKey::None;
                break;
            }
            self.peeked = true;
        }
        Ok(())
    }

    /// The key of the version that `fill` found, or of the version it is checking.
    fn key<'a>(&'a self, view: &'a DeltaView) -> Option<&'a [u8]> {
        match &self.key {
            ViewKey::Mapped { slot, at, len } => view
                .run_in(*slot)
                .file
                .mapped()
                .and_then(|map| map.get(*at..*at + *len)),
            ViewKey::Copied(key) => Some(key),
            ViewKey::None => None,
        }
    }

    /// The key of the peeked version for a step: borrowed from the mapping, or the copy.
    fn step_key<'a>(&self, view: &'a DeltaView) -> Option<StepKey<'a>> {
        if !self.peeked {
            return None;
        }
        match &self.key {
            ViewKey::Mapped { slot, at, len } => view
                .run_in(*slot)
                .file
                .mapped()
                .and_then(|map| map.get(*at..*at + *len))
                .map(StepKey::Buffer),
            ViewKey::Copied(key) => Some(StepKey::Run(key.clone())),
            ViewKey::None => None,
        }
    }

    /// Leaves the current key: moves to the first entry of the next key.
    fn leave(&mut self, view: &DeltaView) {
        self.pos = view.next_key(self.pos);
        self.peeked = false;
        self.row = None;
        self.key = ViewKey::None;
    }

    /// The version that `fill` found, as slices of a page copy, then leaves its key.
    fn take(&mut self, view: &DeltaView) -> io::Result<RowEntry> {
        let row = self
            .row
            .ok_or_else(|| io::Error::other("range cache view cursor has no current row"))?;
        let pos = self.pos;
        let copy = self.copy_for(view, pos, Some(row.end))?;
        let entry = row.entry_in(&copy.bytes, copy.at, &view.run_of(pos).file)?;
        self.leave(view);
        Ok(entry)
    }

    /// Leaves `key` if the cursor is at one of its versions, visible or not.
    fn skip_key(&mut self, view: &DeltaView, key: &[u8]) -> io::Result<()> {
        if self.done || self.pos >= view.len() {
            return Ok(());
        }
        let here = match self.peeked {
            true => self.key(view) == Some(key),
            false => view.key(self.pos)?.as_ref() == key,
        };
        if here {
            self.leave(view);
        }
        Ok(())
    }

    /// Moves forward to the first key at or after `key`. It never moves backwards.
    fn seek(&mut self, view: &DeltaView, key: &[u8]) -> io::Result<()> {
        if self.done || (self.peeked && self.key(view).is_some_and(|current| current >= key)) {
            return Ok(());
        }
        let to = view.seek(Included(key))?;
        if to > self.pos {
            self.pos = to;
            self.peeked = false;
            self.row = None;
            self.key = ViewKey::None;
        }
        Ok(())
    }
}

/// The key of the version that a step takes, borrowed from a buffer or a mapped file, or held by
/// a copy.
enum StepKey<'a> {
    Buffer(&'a [u8]),
    Run(Bytes),
}

impl StepKey<'_> {
    fn as_bytes(&self) -> &[u8] {
        match self {
            StepKey::Buffer(key) => key,
            StepKey::Run(key) => key,
        }
    }
}

/// The smaller of a scan's end bound and a range's exclusive end.
pub(super) fn min_end(scan_end: &Bound<Bytes>, range_end: &Option<Bytes>) -> Bound<Bytes> {
    let Some(range_end) = range_end else {
        return scan_end.clone();
    };
    match scan_end {
        Included(end) if end < range_end => Included(end.clone()),
        Excluded(end) if end <= range_end => Excluded(end.clone()),
        _ => Excluded(range_end.clone()),
    }
}

/// What one step of a scan found.
enum Step {
    Row(RowEntry),
    /// The newest version of this key is a merge operand.
    Resolve(Bytes),
    End,
}

/// The source of the version that a step takes.
#[derive(Clone, Copy)]
enum Best {
    Buffer(usize),
    View,
}

/// A scan of the pinned layers of one or more adjacent cached ranges. See the module
/// documentation.
pub(crate) struct CachedScan {
    /// The layers of the current part.
    layers: Arc<Layers>,
    /// The end of the current part, within the scan range.
    part_end: Bound<Bytes>,
    /// The layers of the later parts, in key order. Each part starts where the one before ends.
    rest: VecDeque<Arc<Layers>>,
    /// The end of the scan range.
    scan_end: Bound<Bytes>,
    seq: u64,
    /// A cursor over the base of the current layers, which it reads through `layers`.
    base: Option<RangeFileCursor>,
    /// A cursor for each buffer of the current layers that can hold a visible version, newest
    /// first.
    buffers: Vec<DeltaCursor>,
    /// A cursor over the view of the current layers' runs, if one of them can hold a visible
    /// version.
    view: Option<ViewCursor>,
    merge_operator: Option<MergeOperatorType>,
    /// A merge-operand key that `next_sync` found and left for `next` to resolve.
    unresolved: Option<Bytes>,
    /// The scan covers one key. The normal path then uses the point-lookup stack.
    point: bool,
}

impl CachedScan {
    /// Returns a scan of `range` over the layers of adjacent cached ranges, in key order. The
    /// first range holds the start of `range`, and together they hold all of `range`.
    pub(crate) fn new(
        parts: Vec<Arc<Layers>>,
        seq: u64,
        range: &BytesRange,
        merge_operator: Option<MergeOperatorType>,
    ) -> io::Result<Self> {
        let mut rest: VecDeque<_> = parts.into();
        let Some(layers) = rest.pop_front() else {
            return Err(io::Error::other("range cache scan without parts"));
        };
        let scan_end = RangeBounds::end_bound(range).cloned();
        let first_end = min_end(&scan_end, &layers.end);
        let mut scan = Self {
            layers,
            part_end: Unbounded,
            rest,
            scan_end,
            seq,
            base: None,
            buffers: Vec::new(),
            view: None,
            merge_operator,
            unresolved: None,
            point: range.as_point().is_some(),
        };
        let start = RangeBounds::start_bound(range).map(|key| key.as_ref());
        scan.open(start, first_end)?;
        Ok(scan)
    }

    /// Sets whether the scan that this one is part of covers one key. A part of a mixed scan
    /// resolves merge operands with the iterator stack of the whole scan. See `resolve`.
    pub(super) fn set_point(&mut self, point: bool) {
        self.point = point;
    }

    /// Starts reading the current layers from `start` to `end`.
    fn open(&mut self, start: Bound<&[u8]>, end: Bound<Bytes>) -> io::Result<()> {
        let layers = &self.layers;
        // The cursors read the files of the pinned layers, so they take no reference to them.
        self.base = match &layers.base {
            Some(file) => Some(RangeFileCursor::with_bounds(file, start, end.clone())?),
            None => None,
        };
        let seq = self.seq;
        self.buffers.clear();
        // Skip empty buffers and buffers whose versions all exceed `seq`.
        for (i, delta) in layers.buffers().enumerate() {
            if !delta.is_empty() && delta.lo() < seq {
                self.buffers.push(DeltaCursor::new(i, delta, start, seq));
            }
        }
        // One cursor for every run.
        self.view = match &layers.view {
            Some(view) if view.has_visible(seq) => Some(ViewCursor::new(view, start, seq)?),
            _ => None,
        };
        self.part_end = end;
        Ok(())
    }

    /// Moves to the next part, which starts where the current one ends. Returns false after
    /// the last part.
    fn next_part(&mut self) -> io::Result<bool> {
        let Some(start) = self.layers.end.clone() else {
            return Ok(false);
        };
        let reaches_next = match &self.scan_end {
            Unbounded => true,
            Included(end) => *end >= start,
            Excluded(end) => *end > start,
        };
        if !reaches_next {
            return Ok(false);
        }
        let Some(layers) = self.rest.pop_front() else {
            return Ok(false);
        };
        let end = min_end(&self.scan_end, &layers.end);
        self.layers = layers;
        self.open(Included(start.as_ref()), end)?;
        Ok(true)
    }

    /// Finds the next key and its newest visible version, without the normal iterator stack.
    fn step(&mut self) -> io::Result<Step> {
        loop {
            let layers = &*self.layers;
            for cursor in self.buffers.iter_mut() {
                cursor.fill(buffer_of(layers, cursor.index), &self.part_end);
            }
            if let Some(cursor) = self.view.as_mut() {
                cursor.fill(view_of(layers), &self.part_end)?;
            }
            // The newest source with the smallest key. On a tie, the newer source wins: buffers
            // are newer than runs, and earlier buffers newer than later ones.
            let mut best: Option<(Best, &[u8])> = None;
            for (i, cursor) in self.buffers.iter().enumerate() {
                if let Some(key) = cursor.key(buffer_of(layers, cursor.index)) {
                    if best.is_none_or(|(_, best_key)| key < best_key) {
                        best = Some((Best::Buffer(i), key));
                    }
                }
            }
            let view_key = self.view.as_ref().filter(|c| c.peeked);
            if let Some(key) = view_key.and_then(|c| c.key(view_of(layers))) {
                if best.is_none_or(|(_, best_key)| key < best_key) {
                    best = Some((Best::View, key));
                }
            }
            let base_key = self.base.as_ref().and_then(|base| base.key());
            let order = match (base_key, best) {
                (None, None) => None,
                (Some(_), None) => Some(std::cmp::Ordering::Less),
                (None, Some(_)) => Some(std::cmp::Ordering::Greater),
                (Some(base_key), Some((_, key))) => Some(base_key.cmp(key)),
            };
            let best = best.map(|(b, _)| b);
            let Some(order) = order else {
                if self.next_part()? {
                    continue;
                }
                return Ok(Step::End);
            };
            if order == std::cmp::Ordering::Less {
                let row = match (self.base.as_mut(), &self.layers.base) {
                    (Some(cursor), Some(file)) => cursor.take(file)?,
                    _ => return Ok(Step::End),
                };
                return Ok(match row.value {
                    ValueDeletable::Merge(_) => Step::Resolve(row.key),
                    _ => Step::Row(row),
                });
            }
            if order == std::cmp::Ordering::Equal {
                if let (Some(cursor), Some(file)) = (self.base.as_mut(), &layers.base) {
                    cursor.skip(file)?;
                }
            }
            let Some(best) = best else {
                return Ok(Step::End);
            };
            // The key of the version to take. Older sources that hold it lose.
            let key = match best {
                Best::Buffer(b) => {
                    let cursor = &self.buffers[b];
                    cursor
                        .key(buffer_of(layers, cursor.index))
                        .map(StepKey::Buffer)
                }
                Best::View => self.view.as_ref().and_then(|c| c.step_key(view_of(layers))),
            };
            let Some(key) = key else {
                return Ok(Step::End);
            };
            for (j, cursor) in self.buffers.iter_mut().enumerate() {
                if !matches!(best, Best::Buffer(b) if b == j) {
                    cursor.skip_key(buffer_of(layers, cursor.index), key.as_bytes());
                }
            }
            if !matches!(best, Best::View) {
                if let Some(cursor) = self.view.as_mut() {
                    cursor.skip_key(view_of(layers), key.as_bytes())?;
                }
            }
            // Copy out only what the scan returns.
            let step = match best {
                Best::Buffer(b) => {
                    let cursor = &mut self.buffers[b];
                    let delta = buffer_of(layers, cursor.index);
                    let step = cursor
                        .version(delta)
                        .and_then(|version| match version.kind {
                            Kind::Value => Some(Step::Row(to_row(&version))),
                            Kind::Tombstone => None,
                            Kind::Merge => Some(Step::Resolve(Bytes::copy_from_slice(version.key))),
                        });
                    cursor.skip_key(delta, key.as_bytes());
                    step
                }
                Best::View => {
                    let view = view_of(layers);
                    let Some(cursor) = self.view.as_mut() else {
                        return Ok(Step::End);
                    };
                    // A tombstone or a merge operand needs no copy of its value.
                    match cursor.row.map(|row| row.kind()) {
                        Some(Kind::Tombstone) => {
                            cursor.leave(view);
                            None
                        }
                        Some(Kind::Merge) => {
                            let key = Bytes::copy_from_slice(key.as_bytes());
                            cursor.leave(view);
                            Some(Step::Resolve(key))
                        }
                        _ => Some(Step::Row(cursor.take(view)?)),
                    }
                }
            };
            if let Some(step) = step {
                return Ok(step);
            }
        }
    }

    /// Resolves merge operands for `key` via the normal iterator stack. Non-point scans use
    /// `[key, key + 0x00)` so entry kinds match scan semantics.
    async fn resolve(&self, key: Bytes) -> Result<Option<RowEntry>, SlateDBError> {
        let range = if self.point {
            BytesRange::new(Included(key.clone()), Included(key))
        } else {
            let mut after = key.to_vec();
            after.push(0);
            BytesRange::new(Included(key), Excluded(Bytes::from(after)))
        };
        let mut iter =
            super::stack_iterator(&self.layers, self.seq, range, self.merge_operator.clone())
                .await?;
        iter.next_entry().await
    }
}

#[async_trait]
impl RowEntryIterator for CachedScan {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        if let Some(key) = self.unresolved.take() {
            if let Some(entry) = self.resolve(key).await? {
                return Ok(Some(entry));
            }
        }
        loop {
            match self.step()? {
                Step::Row(entry) => return Ok(Some(entry)),
                Step::Resolve(key) => {
                    if let Some(entry) = self.resolve(key).await? {
                        return Ok(Some(entry));
                    }
                }
                Step::End => return Ok(None),
            }
        }
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        if self
            .unresolved
            .as_ref()
            .is_some_and(|key| key.as_ref() < next_key)
        {
            self.unresolved = None;
        }
        // Skip the parts that end at or before `next_key`.
        while !self.rest.is_empty()
            && match &self.part_end {
                Unbounded => false,
                Included(end) => next_key > end.as_ref(),
                Excluded(end) => next_key >= end.as_ref(),
            }
        {
            if !self.next_part()? {
                break;
            }
        }
        if let (Some(cursor), Some(file)) = (self.base.as_mut(), &self.layers.base) {
            cursor.seek(file, next_key)?;
        }
        let layers = &*self.layers;
        for cursor in self.buffers.iter_mut() {
            cursor.seek(buffer_of(layers, cursor.index), next_key);
        }
        if let Some(cursor) = self.view.as_mut() {
            cursor.seek(view_of(layers), next_key)?;
        }
        Ok(())
    }

    fn next_sync(&mut self) -> Option<Result<Option<RowEntry>, SlateDBError>> {
        if self.unresolved.is_some() {
            return None;
        }
        match self.step() {
            Ok(Step::Row(entry)) => Some(Ok(Some(entry))),
            Ok(Step::End) => Some(Ok(None)),
            Ok(Step::Resolve(key)) => {
                self.unresolved = Some(key);
                None
            }
            Err(e) => Some(Err(e.into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_iter::DbIterator;
    use crate::range_cache::delta::DeltaMemory;
    use crate::range_cache::file::{RangeCachePageSource, RangeFile, RangeFileWriter, RowLocators};
    use crate::range_cache::view::ViewMemory;
    use crate::range_cache::Run;
    use crate::read_trace::ReadTrace;
    use crate::test_utils::StringConcatMergeOperator;
    use proptest::prelude::*;
    use std::collections::BTreeMap;
    use std::ops::Bound::{self, Unbounded};
    use std::sync::atomic::Ordering;

    /// The sequence number of the base of the test layers. Runs, sealed buffers and the active
    /// buffer hold the versions above it, in that order, up to `MAX_SEQ`.
    const BASE_SEQ: u64 = 100;
    const MAX_SEQ: u64 = 300;

    #[derive(Clone, Copy, Debug)]
    enum Kind {
        Value,
        Tombstone,
        Merge,
    }

    fn key(k: u8) -> Bytes {
        Bytes::from(vec![b'k', k])
    }

    fn entry(k: u8, kind: Kind, seq: u64) -> RowEntry {
        let value = Bytes::from(format!("{k}@{seq}"));
        let value = match kind {
            Kind::Value => ValueDeletable::Value(value),
            Kind::Tombstone => ValueDeletable::Tombstone,
            Kind::Merge => ValueDeletable::Merge(value),
        };
        RowEntry::new(
            key(k),
            value,
            seq,
            (!seq.is_multiple_of(3)).then_some(seq as i64),
            None,
        )
    }

    /// A version as the writer makes it: every write has a create time, from a clock that
    /// does not go back. A merge result takes the newest create time of its versions, so with
    /// such times a tombstone under a merge operand never changes the result. The base drops
    /// tombstones, as compaction does, so the history test depends on this.
    fn history_entry(k: u8, kind: Kind, seq: u64) -> RowEntry {
        let mut entry = entry(k, kind, seq);
        entry.create_ts = Some(seq as i64);
        entry
    }

    fn kind() -> impl Strategy<Value = Kind> {
        prop_oneof![4 => Just(Kind::Value), 2 => Just(Kind::Tombstone), 2 => Just(Kind::Merge)]
    }

    fn bound() -> impl Strategy<Value = Bound<u8>> {
        prop_oneof![
            Just(Unbounded),
            (0u8..40).prop_map(Included),
            (0u8..40).prop_map(Excluded),
        ]
    }

    /// Where the versions above the base go. The cuts split `(BASE_SEQ, MAX_SEQ]` into
    /// intervals: the first `runs` are runs, the last one is the active buffer, and the ones in
    /// between are sealed buffers.
    #[derive(Clone, Debug)]
    struct Shape {
        cuts: Vec<u64>,
        runs: usize,
        /// How the delta view of the runs is built: 0 from the run files alone, 1 run by run as
        /// flushes build it, 2 run by run on top of two older runs below the base that a major
        /// merge then drops, 3 run by run with the newest run written as a flush at the run cap,
        /// which merged a buffer and two runs that the view held and then drops.
        view: u8,
        /// How run `i` is written, by the three bits at `3 * i`: 0 straight from its versions,
        /// 1 and 2 as a flush of two and three sealed buffers that split its interval, and as a
        /// flush at the run cap otherwise, which merges buffers with the newest runs: 3 a buffer
        /// and a run, 4 two buffers and a run, 5 three buffers and two runs, 6 a buffer and two
        /// runs, 7 two buffers and two runs.
        writes: u32,
    }

    /// The sealed buffers and the runs, newest first, whose merge writes a run of `how`.
    fn merge_inputs(how: u32) -> (usize, usize) {
        match how {
            1 => (2, 0),
            2 => (3, 0),
            3 => (1, 1),
            4 => (2, 1),
            5 => (3, 2),
            6 => (1, 2),
            _ => (2, 2),
        }
    }

    fn shape() -> impl Strategy<Value = Shape> {
        proptest::collection::btree_set(BASE_SEQ + 1..MAX_SEQ, 0..10).prop_flat_map(|cuts| {
            let cuts: Vec<u64> = cuts.into_iter().collect();
            let most = cuts.len().min(*crate::range_cache::MAX_RUNS_RANGE.end());
            (Just(cuts), 0..=most, any::<u32>(), 0u8..4).prop_map(|(cuts, runs, writes, view)| {
                Shape {
                    cuts,
                    runs,
                    view,
                    writes,
                }
            })
        })
    }

    /// One version per key and sequence number, as the writer applies them.
    type History = BTreeMap<(u8, u64), Kind>;

    /// Layers with only an active buffer that holds every version of `history`.
    fn all_of(history: &History, make: fn(u8, Kind, u64) -> RowEntry) -> Arc<Layers> {
        let all = Delta::for_test(&DeltaMemory::detached(), 0);
        let mut writer = all.writer();
        for ((k, seq), kind) in history {
            writer.append(&make(*k, *kind, *seq));
        }
        drop(writer);
        Arc::new(Layers {
            active: Arc::new(all),
            sealed: Vec::new(),
            runs: Vec::new(),
            view: None,
            base: None,
            base_seq: 0,
            covered_to: 0,
            end: None,
        })
    }

    /// Builds layers of `shape` from the versions of `history` whose keys pass `keys`: a base
    /// with the state at `BASE_SEQ`, written the way a load or a major merge writes it, then
    /// runs and buffers with every version of their intervals.
    #[allow(clippy::too_many_arguments)]
    async fn layers_of(
        dir: &std::path::Path,
        name: &str,
        history: &History,
        make: fn(u8, Kind, u64) -> RowEntry,
        shape: &Shape,
        page_source: RangeCachePageSource,
        keys: impl Fn(u8) -> bool,
        end: Option<Bytes>,
    ) -> Arc<Layers> {
        let history: History = history
            .iter()
            .filter(|((k, _), _)| keys(*k))
            .map(|(k, v)| (*k, *v))
            .collect();
        let merge: MergeOperatorType = Arc::new(StringConcatMergeOperator);
        let disk = crate::range_cache::file::FileBudget::detached();
        let memory = Arc::new(ViewMemory::default());
        let mut writer = RangeFileWriter::create(dir, &format!("{name}-base"), 512).unwrap();
        let mut rows = crate::range_cache::stack_iterator(
            &all_of(&history, make),
            BASE_SEQ,
            BytesRange::unbounded(),
            Some(merge),
        )
        .await
        .unwrap();
        while let Some(row) = rows.next_entry().await.unwrap() {
            writer.push(&row).unwrap();
        }
        let path = writer.finish().unwrap();
        let base = RangeFile::open(path, page_source, disk.clone()).unwrap();

        // The versions of `(lo, hi]`, key ascending and newest first.
        let versions = |lo: u64, hi: u64| {
            let mut out: Vec<RowEntry> = history
                .iter()
                .filter(|((_, seq), _)| *seq > lo && *seq <= hi)
                .map(|((k, seq), kind)| make(*k, *kind, *seq))
                .collect();
            out.sort_by(|a, b| a.key.cmp(&b.key).then(b.seq.cmp(&a.seq)));
            out
        };
        let bounds: Vec<u64> = std::iter::once(BASE_SEQ)
            .chain(shape.cuts.iter().copied())
            .chain(std::iter::once(MAX_SEQ))
            .collect();
        let intervals = bounds.len() - 1;
        // A run of `(lo, hi]` written as a flush writes it, with its row locators.
        let finish = |writer: RangeFileWriter, lo: u64, hi: u64| {
            let (path, rows) = writer.finish_with_locators().unwrap();
            let file = RangeFile::open(path, page_source, disk.clone()).unwrap();
            (Arc::new(Run::new(file, lo, hi).unwrap()), rows)
        };
        let file_of = |file_name: String, lo: u64, hi: u64| {
            let mut writer = RangeFileWriter::create_run(dir, &file_name, 512)
                .unwrap()
                .with_locators();
            for row in versions(lo, hi) {
                writer.push(&row).unwrap();
            }
            finish(writer, lo, hi)
        };
        let buffer_of = |lo: u64, hi: u64| {
            let delta = Delta::for_test(&DeltaMemory::detached(), lo);
            let mut writer = delta.writer();
            for row in versions(lo, hi) {
                writer.append(&row);
            }
            drop(writer);
            delta
        };
        // Oldest first.
        let mut runs: Vec<(Arc<Run>, RowLocators)> = Vec::new();
        // With view shape 3, the runs that the newest run's flush replaced, newest first.
        let mut replaced: Vec<(Arc<Run>, RowLocators)> = Vec::new();
        for i in 0..shape.runs {
            let (lo, hi) = (bounds[i], bounds[i + 1]);
            let at_cap = shape.view == 3 && i + 1 == shape.runs;
            let how = if at_cap {
                6
            } else {
                (shape.writes >> (3 * i)) & 7
            };
            let run = match how {
                0 => file_of(format!("{name}-run{i}"), lo, hi),
                how => {
                    // Inputs that split the interval into parts, written by the merge of
                    // flushes and flushes at the run cap. Part `j`, newest first, covers
                    // `(edges[parts - j - 1], edges[parts - j]]`; the newest parts are the
                    // sealed buffers. Parts can be empty.
                    let (buffer_parts, run_parts) = merge_inputs(how);
                    let parts = buffer_parts + run_parts;
                    let edges: Vec<u64> = (0..=parts)
                        .map(|k| lo + (hi - lo) * k as u64 / parts as u64)
                        .collect();
                    let part = |j: usize| (edges[parts - j - 1], edges[parts - j]);
                    let sealed: Vec<(Delta, u64)> = (0..buffer_parts)
                        .map(|j| {
                            let (l, h) = part(j);
                            (buffer_of(l, h), h)
                        })
                        .collect();
                    let merged: Vec<(Arc<Run>, RowLocators)> = (buffer_parts..parts)
                        .map(|j| {
                            let (l, h) = part(j);
                            file_of(format!("{name}-run{i}-part{j}"), l, h)
                        })
                        .collect();
                    let buffers: Vec<(&Delta, u64)> = sealed.iter().map(|(d, s)| (d, *s)).collect();
                    let inputs: Vec<&Run> = merged.iter().map(|(r, _)| &**r).collect();
                    let mut writer =
                        RangeFileWriter::create_run(dir, &format!("{name}-run{i}"), 512)
                            .unwrap()
                            .with_locators();
                    let written =
                        crate::range_cache::merge::merge_versions(&mut writer, &buffers, &inputs)
                            .unwrap();
                    assert_eq!(written as usize, versions(lo, hi).len());
                    if at_cap {
                        replaced = merged;
                    }
                    finish(writer, lo, hi)
                }
            };
            runs.push(run);
        }
        let buffer = |lo: u64, hi: u64| {
            let delta = Delta::for_test(&DeltaMemory::detached(), lo);
            let mut writer = delta.writer();
            for row in versions(lo, hi) {
                writer.append(&row);
            }
            drop(writer);
            Arc::new(delta)
        };
        let newest_first: Vec<Arc<Run>> = runs.iter().rev().map(|(r, _)| r.clone()).collect();
        let view = match shape.view {
            _ if runs.is_empty() => None,
            0 => Some(DeltaView::from_runs(&newest_first, &memory).unwrap()),
            how => {
                let older: Vec<(Arc<Run>, RowLocators)> = if how == 2 {
                    vec![
                        file_of(format!("{name}-gone0"), 0, BASE_SEQ / 2),
                        file_of(format!("{name}-gone1"), BASE_SEQ / 2, BASE_SEQ),
                    ]
                } else {
                    Vec::new()
                };
                let flushed = match how {
                    3 => &runs[..runs.len() - 1],
                    _ => &runs[..],
                };
                let mut view = DeltaView::empty(&memory);
                for (run, rows) in older.iter().chain(flushed).chain(replaced.iter().rev()) {
                    view = view.with_run(run, rows).unwrap();
                }
                if how == 2 {
                    let gone: Vec<Arc<Run>> = older.iter().rev().map(|(r, _)| r.clone()).collect();
                    view = view.without(&gone).unwrap().unwrap();
                }
                if how == 3 {
                    let gone: Vec<Arc<Run>> = replaced.iter().map(|(r, _)| r.clone()).collect();
                    let (run, rows) = runs.last().unwrap();
                    view = DeltaView::after_flush(Some(&view), &gone, run, rows, &memory).unwrap();
                }
                Some(view)
            }
        };
        if let Some(view) = &view {
            view.check().unwrap();
        }
        let runs = newest_first;
        let mut sealed = Vec::new();
        for i in shape.runs..intervals - 1 {
            let delta = buffer(bounds[i], bounds[i + 1]);
            delta.sealed_at().store(bounds[i + 1], Ordering::Release);
            sealed.push(delta);
        }
        sealed.reverse();
        let layers = Layers {
            active: buffer(bounds[intervals - 1], MAX_SEQ),
            sealed,
            runs,
            view: view.map(Arc::new),
            base: Some(Arc::new(base)),
            base_seq: BASE_SEQ,
            covered_to: bounds[shape.runs],
            end,
        };
        layers.check(*crate::range_cache::MAX_RUNS_RANGE.end());
        Arc::new(layers)
    }

    async fn drain(iter: &mut DbIterator, limit: usize) -> Vec<RowEntry> {
        let mut out = Vec::new();
        while out.len() < limit {
            match iter.next_entry().await.unwrap() {
                Some(entry) => out.push(entry),
                None => break,
            }
        }
        out
    }

    fn page_source(mmap: bool) -> RangeCachePageSource {
        if mmap {
            RangeCachePageSource::Mmap
        } else {
            RangeCachePageSource::PositionalRead
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 300,
            // About one generated range in four is empty and rejected.
            max_global_rejects: 1_000_000,
            ..ProptestConfig::default()
        })]

        /// The direct scan returns exactly what the normal iterator stack returns over the same
        /// layers at the same sequence number, also after a seek. The layers have a base, up to
        /// nine runs read through a view built from scratch, flush by flush, after a major merge
        /// or after a flush at the run cap, sealed buffers and the active buffer, in mapped files
        /// or with positional reads. With `split`, the scan walks two
        /// adjacent ranges, split at that key, and the normal stack reads the unsplit layers.
        #[test]
        fn should_match_the_normal_iterator_stack(
            history in proptest::collection::btree_map((0u8..40, 1..=MAX_SEQ), kind(), 0..150),
            shape in shape(),
            seq in BASE_SEQ..=MAX_SEQ,
            start in bound(),
            end in bound(),
            prefix_rows in 0usize..20,
            seek_to in proptest::option::of(0u8..40),
            mmap in any::<bool>(),
            split in proptest::option::of(1u8..40),
        ) {
            let range = BytesRange::try_new(start.map(key), end.map(key));
            prop_assume!(range.is_some());
            let range = range.unwrap();
            let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
            runtime.block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let source = page_source(mmap);
                let layers = layers_of(dir.path(), "f", &history, entry, &shape, source, |_| true, None).await;
                let merge: MergeOperatorType = Arc::new(StringConcatMergeOperator);
                let mut expected =
                    crate::range_cache::stack_iterator(&layers, seq, range.clone(), Some(merge.clone()))
                        .await
                        .unwrap();
                // A split scan starts in the part that holds its start.
                let parts = match split {
                    None => vec![layers.clone()],
                    Some(at) => {
                        let high = layers_of(dir.path(), "high", &history, entry, &shape, source, |k| k >= at, None).await;
                        let starts_low = match RangeBounds::start_bound(&range) {
                            Unbounded => true,
                            Included(k) | Excluded(k) => *k < key(at),
                        };
                        if starts_low {
                            let low = layers_of(dir.path(), "low", &history, entry, &shape, source, |k| k < at, Some(key(at))).await;
                            vec![low, high]
                        } else {
                            vec![high]
                        }
                    }
                };
                let scan = CachedScan::new(parts, seq, &range, Some(merge)).unwrap();
                let mut actual = DbIterator::new_resolved(range.clone(), Box::new(scan), ReadTrace::none())
                    .await
                    .unwrap();

                let want = drain(&mut expected, prefix_rows).await;
                let got = drain(&mut actual, prefix_rows).await;
                assert_eq!(got, want);
                if let Some(target) = seek_to {
                    let target = key(target);
                    let allowed = RangeBounds::contains(&range, &target)
                        && want.last().is_none_or(|last| last.key < target);
                    let want_seek = expected.seek(&target).await;
                    let got_seek = actual.seek(&target).await;
                    assert_eq!(got_seek.is_ok(), want_seek.is_ok());
                    assert_eq!(got_seek.is_ok(), allowed);
                }
                assert_eq!(drain(&mut actual, usize::MAX).await, drain(&mut expected, usize::MAX).await);
            });
        }

        /// Cached reads at any sequence number from the base's to the newest return the state
        /// of the whole history at that number, as the normal path over all versions would.
        ///
        /// The history is one table of all versions. The base holds its state at `BASE_SEQ`,
        /// written the way a load or a major merge writes it; up to nine runs, sealed buffers and
        /// the active buffer hold the versions after the base. Unlike the test above, which reads
        /// the same layers both ways, the expected answer here does not come from the layers.
        /// Keys have many versions, so a run often holds several versions of a key, across its
        /// pages, and a read at an older sequence number stops at a version below the newest.
        #[test]
        fn should_match_the_history_at_any_read_seq(
            history in proptest::collection::btree_map((0u8..24, 1..=MAX_SEQ), kind(), 0..200),
            shape in shape(),
            seq in BASE_SEQ..=MAX_SEQ,
            start in bound(),
            end in bound(),
            mmap in any::<bool>(),
        ) {
            let range = BytesRange::try_new(start.map(key), end.map(key));
            prop_assume!(range.is_some());
            let range = range.unwrap();
            let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
            runtime.block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let merge: MergeOperatorType = Arc::new(StringConcatMergeOperator);
                let all = all_of(&history, history_entry);
                let layers = layers_of(dir.path(), "f", &history, history_entry, &shape, page_source(mmap), |_| true, None).await;

                // Scans. Compare what a caller sees. The entry kinds can differ: the base drops
                // tombstones, so a merge over a tombstone in the history is a merge over nothing
                // in the cache, which the stack returns as a merge operand with the same bytes.
                let visible = |rows: Vec<RowEntry>| {
                    rows.into_iter().map(crate::types::KeyValue::from).collect::<Vec<_>>()
                };
                let mut expected =
                    crate::range_cache::stack_iterator(&all, seq, range.clone(), Some(merge.clone()))
                        .await
                        .unwrap();
                let scan = CachedScan::new(vec![layers.clone()], seq, &range, Some(merge.clone())).unwrap();
                let mut actual = DbIterator::new_resolved(range.clone(), Box::new(scan), ReadTrace::none())
                    .await
                    .unwrap();
                assert_eq!(
                    visible(drain(&mut actual, usize::MAX).await),
                    visible(drain(&mut expected, usize::MAX).await)
                );

                // Point reads, as `RangeCache::get` and `Pinned::get` answer them.
                for k in 0u8..24 {
                    let point = BytesRange::new(Included(key(k)), Included(key(k)));
                    let want = crate::range_cache::stack_iterator(&all, seq, point, Some(merge.clone()))
                        .await
                        .unwrap()
                        .next_entry()
                        .await
                        .unwrap()
                        .map(crate::types::KeyValue::from);
                    let got = match crate::range_cache::lookup(&layers, &key(k), seq).unwrap() {
                        crate::range_cache::Found::Value(kv) => Some(kv),
                        crate::range_cache::Found::Absent => None,
                        crate::range_cache::Found::Merge => crate::range_cache::Pinned {
                            layers: layers.clone(),
                            seq,
                        }
                        .get(&key(k), Some(merge.clone()))
                        .await
                        .unwrap(),
                    };
                    assert_eq!(got, want, "key {k} at {seq} in {shape:?}");
                }
            });
        }
    }
}
