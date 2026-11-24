//! Sorted delta view over all run files of a cached range.
//!
//! Each entry stores a run slot plus first-version flag, a 32-bit file offset, and front-coded
//! key metadata (shared prefix length and next byte). Periodic anchor keys let seeks binary-search
//! anchors and scan one segment without reading run files in the common case. Flushes add a run
//! incrementally from its writer's row locators; major merges drop absorbed runs without reading
//! keys except to replace dropped anchors.

use std::borrow::Cow;
use std::cmp::Ordering as Cmp;
use std::io;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::file::{RowLocators, RowSpan};
use super::Run;

/// The most runs that one view holds: the slot field of a selector has 6 bits.
pub(crate) const VIEW_SLOTS: usize = 64;
/// The slot bits of a selector.
const SLOT: u8 = 0x3f;
/// The selector flag of the first, newest, version of a key.
const FIRST: u8 = 0x80;
/// The entries of a segment that a build aims for. A new key starts a segment after this many
/// entries, an old key after twice this many (it needs a key read).
pub(super) const SEGMENT: usize = 16;
/// The largest stored shared-prefix length. The stored value means "at least this many".
const LCP_CAP: usize = 255;

fn corrupt(msg: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("range cache view: {msg}"),
    )
}

fn lcp(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

/// Compares `key` with the concatenation `a ++ b`, and returns the order and the length of the
/// prefix that they share.
fn cmp_split(key: &[u8], a: &[u8], b: &[u8]) -> (Cmp, usize) {
    let head = &key[..key.len().min(a.len())];
    let l = lcp(head, a);
    if l < a.len() {
        if l < head.len() {
            return (head[l].cmp(&a[l]), l);
        }
        // `key` is a proper prefix of `a`.
        return (Cmp::Less, l);
    }
    let tail = &key[a.len()..];
    let m = lcp(tail, b);
    let order = match (tail.get(m), b.get(m)) {
        (Some(x), Some(y)) => x.cmp(y),
        (Some(_), None) => Cmp::Greater,
        (None, Some(_)) => Cmp::Less,
        (None, None) => Cmp::Equal,
    };
    (order, a.len() + m)
}

/// Heap memory held by all live delta views of one cache.
#[derive(Debug, Default)]
pub(crate) struct ViewMemory(AtomicU64);

impl ViewMemory {
    pub(crate) fn bytes(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Delta view of one cached range.
pub(crate) struct DeltaView {
    /// The runs of the view by slot.
    slots: Vec<Option<Arc<Run>>>,
    sel: Vec<u8>,
    off: Vec<u32>,
    lcp: Vec<u8>,
    byte: Vec<u8>,
    /// The prefix that every key of the view shares.
    prefix: Vec<u8>,
    /// The entry of each anchor, ascending. Entry 0 is always the first.
    anchor_at: Vec<u32>,
    /// `anchor_keys[anchor_end[j - 1]..anchor_end[j]]` is the key of anchor `j` after `prefix`.
    anchor_end: Vec<u32>,
    anchor_keys: Vec<u8>,
    /// The smallest `lo` of a run of the view that holds a row, or `u64::MAX`: a scan at `seq`
    /// can see a version of the view only if this is below `seq`.
    lowest: u64,
    memory: Arc<ViewMemory>,
    /// The heap bytes of the view, counted in `memory` and in the metadata budget.
    bytes: u64,
}

impl std::fmt::Debug for DeltaView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaView")
            .field("entries", &self.sel.len())
            .field("anchors", &self.anchor_at.len())
            .field("runs", &self.runs().count())
            .field("bytes", &self.bytes)
            .finish()
    }
}

impl Drop for DeltaView {
    fn drop(&mut self) {
        self.memory.0.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

impl DeltaView {
    /// A view of no run, to add a range's first run to.
    pub(crate) fn empty(memory: &Arc<ViewMemory>) -> Self {
        Out::new(Vec::new(), Vec::new(), 0).finish(memory)
    }

    /// The number of versions in the view.
    pub(crate) fn len(&self) -> usize {
        self.sel.len()
    }

    /// The heap bytes of the view.
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    /// The runs of the view, in slot order.
    pub(crate) fn runs(&self) -> impl Iterator<Item = &Arc<Run>> {
        self.slots.iter().flatten()
    }

    /// True if the view holds `run`.
    pub(crate) fn holds(&self, run: &Arc<Run>) -> bool {
        self.runs().any(|r| Arc::ptr_eq(r, run))
    }

    /// True if a version of the view can be visible at `seq`.
    pub(crate) fn has_visible(&self, seq: u64) -> bool {
        self.lowest < seq
    }

    /// The run of entry `i`.
    pub(crate) fn run_of(&self, i: usize) -> &Run {
        self.slot_run(self.sel[i] & SLOT)
    }

    fn slot_run(&self, slot: u8) -> &Run {
        self.slots[slot as usize]
            .as_deref()
            .expect("a view entry of an empty slot")
    }

    /// The slot of entry `i`.
    pub(crate) fn slot_of(&self, i: usize) -> u8 {
        self.sel[i] & SLOT
    }

    /// The run in `slot`.
    pub(crate) fn run_in(&self, slot: u8) -> &Run {
        self.slot_run(slot)
    }

    fn is_first(&self, i: usize) -> bool {
        self.sel[i] & FIRST != 0
    }

    /// The offset of entry `i`'s row in its run file.
    pub(crate) fn offset(&self, i: usize) -> u64 {
        self.off[i] as u64
    }

    /// The first entry after `i` that starts a key, or the end.
    pub(crate) fn next_key(&self, i: usize) -> usize {
        let mut j = i + 1;
        while j < self.sel.len() && self.sel[j] & FIRST == 0 {
            j += 1;
        }
        j
    }

    /// The header of entry `i`'s row.
    pub(crate) fn row(&self, i: usize) -> io::Result<RowSpan> {
        self.run_of(i).file.row_at(self.offset(i))
    }

    /// The key of entry `i`, read from its run file.
    pub(crate) fn key(&self, i: usize) -> io::Result<Cow<'_, [u8]>> {
        let span = self.row(i)?;
        self.run_of(i).file.bytes_at(span.key.0, span.key.1)
    }

    /// Returns the last entry within `window` after `i` in the same run and page (`< page_end`),
    /// stopping before encountering more than `others` distinct other runs.
    pub(crate) fn page_mates(
        &self,
        i: usize,
        page_end: u64,
        window: usize,
        others: usize,
    ) -> usize {
        let slot = self.sel[i] & SLOT;
        let mut seen = [0u8; 8];
        let mut distinct = 0;
        let mut last = i;
        for j in i + 1..(i + 1 + window).min(self.sel.len()) {
            let s = self.sel[j] & SLOT;
            if s == slot {
                if self.off[j] as u64 >= page_end {
                    break;
                }
                last = j;
            } else if !seen[..distinct].contains(&s) {
                if distinct >= others.min(seen.len()) {
                    break;
                }
                seen[distinct] = s;
                distinct += 1;
            }
        }
        last
    }

    fn anchor_suffix(&self, j: usize) -> &[u8] {
        let start = match j {
            0 => 0,
            _ => self.anchor_end[j - 1] as usize,
        };
        &self.anchor_keys[start..self.anchor_end[j] as usize]
    }

    /// The end of segment `j`: the entry of the next anchor, or the end of the view.
    fn segment_end(&self, j: usize) -> usize {
        self.anchor_at
            .get(j + 1)
            .map_or(self.sel.len(), |&a| a as usize)
    }

    /// The first entry whose key is at or after `start`, which starts a key, or the end. One
    /// binary search over the anchors and a walk of one segment's front coding; it reads a key
    /// from a file only where the front coding does not decide.
    pub(crate) fn seek(&self, start: Bound<&[u8]>) -> io::Result<usize> {
        let (target, strict) = match start {
            Bound::Unbounded => return Ok(0),
            Bound::Included(k) => (k, false),
            Bound::Excluded(k) => (k, true),
        };
        if self.sel.is_empty() {
            return Ok(0);
        }
        // Every key starts with the prefix: a target outside it is before or after all of them.
        let p = self.prefix.len();
        let head = &target[..target.len().min(p)];
        match head.cmp(&self.prefix[..head.len()]) {
            Cmp::Less => return Ok(0),
            Cmp::Greater => return Ok(self.sel.len()),
            // A target that is a proper prefix of the prefix is before every key.
            Cmp::Equal if target.len() < p => return Ok(0),
            Cmp::Equal => {}
        }
        let tail = &target[p..];
        // The anchors before the target: below it, or at most it for an excluded start.
        let (mut lo, mut hi) = (0, self.anchor_at.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            let before = match self.anchor_suffix(mid).cmp(tail) {
                Cmp::Less => true,
                Cmp::Equal => strict,
                Cmp::Greater => false,
            };
            if before {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 {
            return Ok(0);
        }
        let s = lo - 1;
        let anchor = self.anchor_suffix(s);
        let mut m = p + lcp(anchor, tail);
        let at = self.anchor_at[s] as usize;
        let end = self.segment_end(s);
        // The anchor is below the target, or equal to it for an excluded start: then the next key
        // is the answer.
        let mut equal = strict && anchor == tail;
        for i in at + 1..end {
            if !self.is_first(i) {
                continue;
            }
            if equal {
                return Ok(i);
            }
            match self.coded_cmp(i, m, target)? {
                Coded::Less(l) => m = l,
                Coded::Equal if strict => equal = true,
                Coded::Equal | Coded::Greater(..) => return Ok(i),
            }
        }
        Ok(end)
    }

    /// Compares entry `i`'s key with `target`, given that the previous key is below `target` and
    /// shares `m` bytes with it. Reads the file key only when front coding cannot decide.
    fn coded_cmp(&self, i: usize, m: usize, target: &[u8]) -> io::Result<Coded> {
        let l = self.lcp[i] as usize;
        let b = self.byte[i];
        let decided = if l == LCP_CAP && m >= LCP_CAP {
            None
        } else if l < m {
            Some(Coded::Greater(l, b))
        } else if l > m {
            Some(Coded::Less(m))
        } else {
            match target.get(m) {
                Some(t) if b < *t => Some(Coded::Less(m)),
                Some(t) if b > *t => Some(Coded::Greater(m, b)),
                _ => None,
            }
        };
        if let Some(decided) = decided {
            return Ok(decided);
        }
        let key = self.key(i)?;
        let l = lcp(&key, target);
        Ok(match key.as_ref().cmp(target) {
            Cmp::Less => Coded::Less(l),
            Cmp::Equal => Coded::Equal,
            Cmp::Greater => Coded::Greater(l, key[l]),
        })
    }

    /// Builds a view from `runs` (newest first) by reading every key from their files.
    #[cfg(test)]
    pub(crate) fn from_runs(runs: &[Arc<Run>], memory: &Arc<ViewMemory>) -> io::Result<Self> {
        if runs.len() > VIEW_SLOTS {
            return Err(corrupt("too many runs"));
        }
        let mut rows: Vec<(Vec<u8>, u64, u8, u32)> = Vec::new();
        for (slot, run) in runs.iter().enumerate() {
            check_size(run)?;
            run.file.for_each_row(|offset, key, seq| {
                rows.push((key.to_vec(), seq, slot as u8, offset as u32));
                Ok(())
            })?;
        }
        rows.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
        let prefix = match (rows.first(), rows.last()) {
            (Some(first), Some(last)) => first.0[..lcp(&first.0, &last.0)].to_vec(),
            _ => Vec::new(),
        };
        let slots = runs.iter().map(|r| Some(r.clone())).collect();
        let mut out = Out::new(prefix, slots, rows.len());
        let mut prev: Option<&[u8]> = None;
        for (key, _, slot, offset) in &rows {
            match prev {
                Some(p) if p == key.as_slice() => out.push(*slot, *offset, 0, 0),
                _ => {
                    let (l, b) = coding(prev, key);
                    if out.wants_anchor() {
                        out.anchor(&key[out.prefix.len()..]);
                    }
                    out.push(*slot | FIRST, *offset, l, b);
                }
            }
            prev = Some(key);
        }
        Ok(out.finish(memory))
    }

    /// Builds the view after a flush: removes `replaced` from `was` and adds `run` using `rows`.
    pub(crate) fn after_flush(
        was: Option<&DeltaView>,
        replaced: &[Arc<Run>],
        run: &Arc<Run>,
        rows: &RowLocators,
        memory: &Arc<ViewMemory>,
    ) -> io::Result<Self> {
        let kept;
        let base = match was {
            Some(view) if !replaced.is_empty() => {
                kept = view.without(replaced)?;
                kept.as_ref()
            }
            Some(view) => Some(view),
            None if replaced.is_empty() => None,
            None => return Err(corrupt("a flush replaced runs of a range without a view")),
        };
        match base {
            Some(view) => view.with_run(run, rows),
            None => DeltaView::empty(memory).with_run(run, rows),
        }
    }

    /// Adds a newer `run` and its `new` row locators into this view in a free slot.
    pub(crate) fn with_run(&self, run: &Arc<Run>, new: &RowLocators) -> io::Result<Self> {
        check_size(run)?;
        if new.rows() as u64 != run.file.rows() {
            return Err(corrupt("row locators of another run"));
        }
        let slot = (0..VIEW_SLOTS)
            .find(|&s| self.slots.get(s).is_none_or(|r| r.is_none()))
            .ok_or_else(|| corrupt("no free slot"))?;
        let mut slots = self.slots.clone();
        if slots.len() <= slot {
            slots.resize(slot + 1, None);
        }
        slots[slot] = Some(run.clone());
        let (Some(first), Some(last)) = (new.first(), new.last()) else {
            let mut out = Out::new(self.prefix.clone(), slots, self.len());
            self.copy_all(&mut out, &self.prefix);
            return Ok(out.finish(&self.memory));
        };
        let prefix = if self.sel.is_empty() {
            first[..lcp(first, last)].to_vec()
        } else {
            let keep = lcp(&self.prefix, first).min(lcp(&self.prefix, last));
            self.prefix[..keep].to_vec()
        };
        let mut out = Out::new(prefix, slots, self.len() + new.rows());
        let mut merge = Merge::new(self, &mut out);
        for g in 0..new.groups() {
            merge.insert(new.key(g), new.offsets(g), slot as u8)?;
        }
        merge.finish()?;
        Ok(out.finish(&self.memory))
    }

    /// Removes `dropped` runs from this view. Returns `None` if no runs remain.
    pub(crate) fn without(&self, dropped: &[Arc<Run>]) -> io::Result<Option<Self>> {
        let mut slots = self.slots.clone();
        let mut dead = [false; VIEW_SLOTS];
        for run in dropped {
            let slot = slots
                .iter()
                .position(|r| r.as_ref().is_some_and(|r| Arc::ptr_eq(r, run)))
                .ok_or_else(|| corrupt("a dropped run that the view does not hold"))?;
            dead[slot] = true;
            slots[slot] = None;
        }
        while slots.last().is_some_and(|r| r.is_none()) {
            slots.pop();
        }
        if slots.is_empty() {
            return Ok(None);
        }
        let live = |i: usize| !dead[self.slot_of(i) as usize];
        let kept = (0..self.len()).filter(|&i| live(i)).count();
        let mut out = Out::new(self.prefix.clone(), slots, kept);
        let mut gone: Option<(u8, u8)> = None;
        let mut anchor = 0;
        let mut i = 0;
        while i < self.len() {
            let end = self.next_key(i);
            let is_anchor = self.anchor_at.get(anchor).is_some_and(|&a| a as usize == i);
            if is_anchor {
                anchor += 1;
            }
            let Some(first) = (i..end).find(|&j| live(j)) else {
                gone = Some(chain(gone, self.lcp[i], self.byte[i]));
                i = end;
                continue;
            };
            let (l, b) = match gone.take() {
                Some(d) => chain(Some(d), self.lcp[i], self.byte[i]),
                None => (self.lcp[i], self.byte[i]),
            };
            let (l, b) = if out.sel.is_empty() { (0, 0) } else { (l, b) };
            if is_anchor {
                out.anchor(self.anchor_suffix(anchor - 1));
            } else if out.must_anchor() {
                let key = self.key(i)?;
                out.anchor(&key[out.prefix.len()..]);
            }
            out.push((self.sel[first] & SLOT) | FIRST, self.off[first], l, b);
            for j in first + 1..end {
                if live(j) {
                    out.push(self.sel[j] & SLOT, self.off[j], 0, 0);
                }
            }
            i = end;
        }
        Ok(Some(out.finish(&self.memory)))
    }

    /// Copies every entry to `out`, whose prefix is a prefix of this view's.
    fn copy_all(&self, out: &mut Out, prefix: &[u8]) {
        let mut anchor = 0;
        for i in 0..self.len() {
            if self.anchor_at.get(anchor).is_some_and(|&a| a as usize == i) {
                out.anchor_split(&self.prefix[prefix.len()..], self.anchor_suffix(anchor));
                anchor += 1;
            }
            out.push(self.sel[i], self.off[i], self.lcp[i], self.byte[i]);
        }
    }

    /// The entries as (run, offset, first of its key), for tests that compare views by content.
    #[cfg(test)]
    pub(crate) fn entries(&self) -> Vec<(usize, u64, bool)> {
        (0..self.len())
            .map(|i| {
                (
                    std::ptr::from_ref(self.run_of(i)).addr(),
                    self.offset(i),
                    self.is_first(i),
                )
            })
            .collect()
    }

    /// Checks every invariant of the view against its files: the order of the entries, the
    /// flags, the front coding, the prefix, the anchors, and that the view lists every row of
    /// its runs once.
    #[cfg(test)]
    pub(crate) fn check(&self) -> io::Result<()> {
        let mut prev: Option<(Vec<u8>, u64)> = None;
        let mut anchor = 0;
        let mut rows = 0u64;
        for i in 0..self.len() {
            let span = self.row(i)?;
            let key = self.key(i)?.to_vec();
            assert!(
                key.starts_with(&self.prefix),
                "entry {i} outside the prefix"
            );
            let run = self.run_of(i);
            assert!(
                span.seq > run.lo && span.seq <= run.hi,
                "entry {i} outside its run"
            );
            let first = match &prev {
                None => true,
                Some((k, s)) => {
                    assert!(
                        k < &key || (k == &key && *s > span.seq),
                        "entry {i} out of order"
                    );
                    k != &key
                }
            };
            assert_eq!(self.is_first(i), first, "the first flag of entry {i}");
            if first {
                if let Some((k, _)) = &prev {
                    let l = lcp(k, &key);
                    assert_eq!(self.lcp[i] as usize, l.min(LCP_CAP), "lcp of entry {i}");
                    if l < LCP_CAP {
                        assert_eq!(self.byte[i], key[l], "byte of entry {i}");
                    }
                }
            }
            if self.anchor_at.get(anchor).is_some_and(|&a| a as usize == i) {
                assert!(first, "anchor {anchor} inside a key");
                assert_eq!(
                    self.anchor_suffix(anchor),
                    &key[self.prefix.len()..],
                    "anchor {anchor}"
                );
                anchor += 1;
            }
            prev = Some((key, span.seq));
            rows += 1;
        }
        assert_eq!(anchor, self.anchor_at.len(), "anchors past the entries");
        assert!(self.len() == 0 || self.anchor_at.first() == Some(&0));
        let expected: u64 = self.runs().map(|r| r.file.rows()).sum();
        assert_eq!(rows, expected, "the view does not list every row once");
        Ok(())
    }
}

/// The result of comparing a key with a target by front coding: below it, sharing this many
/// bytes; equal; above it, sharing this many bytes, then this byte of the key.
enum Coded {
    Less(usize),
    Equal,
    Greater(usize, u8),
}

/// The front coding of `key` after `prev`: the length of their shared prefix, capped, and the
/// byte of `key` after it.
fn coding(prev: Option<&[u8]>, key: &[u8]) -> (u8, u8) {
    match prev {
        None => (0, 0),
        Some(prev) => {
            let l = lcp(prev, key);
            (l.min(LCP_CAP) as u8, key.get(l).copied().unwrap_or(0))
        }
    }
}

/// The front coding of a key after a run of dropped keys: for sorted keys `a < b < c`, the
/// prefix that `a` and `c` share is the shorter of what `a` and `b` and what `b` and `c` share, and
/// `c`'s byte after it is the byte of the last key in the chain that has the shortest.
fn chain(dropped: Option<(u8, u8)>, l: u8, b: u8) -> (u8, u8) {
    match dropped {
        Some((dl, db)) if dl < l => (dl, db),
        _ => (l, b),
    }
}

/// Run files must be smaller than 4 GiB, so that a row offset fits in 32 bits.
fn check_size(run: &Run) -> io::Result<()> {
    if run.file.file_len() > u32::MAX as u64 {
        return Err(corrupt("a run file of 4 GiB or more"));
    }
    Ok(())
}

/// A view being built, entry by entry.
struct Out {
    slots: Vec<Option<Arc<Run>>>,
    sel: Vec<u8>,
    off: Vec<u32>,
    lcp: Vec<u8>,
    byte: Vec<u8>,
    prefix: Vec<u8>,
    anchor_at: Vec<u32>,
    anchor_end: Vec<u32>,
    anchor_keys: Vec<u8>,
    /// Entries since the last anchor.
    since: usize,
}

impl Out {
    fn new(prefix: Vec<u8>, slots: Vec<Option<Arc<Run>>>, entries: usize) -> Self {
        Self {
            slots,
            sel: Vec::with_capacity(entries),
            off: Vec::with_capacity(entries),
            lcp: Vec::with_capacity(entries),
            byte: Vec::with_capacity(entries),
            prefix,
            anchor_at: Vec::with_capacity(entries / SEGMENT + 1),
            anchor_end: Vec::with_capacity(entries / SEGMENT + 1),
            anchor_keys: Vec::new(),
            since: 0,
        }
    }

    /// True where a new key, whose bytes are at hand, should start a segment.
    fn wants_anchor(&self) -> bool {
        self.sel.is_empty() || self.since >= SEGMENT
    }

    /// True where an old key should start a segment, though it needs a key read.
    fn must_anchor(&self) -> bool {
        self.sel.is_empty() || self.since >= 2 * SEGMENT
    }

    /// Starts a segment at the next entry, whose key after the prefix is `suffix`.
    fn anchor(&mut self, suffix: &[u8]) {
        self.anchor_split(&[], suffix);
    }

    /// Starts a segment at the next entry, whose key after the prefix is `a ++ b`.
    fn anchor_split(&mut self, a: &[u8], b: &[u8]) {
        self.anchor_at.push(self.sel.len() as u32);
        self.anchor_keys.extend_from_slice(a);
        self.anchor_keys.extend_from_slice(b);
        self.anchor_end.push(self.anchor_keys.len() as u32);
        self.since = 0;
    }

    fn push(&mut self, sel: u8, off: u32, lcp: u8, byte: u8) {
        self.sel.push(sel);
        self.off.push(off);
        self.lcp.push(lcp);
        self.byte.push(byte);
        self.since += 1;
    }

    fn finish(self, memory: &Arc<ViewMemory>) -> DeltaView {
        let bytes = (self.sel.capacity()
            + self.off.capacity() * 4
            + self.lcp.capacity()
            + self.byte.capacity()
            + self.prefix.capacity()
            + self.anchor_at.capacity() * 4
            + self.anchor_end.capacity() * 4
            + self.anchor_keys.capacity()
            + self.slots.capacity() * size_of::<Option<Arc<Run>>>()
            + size_of::<DeltaView>()) as u64;
        memory.0.fetch_add(bytes, Ordering::Relaxed);
        let lowest = self
            .slots
            .iter()
            .flatten()
            .filter(|r| r.file.rows() > 0)
            .map(|r| r.lo)
            .min()
            .unwrap_or(u64::MAX);
        DeltaView {
            slots: self.slots,
            sel: self.sel,
            off: self.off,
            lcp: self.lcp,
            byte: self.byte,
            prefix: self.prefix,
            anchor_at: self.anchor_at,
            anchor_end: self.anchor_end,
            anchor_keys: self.anchor_keys,
            lowest,
            memory: memory.clone(),
            bytes,
        }
    }
}

/// What the merge emitted last.
enum Last<'n> {
    Nothing,
    /// An old key, copied with its versions. The walk knows what it shares with the target.
    Old,
    /// A new key (possibly merged with an old key of the same bytes).
    New(&'n [u8]),
}

/// Merges a new run's keys into an existing view.
struct Merge<'v, 'o, 'n> {
    old: &'v DeltaView,
    out: &'o mut Out,
    p: usize,
    a: usize,
    has_prev: bool,
    prev_lcp: usize,
    last_target: Option<&'n [u8]>,
    last: Last<'n>,
    fix: Option<(u8, u8)>,
}

impl<'v, 'o, 'n> Merge<'v, 'o, 'n> {
    fn new(old: &'v DeltaView, out: &'o mut Out) -> Self {
        Self {
            old,
            out,
            p: 0,
            a: 0,
            has_prev: false,
            prev_lcp: 0,
            last_target: None,
            last: Last::Nothing,
            fix: None,
        }
    }

    fn anchor_cmp(&self, j: usize, key: &[u8]) -> (Cmp, usize) {
        let (order, l) = cmp_split(key, &self.old.prefix, self.old.anchor_suffix(j));
        (order.reverse(), l)
    }

    fn emit_old_key(&mut self) -> io::Result<()> {
        let old = self.old;
        let i = self.p;
        let is_anchor = old.anchor_at.get(self.a).is_some_and(|&a| a as usize == i);
        let (l, b) = self.fix.take().unwrap_or((old.lcp[i], old.byte[i]));
        let (l, b) = if self.out.sel.is_empty() {
            (0, 0)
        } else {
            (l, b)
        };
        if is_anchor {
            let rebase = &old.prefix[self.out.prefix.len()..];
            self.out.anchor_split(rebase, old.anchor_suffix(self.a));
            self.a += 1;
        } else if self.out.must_anchor() {
            let key = old.key(i)?;
            self.out.anchor(&key[self.out.prefix.len()..]);
        }
        self.out.push(old.sel[i], old.off[i], l, b);
        let end = old.next_key(i);
        for j in i + 1..end {
            self.out.push(old.sel[j], old.off[j], 0, 0);
        }
        self.p = end;
        self.has_prev = true;
        self.last = Last::Old;
        Ok(())
    }

    /// Emits the versions of the new key `key` before the old entry at `p`. `prev_lcp` is what the
    /// last emitted old key shares with it, if `last` is old.
    fn emit_new_key(&mut self, key: &'n [u8], offsets: &[u32], slot: u8, prev_lcp: usize) {
        let (l, b) = match self.last {
            Last::Nothing => (0, 0),
            Last::Old => (
                prev_lcp.min(LCP_CAP) as u8,
                key.get(prev_lcp).copied().unwrap_or(0),
            ),
            Last::New(prev) => coding(Some(prev), key),
        };
        if self.out.wants_anchor() {
            self.out.anchor(&key[self.out.prefix.len()..]);
        }
        let mut sel = slot | FIRST;
        for &offset in offsets {
            self.out.push(sel, offset, l, b);
            sel = slot;
        }
        self.last = Last::New(key);
    }

    /// Inserts the versions of the new key `key` (offsets newest first) in the merge.
    fn insert(&mut self, key: &'n [u8], offsets: &[u32], slot: u8) -> io::Result<()> {
        let old = self.old;
        let n = old.len();
        // What the old key before `p` shares with `key`: for sorted `prev < last < key`, the
        // shorter of what `prev` shares with `last` and what `last` shares with `key`.
        let mut m = match self.last_target {
            Some(last) => self.prev_lcp.min(lcp(last, key)),
            None => 0,
        };
        // Skip whole segments: the last anchor at or after `p` whose key is at most `key`.
        let mut jump = None;
        let mut j = self.a;
        while j < old.anchor_at.len() {
            let (order, l) = self.anchor_cmp(j, key);
            if order == Cmp::Greater {
                break;
            }
            jump = Some((j, order, l));
            j += 1;
        }
        if let Some((j, order, l)) = jump {
            let at = old.anchor_at[j] as usize;
            while self.p < at {
                self.emit_old_key()?;
            }
            debug_assert_eq!(self.a, j);
            if order == Cmp::Equal {
                return self.merge_at(key, offsets, slot);
            }
            // The anchor is below `key`.
            self.emit_old_key()?;
            m = l;
        }
        // Walk the front coding up to the next anchor, whose key is above `key`.
        let limit = old.anchor_at.get(self.a).map_or(n, |&a| a as usize);
        while self.p < limit {
            debug_assert!(self.has_prev, "a walk without a previous key");
            match old.coded_cmp(self.p, m, key)? {
                Coded::Less(l) => {
                    self.emit_old_key()?;
                    m = l;
                }
                Coded::Equal => return self.merge_at(key, offsets, slot),
                Coded::Greater(l, b) => {
                    return self.insert_before(key, offsets, slot, m, l, b);
                }
            }
        }
        if self.p >= n {
            // After every old key.
            self.emit_new_key(key, offsets, slot, m);
            self.prev_lcp = m;
            self.last_target = Some(key);
            return Ok(());
        }
        // Before the next anchor, whose key is above `key`.
        let (order, l) = self.anchor_cmp(self.a, key);
        debug_assert_eq!(order, Cmp::Greater);
        let b = cmp_split_byte(&old.prefix, old.anchor_suffix(self.a), l);
        self.insert_before(key, offsets, slot, m, l, b)
    }

    /// Inserts `key` before the old key at `p`, which is above it, shares `l` bytes with it and
    /// then has byte `b`. The old key before `p`, if any, shares `m` bytes with `key`.
    fn insert_before(
        &mut self,
        key: &'n [u8],
        offsets: &[u32],
        slot: u8,
        m: usize,
        l: usize,
        b: u8,
    ) -> io::Result<()> {
        self.emit_new_key(key, offsets, slot, m);
        self.fix = Some((l.min(LCP_CAP) as u8, b));
        self.prev_lcp = m;
        self.last_target = Some(key);
        Ok(())
    }

    /// Merges `key` with the old key at `p`, which has the same bytes: the new versions go first,
    /// and the old ones follow as older versions of the key.
    fn merge_at(&mut self, key: &'n [u8], offsets: &[u32], slot: u8) -> io::Result<()> {
        let old = self.old;
        let i = self.p;
        let is_anchor = old.anchor_at.get(self.a).is_some_and(|&a| a as usize == i);
        // The old key's place in the order goes to the new key; a pending fix was for the old
        // key as the first of its key, which it no longer is.
        self.fix = None;
        let (l, b) = match self.last {
            Last::Nothing => (0, 0),
            // The last emitted key is the old key before `p`, so the old key's own coding is the
            // new key's: they have the same bytes.
            Last::Old => (old.lcp[i], old.byte[i]),
            Last::New(prev) => coding(Some(prev), key),
        };
        if is_anchor || self.out.wants_anchor() {
            self.out.anchor(&key[self.out.prefix.len()..]);
        }
        if is_anchor {
            self.a += 1;
        }
        let mut sel = slot | FIRST;
        for &offset in offsets {
            self.out.push(sel, offset, l, b);
            sel = slot;
        }
        let end = old.next_key(i);
        for j in i..end {
            self.out.push(old.sel[j] & SLOT, old.off[j], 0, 0);
        }
        self.p = end;
        self.has_prev = true;
        // The old key before the new `p` is this key.
        self.prev_lcp = key.len();
        self.last_target = Some(key);
        self.last = Last::New(key);
        Ok(())
    }

    /// Copies the old entries after the last new key.
    fn finish(mut self) -> io::Result<()> {
        while self.p < self.old.len() {
            self.emit_old_key()?;
        }
        Ok(())
    }
}

/// The byte at `l` of the concatenation `a ++ b`.
fn cmp_split_byte(a: &[u8], b: &[u8], l: usize) -> u8 {
    if l < a.len() {
        a[l]
    } else {
        b.get(l - a.len()).copied().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::range_cache::file::{FileBudget, RangeCachePageSource, RangeFile, RangeFileWriter};
    use crate::types::{RowEntry, ValueDeletable};
    use proptest::prelude::*;

    /// Writes a run of `(lo, hi]` with `versions`, (key, seq), in any order, with a writer that
    /// collects its row locators, as a flush does. Checks that they equal the locators read back
    /// from the file.
    fn run(
        dir: &std::path::Path,
        name: &str,
        lo: u64,
        hi: u64,
        mut versions: Vec<(Vec<u8>, u64)>,
        page_size: usize,
    ) -> (Arc<Run>, RowLocators) {
        versions.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
        let mut writer = RangeFileWriter::create_run(dir, name, page_size)
            .unwrap()
            .with_locators();
        for (key, seq) in versions {
            let value = ValueDeletable::Value(bytes::Bytes::from(format!("v{seq}")));
            writer
                .push(&RowEntry::new(key.into(), value, seq, None, None))
                .unwrap();
        }
        let (path, locators) = writer.finish_with_locators().unwrap();
        let file =
            RangeFile::open(path, RangeCachePageSource::Mmap, FileBudget::detached()).unwrap();
        assert_eq!(locators, RowLocators::read(&file).unwrap());
        (Arc::new(Run::new(file, lo, hi).unwrap()), locators)
    }

    /// Keys of the shapes that matter: short ones, ones that are prefixes of others, the empty
    /// key, and long ones that share a long prefix and differ late (also past the 255-byte cap).
    fn key_strategy() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            proptest::collection::vec(0u8..4, 0..4),
            (0u8..4, 0u16..64).prop_map(|(a, n)| {
                let mut k = b"tpcc/order_line/".to_vec();
                k.extend_from_slice(&[0, 0, 0, 7, a]);
                k.extend_from_slice(&n.to_be_bytes());
                k
            }),
            (0u8..3, 0u8..4).prop_map(|(a, b)| {
                let mut k = vec![b'x'; 300];
                k.push(a);
                k.push(b);
                k
            }),
        ]
    }

    /// The versions of a run, (key, seq), and its interval.
    type Spec = (Vec<(Vec<u8>, u64)>, u64, u64);

    /// The expected entries of `runs`: every version, key ascending then newest first, as
    /// (key, seq).
    fn expected(runs: &[Spec]) -> Vec<(Vec<u8>, u64)> {
        let mut all: Vec<(Vec<u8>, u64)> = runs.iter().flat_map(|r| r.0.clone()).collect();
        all.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
        all
    }

    fn keys_of(view: &DeltaView) -> Vec<(Vec<u8>, u64)> {
        (0..view.len())
            .map(|i| (view.key(i).unwrap().to_vec(), view.row(i).unwrap().seq))
            .collect()
    }

    /// Brute force: the first entry at or after `start`.
    fn brute_seek(entries: &[(Vec<u8>, u64)], start: &Bound<Vec<u8>>) -> usize {
        entries
            .iter()
            .position(|(k, _)| match start {
                Bound::Unbounded => true,
                Bound::Included(s) => k >= s,
                Bound::Excluded(s) => k > s,
            })
            .unwrap_or(entries.len())
    }

    fn check_seeks(view: &DeltaView, entries: &[(Vec<u8>, u64)], targets: &[Vec<u8>]) {
        let mut probes: Vec<Vec<u8>> = targets.to_vec();
        probes.extend(entries.iter().map(|(k, _)| k.clone()));
        for t in &probes {
            for bound in [
                Bound::Included(t.clone()),
                Bound::Excluded(t.clone()),
                Bound::Unbounded,
            ] {
                let got = view.seek(bound.as_ref().map(|k| k.as_slice())).unwrap();
                assert_eq!(got, brute_seek(entries, &bound), "seek {bound:?}");
            }
        }
    }

    /// Checks `page_mates` from every entry against its definition.
    fn check_page_mates(view: &DeltaView, page_size: u64) {
        for i in 0..view.len() {
            for (window, others) in [(64, 3), (4, 0), (16, 1)] {
                let page_end = view.offset(i) - view.offset(i) % page_size + page_size;
                let j = view.page_mates(i, page_end, window, others);
                assert!(j >= i && j <= i + window);
                assert_eq!(view.slot_of(j), view.slot_of(i));
                assert!(view.offset(j) < page_end);
                let mut seen: Vec<u8> = (i + 1..j)
                    .map(|k| view.slot_of(k))
                    .filter(|s| *s != view.slot_of(i))
                    .collect();
                seen.sort_unstable();
                seen.dedup();
                assert!(seen.len() <= others, "{} other runs before {j}", seen.len());
                // The next entry of the same run and page is not within reach.
                let next = (j + 1..view.len().min(i + 1 + window))
                    .find(|&k| view.slot_of(k) == view.slot_of(i));
                if let Some(k) = next {
                    let mut between: Vec<u8> = (i + 1..k)
                        .map(|m| view.slot_of(m))
                        .filter(|s| *s != view.slot_of(i))
                        .collect();
                    between.sort_unstable();
                    between.dedup();
                    assert!(view.offset(k) >= page_end || between.len() > others);
                }
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 200, ..ProptestConfig::default() })]

        /// Views built incrementally, run by run as flushes build them, with runs dropped as a
        /// major merge drops them, and after a flush at the run cap, which replaces the newest
        /// runs with its own, list the same entries as a view built from the run files alone,
        /// keep every invariant, and seek like a brute-force search.
        #[test]
        fn should_build_views_that_match_their_runs(
            batches in proptest::collection::vec(
                proptest::collection::btree_set(key_strategy(), 0..40), 1..7),
            versions in 1u64..3,
            absorbed in 0usize..3,
            replaced in 1usize..3,
            flushed in proptest::collection::btree_set(key_strategy(), 0..20),
            targets in proptest::collection::vec(key_strategy(), 0..20),
            page_size in prop_oneof![Just(512usize), Just(4096usize)],
        ) {
            let dir = tempfile::tempdir().unwrap();
            let memory = Arc::new(ViewMemory::default());
            // Runs oldest first; each holds `versions` versions of its keys.
            let mut seq = 0;
            let mut specs = Vec::new();
            for batch in &batches {
                let lo = seq;
                let mut rows = Vec::new();
                for key in batch {
                    for _ in 0..versions {
                        seq += 1;
                        rows.push((key.clone(), seq));
                    }
                }
                seq += 1;
                specs.push((rows, lo, seq));
            }
            let runs: Vec<(Arc<Run>, RowLocators)> = specs
                .iter()
                .enumerate()
                .map(|(i, (rows, lo, hi))| run(dir.path(), &format!("r{i}"), *lo, *hi, rows.clone(), page_size))
                .collect();
            // Incremental: the oldest run on an empty view, then each newer one.
            let mut view = DeltaView::empty(&memory);
            for (r, rows) in &runs {
                view = view.with_run(r, rows).unwrap();
                view.check().unwrap();
            }
            let newest_first: Vec<Arc<Run>> = runs.iter().rev().map(|(r, _)| r.clone()).collect();
            let scratch = DeltaView::from_runs(&newest_first, &memory).unwrap();
            prop_assert_eq!(keys_of(&view), keys_of(&scratch));
            prop_assert_eq!(view.entries(), scratch.entries());
            prop_assert_eq!(keys_of(&view), expected(&specs));
            check_seeks(&view, &expected(&specs), &targets);
            check_page_mates(&view, page_size as u64);

            // A major merge absorbs the oldest runs.
            let absorbed = absorbed.min(runs.len());
            let gone: Vec<Arc<Run>> = runs[..absorbed].iter().rev().map(|(r, _)| r.clone()).collect();
            match view.without(&gone).unwrap() {
                None => prop_assert_eq!(absorbed, runs.len()),
                Some(left) => {
                    left.check().unwrap();
                    let want = expected(&specs[absorbed..]);
                    prop_assert_eq!(keys_of(&left), want.clone());
                    check_seeks(&left, &want, &targets);
                }
            }

            // A flush at the run cap: its run holds its own versions and those of the newest
            // runs, which it replaces.
            let replaced = replaced.min(runs.len());
            let kept = runs.len() - replaced;
            let lo = specs[kept].1;
            let mut rows: Vec<(Vec<u8>, u64)> = specs[kept..].iter().flat_map(|s| s.0.clone()).collect();
            for key in &flushed {
                seq += 1;
                rows.push((key.clone(), seq));
            }
            seq += 1;
            let (merged, merged_rows) = run(dir.path(), "merged", lo, seq, rows.clone(), page_size);
            let replaces: Vec<Arc<Run>> = runs[kept..].iter().rev().map(|(r, _)| r.clone()).collect();
            let after = DeltaView::after_flush(Some(&view), &replaces, &merged, &merged_rows, &memory).unwrap();
            after.check().unwrap();
            let mut remaining: Vec<Arc<Run>> = vec![merged.clone()];
            remaining.extend(runs[..kept].iter().rev().map(|(r, _)| r.clone()));
            let rebuilt = DeltaView::from_runs(&remaining, &memory).unwrap();
            prop_assert_eq!(after.entries(), rebuilt.entries());
            let mut want_specs = specs[..kept].to_vec();
            want_specs.push((rows, lo, seq));
            let want = expected(&want_specs);
            prop_assert_eq!(keys_of(&after), want.clone());
            check_seeks(&after, &want, &targets);
            check_page_mates(&after, page_size as u64);
            prop_assert_eq!(after.runs().count(), kept + 1);
            prop_assert!(after.holds(&merged) && replaces.iter().all(|r| !after.holds(r)));

            drop((view, scratch, after, rebuilt));
            prop_assert_eq!(memory.bytes(), 0);
        }
    }

    #[test]
    fn should_build_a_first_view_and_reject_replacing_without_one() {
        let dir = tempfile::tempdir().unwrap();
        let memory = Arc::new(ViewMemory::default());
        let keys: Vec<(Vec<u8>, u64)> = (0..100u32)
            .map(|i| (format!("key{i:06}").into_bytes(), i as u64 + 1))
            .collect();
        let (r, rows) = run(dir.path(), "a", 0, 100, keys, 512);
        let first = DeltaView::after_flush(None, &[], &r, &rows, &memory).unwrap();
        first.check().unwrap();
        assert_eq!(first.len(), 100);
        assert!(first.has_visible(1) && !first.has_visible(0));
        assert!(
            DeltaView::after_flush(None, std::slice::from_ref(&r), &r, &rows, &memory).is_err()
        );
        let (other, other_rows) = run(dir.path(), "b", 100, 101, vec![(b"k".to_vec(), 101)], 512);
        assert!(first.with_run(&other, &rows).is_err());
        let both = first.with_run(&other, &other_rows).unwrap();
        both.check().unwrap();
        assert_eq!(both.without(&[other, r]).unwrap().map(|v| v.len()), None);
    }

    #[test]
    fn should_count_its_memory_until_it_drops() {
        let dir = tempfile::tempdir().unwrap();
        let memory = Arc::new(ViewMemory::default());
        let keys: Vec<(Vec<u8>, u64)> = (0..1000u32)
            .map(|i| (format!("key{i:06}").into_bytes(), i as u64 + 1))
            .collect();
        let (r, _) = run(dir.path(), "a", 0, 1000, keys, 4096);
        let view = DeltaView::from_runs(&[r], &memory).unwrap();
        assert!(view.bytes() >= 7 * 1000);
        assert_eq!(memory.bytes(), view.bytes());
        drop(view);
        assert_eq!(memory.bytes(), 0);
    }
}
