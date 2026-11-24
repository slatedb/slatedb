//! Write buffers for cached ranges.
//!
//! A [`Delta`] stores versions above sequence number `lo` in a single-writer [`Buffer`]. Its
//! `sealed_at` word controls reader visibility. Each range keeps one active delta and zero or
//! more sealed deltas awaiting flush. Large deltas build a Bloom filter so point lookups skip
//! absent keys in one probe. [`DeltaMemory`] tracks exact chunk and filter bytes under the cap.

use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use bytes::Bytes;
use slatedb_common::metrics::GaugeFn;

use super::buffer::{Buffer, Cursor, Kind, Version, Writer, FIRST_CHUNK};
use super::file::copy_key_value;
use super::filter::{blocks_for, hash_key, SharedFilter, BITS_PER_KEY, BLOCK_BYTES};
use super::writer::Padded;
use crate::bytes_range::BytesRange;
use crate::error::SlateDBError;
use crate::iter::RowEntryIterator;
use crate::types::{RowEntry, ValueDeletable};

/// How the deltas of one cache size themselves. See [`crate::RangeCacheTuning`].
#[derive(Clone, Copy, Debug)]
pub(super) struct DeltaShape {
    /// The chunk bytes at which the writer seals an active delta.
    pub(super) seal_bytes: u64,
    /// The chunk bytes from which a delta has a filter. `u64::MAX` turns filters off.
    pub(super) filter_from: u64,
}

impl DeltaShape {
    /// The first chunk of a delta: at most a quarter of the seal size, so that a delta with a
    /// small seal size does not seal at its first version, and the buffer's own size otherwise.
    fn first_chunk(&self) -> usize {
        usize::try_from(self.seal_bytes / 4)
            .unwrap_or(usize::MAX)
            .checked_next_power_of_two()
            .unwrap_or(FIRST_CHUNK)
            .min(FIRST_CHUNK)
    }
}

/// Total heap memory held by live deltas (chunks and filters) across one cache.
pub(super) struct DeltaMemory {
    bytes: Padded<AtomicU64>,
    gauge: Arc<dyn GaugeFn>,
}

impl DeltaMemory {
    pub(super) fn new(gauge: Arc<dyn GaugeFn>) -> Arc<Self> {
        Arc::new(Self {
            bytes: Padded(AtomicU64::new(0)),
            gauge,
        })
    }

    /// A count that reports to no gauge, for tests.
    #[cfg(test)]
    pub(super) fn detached() -> Arc<Self> {
        Self::new(
            slatedb_common::metrics::MetricsRecorderHelper::noop()
                .gauge("range_cache_test_delta_memory")
                .register(),
        )
    }

    /// The bytes of the chunks and filters of all live deltas.
    pub(super) fn bytes(&self) -> u64 {
        self.bytes.0.load(Ordering::Relaxed)
    }

    /// Sets the gauge to the current count.
    pub(super) fn publish(&self) {
        self.gauge.set(self.bytes() as i64);
    }

    /// Returns true if `bytes` more keep the count at or below `cap`. Only the writer adds to the
    /// count, so the answer holds until it adds: other threads only subtract.
    fn fits(&self, bytes: u64, cap: u64) -> bool {
        self.bytes()
            .checked_add(bytes)
            .is_some_and(|total| total <= cap)
    }

    fn add(&self, bytes: u64) {
        let total = self.bytes.0.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.gauge.set(total as i64);
    }

    fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let total = self.bytes.0.fetch_sub(bytes, Ordering::Relaxed) - bytes;
        self.gauge.set(total as i64);
    }
}

/// Versions that the writer applied to a range above a sequence number. See the module
/// documentation.
pub(super) struct Delta {
    buffer: Buffer,
    /// Every version has a greater sequence number.
    lo: u64,
    shape: DeltaShape,
    /// The filter of the delta's keys, once the delta is large. Only the writer sets it.
    filter: OnceLock<SharedFilter>,
    memory: Arc<DeltaMemory>,
}

impl Delta {
    /// An empty delta for versions above `lo`, whose chunks and filter count in `memory`. It
    /// allocates nothing until the first version arrives.
    pub(super) fn new(memory: &Arc<DeltaMemory>, lo: u64, shape: DeltaShape) -> Self {
        Self {
            buffer: Buffer::with_first_chunk(shape.first_chunk()),
            lo,
            shape,
            filter: OnceLock::new(),
            memory: memory.clone(),
        }
    }

    /// An empty delta with the default shape, for tests.
    #[cfg(test)]
    pub(super) fn for_test(memory: &Arc<DeltaMemory>, lo: u64) -> Self {
        Self::new(
            memory,
            lo,
            DeltaShape {
                seal_bytes: 4 * 1024 * 1024,
                filter_from: u64::MAX,
            },
        )
    }

    /// Every version of the delta has a sequence number above this one.
    pub(super) fn lo(&self) -> u64 {
        self.lo
    }

    /// The cache applies no write with a greater sequence number to this delta. It is
    /// `u64::MAX` while the delta is active.
    pub(super) fn sealed_at(&self) -> &AtomicU64 {
        self.buffer.sealed_at()
    }

    /// The estimated size of the versions, as the memtable counts its rows. The `delta_bytes`
    /// gauge uses it.
    pub(super) fn size(&self) -> usize {
        self.buffer.estimated_bytes() as usize
    }

    /// The bytes of the delta's chunks. The seal threshold applies to them.
    pub(super) fn allocated_bytes(&self) -> u64 {
        self.buffer.allocated_bytes()
    }

    /// The memory that the delta holds: its chunks and its filter.
    pub(super) fn memory_bytes(&self) -> u64 {
        self.buffer.allocated_bytes() + self.filter_bytes()
    }

    /// The bytes of the delta's filter, or 0.
    pub(super) fn filter_bytes(&self) -> u64 {
        self.filter.get().map_or(0, |f| f.bytes() as u64)
    }

    /// The delta's filter, if it has one. A reader probes it after it loads its sequence number.
    #[inline]
    pub(super) fn filter(&self) -> Option<&SharedFilter> {
        self.filter.get()
    }

    /// The number of versions.
    #[cfg(test)]
    pub(super) fn len(&self) -> u64 {
        self.buffer.len()
    }

    /// Returns true if the delta holds no version. A reader that loaded its sequence number
    /// before this check, and finds the delta empty, can skip it: the writer puts each version
    /// in its delta before it commits it.
    pub(super) fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub(super) fn buffer(&self) -> &Buffer {
        &self.buffer
    }

    /// The newest version of `key` at or below `seq`, read in place.
    pub(super) fn newest(&self, key: &[u8], seq: u64) -> Option<Version<'_>> {
        self.buffer.get(key, seq)
    }

    /// The writer of the delta. Only the batch writer task appends.
    pub(super) fn writer(&self) -> DeltaWriter<'_> {
        DeltaWriter {
            writer: self.buffer.writer(),
            delta: self,
        }
    }

    /// The keys that a filter created now should hold: the versions at the delta's size when it
    /// seals, projected from its versions so far, at most a sixteenth of the seal size in filter
    /// bytes.
    fn filter_keys(&self) -> usize {
        let (len, allocated) = (self.buffer.len(), self.allocated_bytes().max(1));
        let projected =
            (len as u128 * self.shape.seal_bytes.max(allocated) as u128 / allocated as u128) as u64;
        let most = self.shape.seal_bytes / 16 * 8 / BITS_PER_KEY as u64;
        projected.min(most).max(len).max(1) as usize
    }
}

impl Drop for Delta {
    fn drop(&mut self) {
        self.memory.release(self.memory_bytes());
    }
}

/// Appends the rows of a batch to a delta. See [`Delta::writer`].
pub(super) struct DeltaWriter<'a> {
    writer: Writer<'a>,
    delta: &'a Delta,
}

impl DeltaWriter<'_> {
    /// Copies `entry` into the delta. The cache holds no row with an expire time.
    #[cfg(test)]
    pub(super) fn append(&mut self, entry: &RowEntry) {
        assert!(
            self.append_within(entry, u64::MAX),
            "an append without a cap"
        );
    }

    /// Appends `entry` if total delta memory stays at or below `cap`; returns false unchanged
    /// otherwise. Sets Bloom filter bits before linking the node.
    pub(super) fn append_within(&mut self, entry: &RowEntry, cap: u64) -> bool {
        debug_assert!(entry.expire_ts.is_none(), "a delta row with an expire time");
        let (kind, value): (Kind, &[u8]) = match &entry.value {
            ValueDeletable::Value(v) => (Kind::Value, v),
            ValueDeletable::Merge(v) => (Kind::Merge, v),
            ValueDeletable::Tombstone => (Kind::Tombstone, &[]),
        };
        let delta = self.delta;
        let memory = &*delta.memory;
        let filter = delta.filter.get();
        let fits = |grown: u64| {
            if grown > 0 && !memory.fits(grown, cap) {
                return false;
            }
            if let Some(filter) = filter {
                filter.insert(hash_key(&entry.key));
            }
            true
        };
        let Some(grown) =
            self.writer
                .try_append(&entry.key, entry.seq, kind, entry.create_ts, value, fits)
        else {
            return false;
        };
        if grown > 0 {
            memory.add(grown);
            if filter.is_none() && delta.allocated_bytes() >= delta.shape.filter_from {
                self.add_filter(cap);
            }
        }
        true
    }

    /// Gives the delta a filter of its keys, if the filter fits under `cap`. It runs on the
    /// writer, once per delta, when a new chunk takes the delta past the filter's size.
    fn add_filter(&self, cap: u64) {
        let delta = self.delta;
        let keys = delta.filter_keys();
        let bytes = (blocks_for(keys) * BLOCK_BYTES) as u64;
        if !delta.memory.fits(bytes, cap) {
            return;
        }
        let filter = SharedFilter::with_capacity(keys);
        let buffer = &delta.buffer;
        let mut cursor = buffer.seek(Bound::Unbounded);
        while let Some(version) = buffer.version(&cursor) {
            filter.insert(hash_key(version.key));
            buffer.advance(&mut cursor);
        }
        debug_assert_eq!(filter.bytes() as u64, bytes);
        delta.memory.add(bytes);
        // Publishes the filter with its bits: a reader that finds it sees them.
        if delta.filter.set(filter).is_err() {
            unreachable!("only the writer sets a delta's filter");
        }
    }
}

/// Copies a version out of a delta into a row, with one allocation for its key and value.
pub(super) fn to_row(version: &Version<'_>) -> RowEntry {
    let (key, value) = copy_key_value(version.key, version.value);
    RowEntry {
        key,
        value: match version.kind {
            Kind::Value => ValueDeletable::Value(value),
            Kind::Merge => ValueDeletable::Merge(value),
            Kind::Tombstone => ValueDeletable::Tombstone,
        },
        seq: version.seq,
        create_ts: version.create_ts,
        expire_ts: None,
    }
}

/// Returns true if `key` is past `end`.
pub(super) fn past_end(end: &Bound<Bytes>, key: &[u8]) -> bool {
    match end {
        Bound::Unbounded => false,
        Bound::Included(end) => key > end.as_ref(),
        Bound::Excluded(end) => key >= end.as_ref(),
    }
}

/// Every version of a delta in a key range, key ascending and newest first, for the normal
/// iterator stack, which filters them by sequence number. Rebuilds and merge resolution use it.
/// It holds the delta, so it outlives the layers that it came from.
pub(super) struct DeltaIterator {
    delta: Arc<Delta>,
    cursor: Cursor,
    end: Bound<Bytes>,
}

impl DeltaIterator {
    pub(super) fn new(delta: Arc<Delta>, range: &BytesRange) -> Self {
        use std::ops::RangeBounds;
        let start = range.start_bound().map(|key| key.as_ref());
        let cursor = delta.buffer.seek(start);
        Self {
            end: range.end_bound().cloned(),
            delta,
            cursor,
        }
    }

    fn current(&self) -> Option<Version<'_>> {
        self.delta
            .buffer
            .version(&self.cursor)
            .filter(|version| !past_end(&self.end, version.key))
    }
}

#[async_trait]
impl RowEntryIterator for DeltaIterator {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        let Some(row) = self.current().map(|version| to_row(&version)) else {
            return Ok(None);
        };
        self.delta.buffer.advance(&mut self.cursor);
        Ok(Some(row))
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        // Never move backwards, and never past the end again.
        if self.current().is_some_and(|version| version.key < next_key) {
            self.cursor = self.delta.buffer.seek(Bound::Included(next_key));
        }
        // Keep in-memory seeking cooperative, as the memtable's iterator does.
        tokio::task::coop::consume_budget().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytes_range::BytesRange;
    use crate::mem_table::KVTable;
    use crate::types::RowEntry;
    use std::ops::Bound::{Excluded, Included, Unbounded};

    fn row(key: &str, seq: u64, value: Option<&str>) -> RowEntry {
        RowEntry::new(
            Bytes::copy_from_slice(key.as_bytes()),
            match value {
                Some(v) => ValueDeletable::Value(Bytes::copy_from_slice(v.as_bytes())),
                None => ValueDeletable::Tombstone,
            },
            seq,
            Some(seq as i64),
            None,
        )
    }

    async fn drain(iter: &mut dyn RowEntryIterator) -> Vec<RowEntry> {
        let mut out = Vec::new();
        while let Some(row) = iter.next().await.unwrap() {
            out.push(row);
        }
        out
    }

    /// The iterator returns what the memtable's iterator returned over the same rows, also
    /// after a seek.
    #[tokio::test]
    async fn should_iterate_as_the_memtable_does() {
        let memory = DeltaMemory::detached();
        let delta = Arc::new(Delta::for_test(&memory, 0));
        let table = KVTable::new();
        let rows = [
            row("a", 3, Some("a3")),
            row("b", 1, Some("b1")),
            row("b", 4, None),
            row("c", 2, Some("c2")),
            row("c", 5, Some("c5")),
            row("d", 6, Some("d6")),
        ];
        let mut writer = delta.writer();
        for r in &rows {
            writer.append(r);
            table.put(r.clone());
        }
        drop(writer);
        assert_eq!(delta.size(), table.metadata().entries_size_in_bytes);
        assert_eq!(delta.len() as usize, table.metadata().entry_num);
        let b = |s: &str| Bytes::copy_from_slice(s.as_bytes());
        let ranges = [
            BytesRange::unbounded(),
            BytesRange::new(Included(b("b")), Excluded(b("d"))),
            BytesRange::new(Excluded(b("b")), Included(b("c"))),
            BytesRange::new(Included(b("bb")), Unbounded),
        ];
        for range in ranges {
            let mut ours = DeltaIterator::new(delta.clone(), &range);
            let mut theirs = table.range(
                range.clone(),
                crate::iter::IterationOrder::Ascending,
                crate::read_trace::ReadTrace::none(),
            );
            assert_eq!(
                drain(&mut ours).await,
                drain(&mut theirs).await,
                "{range:?}"
            );
            for target in ["", "b", "bz", "c", "z"] {
                let mut ours = DeltaIterator::new(delta.clone(), &range);
                let mut theirs = table.range(
                    range.clone(),
                    crate::iter::IterationOrder::Ascending,
                    crate::read_trace::ReadTrace::none(),
                );
                // One row, then a seek, then the rest.
                assert_eq!(ours.next().await.unwrap(), theirs.next().await.unwrap());
                ours.seek(target.as_bytes()).await.unwrap();
                theirs.seek(target.as_bytes()).await.unwrap();
                assert_eq!(
                    drain(&mut ours).await,
                    drain(&mut theirs).await,
                    "{range:?} after a seek to {target:?}"
                );
            }
        }
    }

    fn shaped(memory: &Arc<DeltaMemory>, filter_from: u64) -> Delta {
        Delta::new(
            memory,
            0,
            DeltaShape {
                seal_bytes: 1024 * 1024,
                filter_from,
            },
        )
    }

    fn big_row(i: u64) -> RowEntry {
        RowEntry::new(
            Bytes::from(format!("k{i:08}")),
            ValueDeletable::Value(Bytes::from(vec![b'v'; 200])),
            i + 1,
            None,
            None,
        )
    }

    /// An append that would take the memory past the cap is refused before it changes anything,
    /// and every other append fits: the memory never passes the cap and stops close to it.
    #[test]
    fn should_refuse_an_append_that_does_not_fit_under_the_cap() {
        let memory = DeltaMemory::detached();
        let other = shaped(&memory, u64::MAX);
        other.writer().append(&big_row(0));
        let delta = shaped(&memory, u64::MAX);
        let cap = 64 * 1024;
        let mut writer = delta.writer();
        let mut i = 0;
        loop {
            let (len, bytes) = (delta.len(), memory.bytes());
            if !writer.append_within(&big_row(i), cap) {
                assert_eq!((delta.len(), memory.bytes()), (len, bytes));
                break;
            }
            assert!(memory.bytes() <= cap);
            i += 1;
        }
        // Chunks double from 4 KiB: with the other delta's 4 KiB, chunks of 4, 8, 16 and 32 KiB
        // reach the cap exactly, and the next one, 64 KiB, does not fit.
        assert_eq!(memory.bytes(), cap);
        // A version that fits in the current chunk's room needs no new chunk, so it is not
        // refused at the cap.
        let tiny = RowEntry::new_tombstone(b"k", 1_000_000);
        let room = writer.append_within(&tiny, cap);
        assert_eq!(memory.bytes(), cap, "appended into the room: {room}");
        drop(writer);
        drop(other);
        drop(delta);
        assert_eq!(memory.bytes(), 0);
    }

    /// A delta gets a filter when its chunks reach the filter size: the filter holds every key
    /// that the delta held then and every key appended after, and its bytes count in the memory
    /// until the delta drops. A filter that does not fit under the cap is not made.
    #[test]
    fn should_add_a_filter_when_the_delta_grows() {
        let memory = DeltaMemory::detached();
        let delta = shaped(&memory, 16 * 1024);
        let mut writer = delta.writer();
        let mut i = 0;
        while delta.allocated_bytes() < 16 * 1024 {
            assert!(delta.filter().is_none());
            writer.append(&big_row(i));
            i += 1;
        }
        let filter = delta.filter().expect("a filter at the filter size");
        for j in i..i + 500 {
            writer.append(&big_row(j));
        }
        drop(writer);
        for j in 0..i + 500 {
            assert!(
                filter.may_contain(hash_key(big_row(j).key.as_ref())),
                "key {j}"
            );
        }
        assert!(delta.filter_bytes() > 0);
        assert_eq!(
            memory.bytes(),
            delta.allocated_bytes() + delta.filter_bytes()
        );
        assert_eq!(delta.memory_bytes(), memory.bytes());
        drop(delta);
        assert_eq!(memory.bytes(), 0);

        // The same delta under a cap that holds its chunks but not its filter.
        let small = shaped(&memory, 16 * 1024);
        let mut writer = small.writer();
        let mut i = 0;
        while small.allocated_bytes() < 16 * 1024 {
            assert!(writer.append_within(&big_row(i), 28 * 1024));
            i += 1;
        }
        assert!(small.filter().is_none());
        assert_eq!(memory.bytes(), small.allocated_bytes());
    }

    /// The cache's count of delta memory holds each delta's chunks until its last holder drops it.
    #[test]
    fn should_count_delta_memory_until_the_last_holder_drops_it() {
        let memory = DeltaMemory::detached();
        let first = Arc::new(Delta::for_test(&memory, 0));
        let second = Arc::new(Delta::for_test(&memory, 0));
        assert_eq!(memory.bytes(), 0);
        for (i, delta) in [&first, &second].into_iter().enumerate() {
            let mut writer = delta.writer();
            for j in 0..2_000u64 {
                writer.append(&row(
                    &format!("k{j:05}"),
                    i as u64 * 10_000 + j,
                    Some("value"),
                ));
            }
        }
        assert!(first.allocated_bytes() > 0);
        assert_eq!(
            memory.bytes(),
            first.allocated_bytes() + second.allocated_bytes()
        );
        // A scan holds the first delta after the layers dropped it.
        let held = DeltaIterator::new(first.clone(), &BytesRange::unbounded());
        let first_bytes = first.allocated_bytes();
        drop(first);
        drop(second);
        assert_eq!(memory.bytes(), first_bytes);
        drop(held);
        assert_eq!(memory.bytes(), 0);
    }
}
