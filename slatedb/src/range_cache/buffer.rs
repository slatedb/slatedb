//! Single-writer, lock-free multi-reader skiplist ordered by `(key asc, seq desc)`.
//!
//! Nodes live in arena chunks freed only on drop. Soundness invariants cited by `unsafe` blocks:
//! - **R1, one writer**: [`Buffer::writer`] enforces exclusivity via `Acquire`/`Release`.
//! - **R2, complete before linked**: nodes are fully initialized before `Release`-linking from
//!   level 0 up; readers load links with `Acquire`.
//! - **R3, immutable once linked**: linked headers, keys, and values never mutate or unlink.
//! - **R4, freed at drop**: chunks stay allocated while any `&Buffer` lives.
//! - **R5, checked cursors**: [`Cursor`] verifies the owning buffer's unique process-wide id.

use std::alloc::{self, Layout};
use std::cell::UnsafeCell;
use std::cmp::Ordering as KeyOrder;
use std::ops::Bound;
use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};

/// The levels of the skiplist. Each level holds about a quarter of the nodes of the one below,
/// so 12 levels index 4^12 (16.7 million) versions well, far more than a delta holds before a
/// rebuild.
const MAX_HEIGHT: usize = 12;
/// The first chunk of a buffer. Each later chunk doubles, up to [`MAX_CHUNK`], so a buffer that
/// holds a few versions allocates little. [`Buffer::with_first_chunk`] can start smaller, down to
/// [`MIN_FIRST_CHUNK`].
pub(super) const FIRST_CHUNK: usize = 4 * 1024;
pub(super) const MIN_FIRST_CHUNK: usize = 512;
const MAX_CHUNK: usize = 64 * 1024;
/// A node larger than this gets a chunk of its own, so that a large value does not waste the
/// rest of a regular chunk.
const LARGE_NODE: usize = MAX_CHUNK / 4;

const KIND_MASK: u8 = 0b11;
const KIND_VALUE: u8 = 0;
const KIND_TOMBSTONE: u8 = 1;
const KIND_MERGE: u8 = 2;
const HAS_CREATE_TS: u8 = 1 << 2;
/// The node replaces the nodes after it on level 0 that have the same key and sequence number:
/// they were appended earlier, and readers skip them.
const REPLACES: u8 = 1 << 3;

/// What a version holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    Value,
    Tombstone,
    Merge,
}

/// A version in a buffer, borrowed from it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Version<'a> {
    pub(super) key: &'a [u8],
    pub(super) seq: u64,
    pub(super) kind: Kind,
    pub(super) create_ts: Option<i64>,
    /// Empty for a tombstone.
    pub(super) value: &'a [u8],
}

impl Version<'_> {
    /// The bytes that SlateDB's `RowEntry::estimated_size` counts for this version, which has no
    /// expire time. The size gauge of a delta adds these, as the memtable's does.
    pub(super) fn estimated_size(&self) -> u64 {
        estimated_size(self.key.len(), self.value.len(), self.create_ts.is_some())
    }
}

fn estimated_size(key_len: usize, value_len: usize, create_ts: bool) -> u64 {
    (key_len + value_len + 8 + if create_ts { 8 } else { 0 }) as u64
}

/// The header of a node. The node's links, `height` of them, follow it, then its key, then its
/// value. Nodes are 8-byte aligned, and so is the header's size.
#[repr(C)]
struct Node {
    seq: u64,
    create_ts: i64,
    value_len: u64,
    key_len: u32,
    height: u8,
    flags: u8,
    _pad: [u8; 2],
}

const HEADER: usize = size_of::<Node>();
const LINK: usize = size_of::<AtomicPtr<Node>>();
const ALIGN: usize = align_of::<Node>();
// Links start right after the header, so the header's size must keep them aligned.
const _: () =
    assert!(HEADER.is_multiple_of(ALIGN) && ALIGN.is_multiple_of(align_of::<AtomicPtr<Node>>()));

/// The header that starts each chunk. Chunks form a list from the newest, which the buffer frees
/// when it drops.
#[repr(C)]
struct ChunkHeader {
    prev: *mut ChunkHeader,
    len: usize,
}

const CHUNK_HEADER: usize = size_of::<ChunkHeader>().next_multiple_of(ALIGN);

/// The size of a node with `height` links, rounded up to the alignment of nodes.
fn node_size(height: usize, key_len: usize, value_len: usize) -> usize {
    HEADER
        .checked_add(height * LINK)
        .and_then(|n| n.checked_add(key_len))
        .and_then(|n| n.checked_add(value_len))
        .and_then(|n| n.checked_next_multiple_of(ALIGN))
        .expect("a range cache version fits in memory")
}

/// The link of `node` on `level`.
///
/// # Safety
///
/// `node` points to a node that a writer wrote completely (R2), in a buffer that is still
/// allocated (R4), and `level` is below the node's height.
unsafe fn link<'a>(node: *const Node, level: usize) -> &'a AtomicPtr<Node> {
    // SAFETY: the caller's contract. The links follow the header, inside the node's chunk, and
    // `node` carries the provenance of the whole chunk. They were initialized before the node
    // was published, and are only ever accessed atomically after that.
    unsafe {
        &*node
            .cast::<u8>()
            .add(HEADER + level * LINK)
            .cast::<AtomicPtr<Node>>()
    }
}

/// The header of `node`.
///
/// # Safety
///
/// As for [`link`].
unsafe fn header<'a>(node: *const Node) -> &'a Node {
    // SAFETY: the caller's contract. A linked header never changes (R3).
    unsafe { &*node }
}

/// The key of `node`.
///
/// # Safety
///
/// As for [`link`].
unsafe fn key<'a>(node: *const Node) -> &'a [u8] {
    // SAFETY: the caller's contract. The key follows the links, inside the node's chunk, and a
    // linked node's key never changes (R3).
    unsafe {
        let h = header(node);
        let start = node.cast::<u8>().add(HEADER + h.height as usize * LINK);
        slice::from_raw_parts(start, h.key_len as usize)
    }
}

/// The version that `node` holds.
///
/// # Safety
///
/// As for [`link`].
unsafe fn version<'a>(node: *const Node) -> Version<'a> {
    // SAFETY: the caller's contract. The value follows the key, inside the node's chunk, and a
    // linked node's bytes never change (R3).
    unsafe {
        let h = header(node);
        let key = key(node);
        let value = slice::from_raw_parts(key.as_ptr().add(key.len()), h.value_len as usize);
        Version {
            key,
            seq: h.seq,
            kind: match h.flags & KIND_MASK {
                KIND_TOMBSTONE => Kind::Tombstone,
                KIND_MERGE => Kind::Merge,
                _ => Kind::Value,
            },
            create_ts: (h.flags & HAS_CREATE_TS != 0).then_some(h.create_ts),
            value,
        }
    }
}

/// Returns true if `node` comes before the newest version of `target` at or below `seq`: its key
/// is smaller, or it is a newer version of the same key.
///
/// # Safety
///
/// As for [`link`].
unsafe fn before_version(node: *const Node, target: &[u8], seq: u64) -> bool {
    // SAFETY: the caller's contract.
    unsafe {
        match key(node).cmp(target) {
            KeyOrder::Less => true,
            KeyOrder::Equal => header(node).seq > seq,
            KeyOrder::Greater => false,
        }
    }
}

/// Returns true if `node` holds the version of `target` at exactly `seq`.
///
/// # Safety
///
/// As for [`link`].
unsafe fn same_version(node: *const Node, target: &[u8], seq: u64) -> bool {
    // SAFETY: the caller's contract.
    unsafe { header(node).seq == seq && key(node) == target }
}

/// The ids of buffers. No two buffers of a process share one (R5).
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// A value on its own cache line. Apple silicon has 128-byte lines.
#[repr(align(128))]
struct CacheLine<T>(T);

/// A sorted map of versions. See the module documentation.
pub(super) struct Buffer {
    /// The fields that every reader loads, on one cache line that the writer rarely stores to.
    read: CacheLine<ReadSide>,
    /// The fields that the writer stores to for each version.
    write: CacheLine<WriteSide>,
}

struct ReadSide {
    /// The first node of each level, or null.
    head: [AtomicPtr<Node>; MAX_HEIGHT],
    /// The levels that hold a node. Searches start at the highest one.
    height: AtomicUsize,
    /// The buffer's identity for its cursors (R5).
    id: u64,
    /// The validity word of the range cache: the highest sequence number that the buffer holds
    /// completely, or `u64::MAX` while the cache may still append to it. The buffer itself does
    /// not use it; it sits here so that a reader's check of it touches the line it loads anyway.
    sealed_at: AtomicU64,
}

struct WriteSide {
    /// True while a [`Writer`] lives (R1).
    busy: AtomicBool,
    /// The versions that readers can find: appends, less those that replaced an earlier one.
    len: AtomicU64,
    /// The sum of [`Version::estimated_size`] over those versions.
    estimated: AtomicU64,
    /// The bytes of all chunks.
    allocated: AtomicU64,
    /// Only a writer, or the drop, touches it (R1, R4).
    arena: UnsafeCell<Arena>,
}

/// The chunk state. Only a writer touches it.
struct Arena {
    /// The newest chunk, or null.
    chunks: *mut ChunkHeader,
    /// The next free byte of the current regular chunk, and the bytes left after it.
    next: *mut u8,
    room: usize,
    /// The size of the next regular chunk.
    next_chunk: usize,
    /// The state of the generator of node heights.
    rng: u64,
}

// SAFETY: the buffer owns its chunks; nothing in it is tied to the thread that made it.
unsafe impl Send for Buffer {}
// SAFETY: shared access is reads of atomics and of linked nodes, which never change (R3), and
// the arena, which only the one writer touches (R1).
unsafe impl Sync for Buffer {}

impl Default for Buffer {
    fn default() -> Self {
        Self::new()
    }
}

impl Buffer {
    /// Returns an empty buffer. It allocates no chunk until the first append.
    pub(super) fn new() -> Self {
        Self::with_first_chunk(FIRST_CHUNK)
    }

    /// Returns an empty buffer whose first chunk is `first_chunk` bytes, clamped to
    /// `MIN_FIRST_CHUNK..=FIRST_CHUNK`. Later chunks double as usual.
    pub(super) fn with_first_chunk(first_chunk: usize) -> Self {
        let first_chunk = first_chunk.clamp(MIN_FIRST_CHUNK, FIRST_CHUNK);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            read: CacheLine(ReadSide {
                head: std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut())),
                height: AtomicUsize::new(0),
                id,
                sealed_at: AtomicU64::new(u64::MAX),
            }),
            write: CacheLine(WriteSide {
                busy: AtomicBool::new(false),
                len: AtomicU64::new(0),
                estimated: AtomicU64::new(0),
                allocated: AtomicU64::new(0),
                arena: UnsafeCell::new(Arena {
                    chunks: ptr::null_mut(),
                    next: ptr::null_mut(),
                    room: 0,
                    next_chunk: first_chunk,
                    // Any odd seed works for the generator.
                    rng: id.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1,
                }),
            }),
        }
    }

    /// The range cache's validity word. See [`ReadSide::sealed_at`].
    pub(super) fn sealed_at(&self) -> &AtomicU64 {
        &self.read.0.sealed_at
    }

    /// Returns true if the buffer holds no version that a reader can see. It reads the first link
    /// of level 0, which changes only when a version with a new smallest key arrives, so a check
    /// of a written buffer seldom misses the cache.
    pub(super) fn is_empty(&self) -> bool {
        self.read.0.head[0].load(Ordering::Acquire).is_null()
    }

    /// Returns the number of visible versions. Reads check [`Buffer::is_empty`] instead to avoid
    /// loading this writer-updated counter.
    #[allow(dead_code)]
    pub(super) fn len(&self) -> u64 {
        self.write.0.len.load(Ordering::Acquire)
    }

    /// The sum of [`Version::estimated_size`] over the versions that readers can find.
    pub(super) fn estimated_bytes(&self) -> u64 {
        self.write.0.estimated.load(Ordering::Relaxed)
    }

    /// The bytes of the chunks that the buffer holds.
    pub(super) fn allocated_bytes(&self) -> u64 {
        self.write.0.allocated.load(Ordering::Relaxed)
    }

    /// Returns the first node, on level 0, for which `before` is false. `before` must be true for
    /// a prefix of the nodes in order and false after it.
    fn search(&self, before: impl Fn(*const Node) -> bool) -> *const Node {
        let read = &self.read.0;
        // A stale height only makes the search start lower; every level is a sorted list.
        let mut level = read.height.load(Ordering::Relaxed);
        let mut pred: *const Node = ptr::null();
        let mut found: *const Node = ptr::null();
        while level > 0 {
            level -= 1;
            let mut at = if pred.is_null() {
                &read.head[level]
            } else {
                // SAFETY: `pred` came from an `Acquire` load of a link of this buffer (R2, R4),
                // and it was found on a level above this one, so its height exceeds `level`.
                unsafe { link(pred, level) }
            };
            loop {
                // Pairs with the writer's `Release` store of the link (R2).
                let next: *const Node = at.load(Ordering::Acquire);
                if next.is_null() || !before(next) {
                    found = next;
                    break;
                }
                pred = next;
                // SAFETY: as above; `next` is on this level, so its height exceeds `level`.
                at = unsafe { link(next, level) };
            }
        }
        found
    }

    /// Returns the newest version of `key` with a sequence number at or below `max_seq`.
    pub(super) fn get(&self, key: &[u8], max_seq: u64) -> Option<Version<'_>> {
        // SAFETY: `search` passes nodes of this buffer that it loaded from links (R2, R4).
        let node = self.search(|n| unsafe { before_version(n, key, max_seq) });
        if node.is_null() {
            return None;
        }
        // SAFETY: a node of this buffer that `search` loaded from a link, which `&self` keeps
        // allocated (R4).
        let found = unsafe { version(node) };
        (found.key == key).then_some(found)
    }

    /// Returns a cursor at the first version whose key is in `start`, newest first.
    pub(super) fn seek(&self, start: Bound<&[u8]>) -> Cursor {
        let node = match start {
            Bound::Unbounded => self.read.0.head[0].load(Ordering::Acquire),
            // SAFETY (both arms): `search` passes nodes of this buffer that it loaded from links.
            Bound::Included(k) => self.search(|n| unsafe { key(n) } < k),
            Bound::Excluded(k) => self.search(|n| unsafe { key(n) } <= k),
        };
        Cursor {
            buffer: self.read.0.id,
            node,
        }
    }

    fn check(&self, cursor: &Cursor) {
        assert_eq!(
            cursor.buffer, self.read.0.id,
            "a range cache cursor read through a buffer other than its own"
        );
    }

    /// The version at `cursor`, or `None` at the end.
    pub(super) fn version(&self, cursor: &Cursor) -> Option<Version<'_>> {
        self.check(cursor);
        // SAFETY: the cursor belongs to this buffer (R5), so its node is a node of this buffer
        // that it loaded from a link, and `&self` keeps it allocated (R4).
        (!cursor.node.is_null()).then(|| unsafe { version(cursor.node) })
    }

    /// Moves `cursor` to the next version. It skips versions that a later append replaced.
    pub(super) fn advance(&self, cursor: &mut Cursor) {
        self.check(cursor);
        let node = cursor.node;
        if node.is_null() {
            return;
        }
        // SAFETY: as in `version`. Every node has a link on level 0.
        unsafe {
            let mut next: *const Node = link(node, 0).load(Ordering::Acquire);
            if header(node).flags & REPLACES != 0 {
                let (k, seq) = (key(node), header(node).seq);
                while !next.is_null() && same_version(next, k, seq) {
                    next = link(next, 0).load(Ordering::Acquire);
                }
            }
            cursor.node = next;
        }
    }

    /// Returns the writer of the buffer.
    ///
    /// # Panics
    ///
    /// If another writer of this buffer lives (R1). The batch writer is the only writer of a
    /// delta, so this is a bug.
    pub(super) fn writer(&self) -> Writer<'_> {
        let claimed = self
            .write
            .0
            .busy
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok();
        assert!(claimed, "a range cache delta has a second writer");
        Writer { buffer: self }
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        let arena = self.write.0.arena.get_mut();
        let mut chunk = arena.chunks;
        while !chunk.is_null() {
            // SAFETY: `chunk` is a chunk of this buffer, allocated in `Arena::chunk` with the
            // length in its header and `ALIGN`, and freed nowhere else. `&mut self` means that no
            // reader or writer holds the buffer (R4).
            unsafe {
                let ChunkHeader { prev, len } = ptr::read(chunk);
                alloc::dealloc(chunk.cast(), Layout::from_size_align_unchecked(len, ALIGN));
                chunk = prev;
            }
        }
    }
}

/// A position in a [`Buffer`]: a version, or the end. Read it through the buffer that made it.
#[derive(Clone, Copy, Debug)]
pub(super) struct Cursor {
    buffer: u64,
    node: *const Node,
}

// SAFETY: a cursor is a plain position. It is dereferenced only through the `&Buffer` that made
// it, which is `Sync` and checked by id (R5).
unsafe impl Send for Cursor {}
// SAFETY: as for `Send`.
unsafe impl Sync for Cursor {}

/// The writer of a [`Buffer`]. See [`Buffer::writer`].
pub(super) struct Writer<'a> {
    buffer: &'a Buffer,
}

impl Writer<'_> {
    /// Appends a version and returns newly allocated chunk bytes (0 if it fit in the current
    /// chunk). A version with an existing `(key, seq)` supersedes the earlier entry.
    #[cfg(test)]
    pub(super) fn append(
        &mut self,
        key: &[u8],
        seq: u64,
        kind: Kind,
        create_ts: Option<i64>,
        value: &[u8],
    ) -> u64 {
        self.try_append(key, seq, kind, create_ts, value, |_| true)
            .expect("an append that fits")
    }

    /// Appends a version only if `fits(growth)` returns true before allocation. Returns `None`
    /// without mutating the buffer if `fits` rejects the growth.
    pub(super) fn try_append(
        &mut self,
        key: &[u8],
        seq: u64,
        kind: Kind,
        create_ts: Option<i64>,
        value: &[u8],
        fits: impl FnOnce(u64) -> bool,
    ) -> Option<u64> {
        let buffer = self.buffer;
        let read = &buffer.read.0;
        let write = &buffer.write.0;
        // SAFETY: this writer holds the role, so nothing else touches the arena (R1).
        let arena = unsafe { &mut *write.arena.get() };
        let height = arena.height();
        let size = node_size(height, key.len(), value.len());
        let growth = arena.growth(size);
        if !fits(growth) {
            return None;
        }

        // On each level, the link to store the node in and the node that it points to now.
        let mut at: [&AtomicPtr<Node>; MAX_HEIGHT] = std::array::from_fn(|l| &read.head[l]);
        let mut succ: [*mut Node; MAX_HEIGHT] = [ptr::null_mut(); MAX_HEIGHT];
        let top = read.height.load(Ordering::Relaxed).max(height);
        let mut pred: *const Node = ptr::null();
        for level in (0..top).rev() {
            let mut here = if pred.is_null() {
                &read.head[level]
            } else {
                // SAFETY: `pred` is a node of this buffer, written by a writer before this one
                // (ordered by R1) and found on a higher level, so its height exceeds `level`.
                unsafe { link(pred, level) }
            };
            loop {
                // Only writers store links, and R1 orders their stores before this load.
                let next = here.load(Ordering::Relaxed);
                // SAFETY: as above, for a node loaded from a link.
                if next.is_null() || !unsafe { before_version(next, key, seq) } {
                    succ[level] = next;
                    break;
                }
                pred = next;
                // SAFETY: as above; `next` is on this level.
                here = unsafe { link(next, level) };
            }
            at[level] = here;
        }
        // The estimated size of the version that this one replaces, if any.
        let replaced = if succ[0].is_null() {
            None
        } else {
            // SAFETY: as above, for a node loaded from a link.
            unsafe { same_version(succ[0], key, seq).then(|| version(succ[0]).estimated_size()) }
        };

        let (memory, grown) = arena.alloc(size);
        debug_assert_eq!(grown, growth, "an allocation other than the one checked");
        let node = memory.cast::<Node>();
        let mut flags = match kind {
            Kind::Value => KIND_VALUE,
            Kind::Tombstone => KIND_TOMBSTONE,
            Kind::Merge => KIND_MERGE,
        };
        if create_ts.is_some() {
            flags |= HAS_CREATE_TS;
        }
        if replaced.is_some() {
            flags |= REPLACES;
        }
        // SAFETY: `memory` is `size` bytes of this buffer's chunk, aligned for a node, that no
        // node uses and nothing can reach yet. The writes stay inside it: `node_size` counts the
        // header, `height` links, the key and the value.
        unsafe {
            ptr::write(
                node,
                Node {
                    seq,
                    create_ts: create_ts.unwrap_or(0),
                    value_len: value.len() as u64,
                    key_len: u32::try_from(key.len()).expect("a key is at most u32::MAX bytes"),
                    height: height as u8,
                    flags,
                    _pad: [0; 2],
                },
            );
            let links = memory.add(HEADER).cast::<AtomicPtr<Node>>();
            for (level, next) in succ.iter().enumerate().take(height) {
                ptr::write(links.add(level), AtomicPtr::new(*next));
            }
            let bytes = memory.add(HEADER + height * LINK);
            ptr::copy_nonoverlapping(key.as_ptr(), bytes, key.len());
            ptr::copy_nonoverlapping(value.as_ptr(), bytes.add(key.len()), value.len());
        }
        // Publish, level 0 first (R2). A reader that finds the node on a level finds it on every
        // level below too, or reaches it there through its predecessor.
        for link in at.iter().take(height) {
            link.store(node, Ordering::Release);
        }
        if height > read.height.load(Ordering::Relaxed) {
            // Readers may load the old height for a while; see `Buffer::search`.
            read.height.store(height, Ordering::Relaxed);
        }

        let estimated = write.estimated.load(Ordering::Relaxed)
            + estimated_size(key.len(), value.len(), create_ts.is_some())
            - replaced.unwrap_or(0);
        write.estimated.store(estimated, Ordering::Relaxed);
        if replaced.is_none() {
            let len = write.len.load(Ordering::Relaxed) + 1;
            write.len.store(len, Ordering::Release);
        }
        if grown > 0 {
            let allocated = write.allocated.load(Ordering::Relaxed) + grown;
            write.allocated.store(allocated, Ordering::Relaxed);
        }
        Some(grown)
    }
}

impl Drop for Writer<'_> {
    fn drop(&mut self) {
        // Orders this writer's stores before the next writer's loads (R1).
        self.buffer.write.0.busy.store(false, Ordering::Release);
    }
}

impl Arena {
    /// A node height: 1, then each further level with probability 1/4.
    fn height(&mut self) -> usize {
        // xorshift64: deterministic, so heights repeat from run to run.
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        (1 + x.trailing_zeros() as usize / 2).min(MAX_HEIGHT)
    }

    /// The bytes of the chunk that [`Arena::alloc`] allocates for a node of `size` bytes, or 0 if
    /// the node fits in the current chunk.
    fn growth(&self, size: usize) -> u64 {
        if size <= self.room {
            0
        } else if size > LARGE_NODE {
            (CHUNK_HEADER + size) as u64
        } else {
            self.next_chunk.max(CHUNK_HEADER + size) as u64
        }
    }

    /// Returns `size` bytes for a node, aligned for it, and the bytes of the chunk that it
    /// allocated for them, or 0. Those are [`Arena::growth`] of `size`.
    fn alloc(&mut self, size: usize) -> (*mut u8, u64) {
        debug_assert!(size.is_multiple_of(ALIGN));
        if size <= self.room {
            let memory = self.next;
            // SAFETY: `size <= room`, so the result stays inside the current chunk.
            self.next = unsafe { memory.add(size) };
            self.room -= size;
            return (memory, 0);
        }
        if size > LARGE_NODE {
            // A chunk of its own; the current chunk keeps its room for later nodes.
            let len = CHUNK_HEADER + size;
            return (self.chunk(len), len as u64);
        }
        let len = self.next_chunk.max(CHUNK_HEADER + size);
        self.next_chunk = (self.next_chunk * 2).min(MAX_CHUNK);
        let memory = self.chunk(len);
        // SAFETY: the chunk holds `CHUNK_HEADER + size <= len` bytes after its header.
        self.next = unsafe { memory.add(size) };
        self.room = len - CHUNK_HEADER - size;
        (memory, len as u64)
    }

    /// Allocates a chunk of `len` bytes, links it into the list, and returns its first byte after
    /// the header.
    fn chunk(&mut self, len: usize) -> *mut u8 {
        let layout = Layout::from_size_align(len, ALIGN).expect("a range cache chunk layout");
        // SAFETY: `len` is at least `CHUNK_HEADER + HEADER`, so the layout is not empty.
        let chunk = unsafe { alloc::alloc(layout) }.cast::<ChunkHeader>();
        if chunk.is_null() {
            alloc::handle_alloc_error(layout);
        }
        // SAFETY: the allocation is `len` bytes, aligned for a chunk header, and its own.
        unsafe {
            ptr::write(
                chunk,
                ChunkHeader {
                    prev: self.chunks,
                    len,
                },
            );
            self.chunks = chunk;
            chunk.cast::<u8>().add(CHUNK_HEADER)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicU64;

    /// A small deterministic generator for the tests.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// Under Miri, which interprets every step, the tests use fewer operations.
    fn scale(normal: usize, miri: usize) -> usize {
        if cfg!(miri) {
            miri
        } else {
            normal
        }
    }

    /// An owned version, for expectations.
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Owned {
        key: Vec<u8>,
        seq: u64,
        kind: Kind,
        create_ts: Option<i64>,
        value: Vec<u8>,
    }

    impl From<Version<'_>> for Owned {
        fn from(v: Version<'_>) -> Self {
            Owned {
                key: v.key.to_vec(),
                seq: v.seq,
                kind: v.kind,
                create_ts: v.create_ts,
                value: v.value.to_vec(),
            }
        }
    }

    fn owned(key: &[u8], seq: u64, kind: Kind, create_ts: Option<i64>, value: &[u8]) -> Owned {
        Owned {
            key: key.to_vec(),
            seq,
            kind,
            create_ts,
            value: value.to_vec(),
        }
    }

    fn append(buffer: &Buffer, v: &Owned) -> u64 {
        buffer
            .writer()
            .append(&v.key, v.seq, v.kind, v.create_ts, &v.value)
    }

    /// Every version from `start`, in cursor order.
    fn scan(buffer: &Buffer, start: Bound<&[u8]>) -> Vec<Owned> {
        let mut cursor = buffer.seek(start);
        let mut out = Vec::new();
        while let Some(v) = buffer.version(&cursor) {
            out.push(Owned::from(v));
            buffer.advance(&mut cursor);
        }
        out
    }

    /// The reference order: key ascending, sequence number descending.
    type Reference = BTreeMap<(Vec<u8>, std::cmp::Reverse<u64>), Owned>;

    fn reference_insert(reference: &mut Reference, v: &Owned) {
        reference.insert((v.key.clone(), std::cmp::Reverse(v.seq)), v.clone());
    }

    fn reference_get(reference: &Reference, key: &[u8], max_seq: u64) -> Option<Owned> {
        reference
            .range((key.to_vec(), std::cmp::Reverse(max_seq))..)
            .next()
            .filter(|((k, _), _)| k.as_slice() == key)
            .map(|(_, v)| v.clone())
    }

    #[test]
    fn should_order_by_key_then_newest_first() {
        let buffer = Buffer::new();
        for (k, seq) in [("b", 2), ("a", 1), ("c", 3), ("b", 5), ("a", 4), ("b", 1)] {
            append(&buffer, &owned(k.as_bytes(), seq, Kind::Value, None, b"v"));
        }
        let order: Vec<_> = scan(&buffer, Bound::Unbounded)
            .into_iter()
            .map(|v| (String::from_utf8(v.key).unwrap(), v.seq))
            .collect();
        let want = [("a", 4), ("a", 1), ("b", 5), ("b", 2), ("b", 1), ("c", 3)];
        assert_eq!(
            order,
            want.iter()
                .map(|(k, s)| (k.to_string(), *s))
                .collect::<Vec<_>>()
        );
        assert_eq!(buffer.len(), 6);
    }

    #[test]
    fn should_read_each_version_of_a_key_at_its_sequence_number() {
        let buffer = Buffer::new();
        let versions = [
            owned(b"k", 10, Kind::Value, Some(100), b"ten"),
            owned(b"k", 20, Kind::Merge, Some(200), b"+twenty"),
            owned(b"k", 30, Kind::Tombstone, Some(300), b""),
            owned(b"k", 40, Kind::Value, None, b"forty"),
        ];
        for v in &versions {
            append(&buffer, v);
        }
        // Neighbours, so that the search has something to step over.
        append(&buffer, &owned(b"j", 25, Kind::Value, None, b"j"));
        append(&buffer, &owned(b"l", 25, Kind::Value, None, b"l"));
        let at = |seq| buffer.get(b"k", seq).map(Owned::from);
        assert_eq!(at(9), None);
        assert_eq!(at(10), Some(versions[0].clone()));
        assert_eq!(at(19), Some(versions[0].clone()));
        assert_eq!(at(20), Some(versions[1].clone()));
        assert_eq!(at(30), Some(versions[2].clone()));
        assert_eq!(at(39), Some(versions[2].clone()));
        assert_eq!(at(40), Some(versions[3].clone()));
        assert_eq!(at(u64::MAX), Some(versions[3].clone()));
        assert_eq!(buffer.get(b"kk", u64::MAX), None);
        assert_eq!(buffer.get(b"", u64::MAX), None);
        assert_eq!(buffer.get(b"m", u64::MAX), None);
        let tombstone = buffer.get(b"k", 30).unwrap();
        assert_eq!(tombstone.kind, Kind::Tombstone);
        assert!(tombstone.value.is_empty());
        assert_eq!(buffer.get(b"k", 20).unwrap().kind, Kind::Merge);
    }

    #[test]
    fn should_seek_from_a_start_key_and_run_past_the_end() {
        let buffer = Buffer::new();
        for i in (0..20u8).step_by(2) {
            append(&buffer, &owned(&[i], 1, Kind::Value, None, &[i]));
            append(&buffer, &owned(&[i], 2, Kind::Value, None, &[i, i]));
        }
        let keys = |start| {
            scan(&buffer, start)
                .into_iter()
                .map(|v| (v.key[0], v.seq))
                .collect::<Vec<_>>()
        };
        // At a present key: its newest version first.
        assert_eq!(
            keys(Bound::Included(&[4][..]))[..3],
            [(4, 2), (4, 1), (6, 2)]
        );
        // Between keys, and excluding a present key.
        assert_eq!(keys(Bound::Included(&[5][..]))[0], (6, 2));
        assert_eq!(keys(Bound::Excluded(&[4][..]))[0], (6, 2));
        assert_eq!(keys(Bound::Excluded(&[5][..]))[0], (6, 2));
        // Before the first key, and past the last.
        assert_eq!(keys(Bound::Included(&[][..])).len(), 20);
        assert!(keys(Bound::Included(&[19][..])).is_empty());
        assert!(keys(Bound::Excluded(&[18][..])).is_empty());
        // A cursor at the end stays there.
        let mut cursor = buffer.seek(Bound::Excluded(&[18][..]));
        buffer.advance(&mut cursor);
        assert_eq!(buffer.version(&cursor), None);
    }

    #[test]
    fn should_read_an_empty_buffer() {
        let buffer = Buffer::new();
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.allocated_bytes(), 0);
        assert_eq!(buffer.estimated_bytes(), 0);
        assert_eq!(buffer.get(b"a", u64::MAX), None);
        for start in [
            Bound::Unbounded,
            Bound::Included(&b"a"[..]),
            Bound::Excluded(&b""[..]),
        ] {
            let mut cursor = buffer.seek(start);
            assert_eq!(buffer.version(&cursor), None);
            buffer.advance(&mut cursor);
            assert_eq!(buffer.version(&cursor), None);
        }
        // A writer that appends nothing changes nothing.
        drop(buffer.writer());
        assert!(buffer.is_empty());
        assert_eq!(buffer.sealed_at().load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn should_hold_large_keys_and_values() {
        let buffer = Buffer::new();
        let big_value = vec![7u8; 3 * MAX_CHUNK];
        let big_key = vec![9u8; LARGE_NODE];
        let small = owned(b"a", 1, Kind::Value, None, b"x");
        assert!(append(&buffer, &small) > 0);
        let before = buffer.allocated_bytes();
        let large = owned(b"b", 2, Kind::Value, Some(5), &big_value);
        let grown = append(&buffer, &large);
        // A chunk of its own, exactly the node and the chunk header.
        assert_eq!(
            grown as usize,
            CHUNK_HEADER + node_size(header_height(&buffer, b"b"), 1, big_value.len())
        );
        assert_eq!(buffer.allocated_bytes(), before + grown);
        // The regular chunk keeps its room: the next small version allocates nothing.
        assert_eq!(append(&buffer, &owned(b"c", 3, Kind::Value, None, b"y")), 0);
        let large_key = owned(&big_key, 4, Kind::Merge, None, b"z");
        append(&buffer, &large_key);
        assert_eq!(buffer.get(b"b", 2).map(Owned::from), Some(large));
        assert_eq!(buffer.get(&big_key, 4).map(Owned::from), Some(large_key));
        assert_eq!(buffer.get(b"a", 1).map(Owned::from), Some(small));
        assert_eq!(scan(&buffer, Bound::Unbounded).len(), 4);
    }

    /// The height of the node of `key`, read back from the buffer.
    fn header_height(buffer: &Buffer, key: &[u8]) -> usize {
        let cursor = buffer.seek(Bound::Included(key));
        buffer.check(&cursor);
        // SAFETY: a node of `buffer`, found by its cursor.
        unsafe { header(cursor.node).height as usize }
    }

    #[test]
    fn should_hold_many_small_versions_across_chunks() {
        let buffer = Buffer::new();
        let mut reference = Reference::new();
        let mut rng = Rng(7);
        let n = scale(20_000, 600);
        let mut chunks = 0;
        for i in 0..n {
            let key = format!("key{:06}", rng.below(n as u64 / 2)).into_bytes();
            let v = owned(&key, i as u64 + 1, Kind::Value, None, &key);
            if append(&buffer, &v) > 0 {
                chunks += 1;
            }
            reference_insert(&mut reference, &v);
        }
        assert!(chunks > 3, "{chunks} chunks");
        let want: Vec<_> = reference.values().cloned().collect();
        assert_eq!(scan(&buffer, Bound::Unbounded), want);
        assert_eq!(buffer.len() as usize, want.len());
    }

    #[test]
    fn should_count_allocated_and_estimated_bytes_exactly() {
        let buffer = Buffer::new();
        let mut rng = Rng(11);
        let mut allocated = 0;
        let mut estimated = 0;
        let mut regular = Vec::new();
        for i in 0..scale(5_000, 400) {
            let value_len = if i % 97 == 0 {
                2 * LARGE_NODE
            } else {
                rng.below(300) as usize
            };
            let v = owned(
                format!("{:08}", rng.below(1_000_000)).as_bytes(),
                i as u64 + 1,
                Kind::Value,
                (i % 2 == 0).then_some(i as i64),
                &vec![1u8; value_len],
            );
            let grown = append(&buffer, &v);
            allocated += grown;
            estimated +=
                (v.key.len() + v.value.len() + 8 + if v.create_ts.is_some() { 8 } else { 0 })
                    as u64;
            if grown > 0 && value_len < LARGE_NODE {
                regular.push(grown as usize);
            }
            assert_eq!(buffer.allocated_bytes(), allocated);
            assert_eq!(buffer.estimated_bytes(), estimated);
        }
        // Regular chunks double from the first size up to the largest.
        assert_eq!(regular[0], FIRST_CHUNK);
        assert!(regular
            .windows(2)
            .all(|w| w[1] == (w[0] * 2).min(MAX_CHUNK)));
        // The chunk list holds exactly what the buffer counted.
        let mut listed = 0;
        // SAFETY: the test owns the buffer and no writer lives.
        let mut chunk = unsafe { (*buffer.write.0.arena.get()).chunks };
        while !chunk.is_null() {
            // SAFETY: a chunk of the buffer, which is alive.
            let h = unsafe { &*chunk };
            listed += h.len as u64;
            chunk = h.prev;
        }
        assert_eq!(listed, allocated);
    }

    /// A buffer that counts its chunks into a shared total until it drops, as the range cache's
    /// deltas do.
    struct Counted<'a> {
        buffer: Buffer,
        total: &'a AtomicU64,
    }

    impl Drop for Counted<'_> {
        fn drop(&mut self) {
            self.total
                .fetch_sub(self.buffer.allocated_bytes(), Ordering::Relaxed);
        }
    }

    #[test]
    fn should_release_its_memory_at_drop() {
        let total = AtomicU64::new(0);
        let buffers: Vec<_> = (0..4)
            .map(|_| {
                std::sync::Arc::new(Counted {
                    buffer: Buffer::new(),
                    total: &total,
                })
            })
            .collect();
        for (i, counted) in buffers.iter().enumerate() {
            for j in 0..scale(2_000, 100) {
                let key = (j as u32).to_be_bytes();
                let v = owned(&key, i as u64, Kind::Value, None, &[0; 40]);
                total.fetch_add(append(&counted.buffer, &v), Ordering::Relaxed);
            }
        }
        let sum: u64 = buffers.iter().map(|c| c.buffer.allocated_bytes()).sum();
        assert_eq!(total.load(Ordering::Relaxed), sum);
        // A reader holds the first buffer: its bytes stay counted, and it stays readable.
        let reader = buffers[0].clone();
        let held = reader.buffer.allocated_bytes();
        drop(buffers);
        assert_eq!(total.load(Ordering::Relaxed), held);
        assert!(reader.buffer.get(&0u32.to_be_bytes(), u64::MAX).is_some());
        drop(reader);
        assert_eq!(total.load(Ordering::Relaxed), 0);
        // Miri and the address sanitizer report any chunk that the drops did not free.
    }

    #[test]
    fn should_replace_a_version_with_the_same_key_and_sequence_number() {
        let buffer = Buffer::new();
        append(&buffer, &owned(b"a", 1, Kind::Value, None, b"a"));
        append(&buffer, &owned(b"k", 5, Kind::Value, None, b"first"));
        append(&buffer, &owned(b"k", 3, Kind::Value, None, b"older"));
        let estimated = buffer.estimated_bytes();
        let second = owned(b"k", 5, Kind::Merge, Some(9), b"second!");
        append(&buffer, &second);
        let third = owned(b"k", 5, Kind::Tombstone, None, b"");
        append(&buffer, &third);
        assert_eq!(buffer.get(b"k", 5).map(Owned::from), Some(third.clone()));
        let versions = scan(&buffer, Bound::Unbounded);
        assert_eq!(versions.len(), 3, "{versions:?}");
        assert_eq!(versions[1], third);
        assert_eq!(versions[2].value, b"older");
        // As the memtable counts a replacement.
        assert_eq!(
            buffer.estimated_bytes(),
            estimated - estimated_size(1, 5, false) + estimated_size(1, 0, false)
        );
        assert_eq!(buffer.len(), 3);
    }

    #[test]
    #[should_panic(expected = "second writer")]
    fn should_refuse_a_second_writer() {
        let buffer = Buffer::new();
        let _first = buffer.writer();
        let _second = buffer.writer();
    }

    #[test]
    #[should_panic(expected = "other than its own")]
    fn should_refuse_a_cursor_of_another_buffer() {
        let a = Buffer::new();
        let b = Buffer::new();
        append(&a, &owned(b"k", 1, Kind::Value, None, b"v"));
        let cursor = a.seek(Bound::Unbounded);
        let _ = b.version(&cursor);
    }

    /// Random appends against a reference map, read at every sequence number.
    #[test]
    fn should_match_a_reference_map() {
        for seed in 0..scale(8, 2) as u64 {
            let mut rng = Rng(seed);
            let buffer = Buffer::new();
            let mut reference = Reference::new();
            let batches = scale(300, 20);
            for seq in 1..=batches as u64 {
                let mut writer = buffer.writer();
                for _ in 0..1 + rng.below(6) {
                    let key = vec![b'k', rng.below(40) as u8];
                    let kind = match rng.below(4) {
                        0 => Kind::Tombstone,
                        1 => Kind::Merge,
                        _ => Kind::Value,
                    };
                    let value = if kind == Kind::Tombstone {
                        Vec::new()
                    } else {
                        format!("{seq}").into_bytes()
                    };
                    let create_ts = (rng.below(2) == 0).then_some(seq as i64);
                    let v = owned(&key, seq, kind, create_ts, &value);
                    writer.append(&v.key, v.seq, v.kind, v.create_ts, &v.value);
                    reference_insert(&mut reference, &v);
                }
            }
            for _ in 0..scale(2_000, 50) {
                let key = vec![b'k', rng.below(42) as u8];
                let max_seq = rng.below(batches as u64 + 2);
                assert_eq!(
                    buffer.get(&key, max_seq).map(Owned::from),
                    reference_get(&reference, &key, max_seq),
                    "seed {seed}"
                );
                let start = match rng.below(3) {
                    0 => Bound::Unbounded,
                    1 => Bound::Included(&key[..]),
                    _ => Bound::Excluded(&key[..]),
                };
                let want: Vec<_> = reference
                    .values()
                    .filter(|v| match start {
                        Bound::Unbounded => true,
                        Bound::Included(k) => v.key.as_slice() >= k,
                        Bound::Excluded(k) => v.key.as_slice() > k,
                    })
                    .cloned()
                    .collect();
                assert_eq!(scan(&buffer, start), want, "seed {seed}");
            }
        }
    }

    /// One writer appends batches and publishes each one's sequence number after it, as the
    /// batch writer commits after it applies. Readers read at a sequence number: three load the
    /// newest one before each read, as a read of the range cache does, and two keep a fixed one,
    /// as snapshots do, while the writer appends above it. Each must see exactly the versions at
    /// or below its number, whatever the writer does meanwhile.
    #[test]
    fn should_give_concurrent_readers_exactly_the_versions_at_their_sequence_number() {
        let batches = scale(2_000, 40) as u64;
        let keys = 64u64;
        // The script: batch `s` holds a few versions of distinct keys, at `s`.
        let mut rng = Rng(42);
        let script: Vec<Vec<Owned>> = (1..=batches)
            .map(|seq| {
                let mut batch: BTreeMap<Vec<u8>, Owned> = BTreeMap::new();
                for _ in 0..1 + rng.below(8) {
                    let key = format!("{:03}", rng.below(keys)).into_bytes();
                    let kind = match rng.below(5) {
                        0 => Kind::Tombstone,
                        1 => Kind::Merge,
                        _ => Kind::Value,
                    };
                    let value = if kind == Kind::Tombstone {
                        Vec::new()
                    } else {
                        vec![seq as u8; 1 + rng.below(200) as usize]
                    };
                    batch.insert(
                        key.clone(),
                        owned(&key, seq, kind, Some(seq as i64), &value),
                    );
                }
                batch.into_values().collect()
            })
            .collect();
        let mut reference = Reference::new();
        for v in script.iter().flatten() {
            reference_insert(&mut reference, v);
        }
        let buffer = Buffer::new();
        let committed = AtomicU64::new(0);
        let reads = AtomicU64::new(0);
        let readers = 5;
        // The writer starts when every reader runs, so that they overlap.
        let start = std::sync::Barrier::new(readers + 1);
        std::thread::scope(|s| {
            for r in 0..readers as u64 {
                let (buffer, committed, reference, reads, start) =
                    (&buffer, &committed, &reference, &reads, &start);
                let fixed = match r {
                    3 => Some(batches / 3),
                    4 => Some(2 * batches / 3),
                    _ => None,
                };
                s.spawn(move || {
                    let mut rng = Rng(100 + r);
                    start.wait();
                    loop {
                        // Load the sequence number first, as a read of the range cache does.
                        let now = committed.load(Ordering::Acquire);
                        let seq = match fixed {
                            Some(fixed) if now < fixed => {
                                std::thread::yield_now();
                                continue;
                            }
                            Some(fixed) => fixed,
                            None => now,
                        };
                        let key = format!("{:03}", rng.below(keys + 1)).into_bytes();
                        let found = buffer.get(&key, seq).map(Owned::from);
                        assert_eq!(found, reference_get(reference, &key, seq), "get at {seq}");
                        // A scan from the key, filtered to `seq`, against the reference.
                        let mut cursor = buffer.seek(Bound::Included(&key));
                        let mut got = Vec::new();
                        while got.len() < 20 {
                            let Some(v) = buffer.version(&cursor) else {
                                break;
                            };
                            if v.seq <= seq {
                                got.push(Owned::from(v));
                            }
                            buffer.advance(&mut cursor);
                        }
                        let want: Vec<_> = reference
                            .range((key.clone(), std::cmp::Reverse(u64::MAX))..)
                            .map(|(_, v)| v)
                            .filter(|v| v.seq <= seq)
                            .take(20)
                            .cloned()
                            .collect();
                        assert_eq!(got, want, "scan at {seq}");
                        reads.fetch_add(1, Ordering::Relaxed);
                        if now == batches {
                            break;
                        }
                    }
                });
            }
            start.wait();
            for (i, batch) in script.iter().enumerate() {
                let mut writer = buffer.writer();
                for v in batch {
                    writer.append(&v.key, v.seq, v.kind, v.create_ts, &v.value);
                    // Readers often meet a node of a batch that is not committed yet.
                    if i % 8 == 0 {
                        std::thread::yield_now();
                    }
                }
                drop(writer);
                committed.store(i as u64 + 1, Ordering::Release);
            }
        });
        assert!(reads.load(Ordering::Relaxed) > 0);
        assert_eq!(
            scan(&buffer, Bound::Unbounded),
            reference.values().cloned().collect::<Vec<_>>()
        );
    }
}
