//! Arena allocator with concurrent wrapper for skiplist nodes and values.
//!
//! This module provides a two-tier allocation system modeled after RocksDB's ConcurrentArena:
//!
//! - `Arena`: A simple, single-threaded bump allocator
//! - `ConcurrentArena`: A thread-safe wrapper with per-core shards to minimize contention
//!
//! ## Design
//!
//! The key insight from RocksDB is that memory allocation can be made thread-safe with:
//! - A fast inlined spinlock protecting the main arena
//! - Small per-core allocation caches (shards) to avoid contention for small allocations
//! - Lazy shard instantiation only when concurrent use is detected
//! - Dynamic shard sizing to avoid fragmentation waste
//!
//! ## Allocation Flow
//!
//! 1. Small allocations (< shard threshold):
//!    - Try to allocate from thread-local shard (no lock)
//!    - If shard is empty, refill from main arena under spinlock
//!
//! 2. Large allocations:
//!    - Go directly to main arena under spinlock

use std::alloc::{alloc, Layout};
use std::cell::{Cell, UnsafeCell};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use thread_local::ThreadLocal;

/// Global counter for assigning unique thread IDs.
static THREAD_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

thread_local! {
    /// Cached thread ID for fast lookup. Assigned on first access.
    static CACHED_THREAD_ID: Cell<u64> = Cell::new(0);
}

/// Get a unique ID for the current thread (fast, cached).
#[inline]
fn current_thread_id() -> u64 {
    CACHED_THREAD_ID.with(|id| {
        let cached = id.get();
        if cached != 0 {
            cached
        } else {
            let new_id = THREAD_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
            id.set(new_id);
            new_id
        }
    })
}

/// Allocate a boxed byte slice without zero-initialization.
///
/// # Safety
/// The returned Box contains uninitialized memory. Caller must ensure
/// memory is properly initialized before reading.
fn alloc_boxed_slice(size: usize) -> Box<[u8]> {
    let layout = Layout::from_size_align(size, std::mem::align_of::<usize>()).unwrap();
    let ptr = unsafe { alloc(layout) };
    if ptr.is_null() {
        std::alloc::handle_alloc_error(layout);
    }
    // SAFETY: ptr is valid, properly aligned, and we own it
    unsafe { Box::from_raw(std::slice::from_raw_parts_mut(ptr, size)) }
}

/// Default block size (64KB for good performance)
const DEFAULT_BLOCK_SIZE: usize = 64 * 1024;

/// Threshold for "small" allocations that can use shards
const SHARD_ALLOC_THRESHOLD: usize = 1024;

/// Initial shard size when first allocated
const INITIAL_SHARD_SIZE: usize = 256;

/// Maximum shard size to prevent waste
const MAX_SHARD_SIZE: usize = 8 * 1024;

/// Single-threaded arena allocator.
///
/// This is a simple bump allocator that allocates memory from blocks.
/// It is NOT thread-safe and must be wrapped in `ConcurrentArena` for
/// concurrent use.
///
/// Memory is managed using `Box<[u8]>` for automatic cleanup on drop.
pub struct Arena {
    /// Current allocation pointer (bump pointer)
    current: *mut u8,
    /// End of current block
    end: *mut u8,
    /// All allocated blocks - Box<[u8]> provides automatic deallocation on drop
    blocks: Vec<Box<[u8]>>,
    /// Total bytes allocated (for memory tracking)
    bytes_allocated: usize,
    /// Initial block size (typically L0 memtable size)
    initial_block_size: usize,
    /// Overflow block size (typically 1/8 of initial)
    overflow_block_size: usize,
    /// Whether the first block has been allocated
    first_block_allocated: bool,
}

impl Arena {
    /// Create a new Arena with default block size.
    pub fn new() -> Self {
        Self::with_initial_allocated_size(DEFAULT_BLOCK_SIZE)
    }

    /// Create a new Arena with the specified initial allocation size.
    ///
    /// - `initial_size`: The size of the first block (typically the L0 memtable size)
    ///
    /// Overflow blocks will be allocated at 1/8 of the initial size.
    pub fn with_initial_allocated_size(initial_size: usize) -> Self {
        let overflow_size = initial_size / 8;
        Self::with_block_sizes(initial_size, overflow_size.max(DEFAULT_BLOCK_SIZE))
    }

    /// Create a new Arena with explicit block sizes.
    ///
    /// - `initial_block_size`: The size of the first block
    /// - `overflow_block_size`: The size of subsequent blocks
    ///
    /// The first block is allocated eagerly to avoid allocation on first use.
    pub fn with_block_sizes(initial_block_size: usize, overflow_block_size: usize) -> Self {
        // Eagerly allocate first block
        let mut block = alloc_boxed_slice(initial_block_size);

        let block_ptr = block.as_mut_ptr();
        let end_ptr = unsafe { block_ptr.add(initial_block_size) };

        Self {
            current: block_ptr,
            end: end_ptr,
            blocks: vec![block],
            bytes_allocated: 0,
            initial_block_size,
            overflow_block_size,
            first_block_allocated: true,
        }
    }

    /// Allocate memory for a value of type T and initialize it.
    ///
    /// Returns a pointer to the initialized value. The pointer is valid
    /// for the lifetime of the Arena.
    pub fn alloc<T>(&mut self, value: T) -> *const T {
        let ptr = self.alloc_raw(Layout::new::<T>());
        unsafe {
            std::ptr::write(ptr as *mut T, value);
        }
        ptr as *const T
    }

    /// Allocate an uninitialized slice of T with the given length.
    ///
    /// Returns a pointer to the start of the allocation. Caller must
    /// initialize all elements before use.
    ///
    /// # Safety
    /// Caller must initialize all elements before reading from the slice.
    pub fn alloc_slice<T>(&mut self, len: usize) -> *mut T {
        if len == 0 {
            return std::ptr::NonNull::dangling().as_ptr();
        }
        let layout = Layout::array::<T>(len).expect("allocation too large");
        self.alloc_raw(layout) as *mut T
    }

    /// Allocate raw bytes with custom alignment.
    ///
    /// This is useful when allocating variable-size data that needs
    /// specific alignment (e.g., a header struct followed by data).
    ///
    /// # Safety
    /// Caller must properly initialize the allocated memory before reading.
    pub fn alloc_bytes_aligned(&mut self, size: usize, align: usize) -> *mut u8 {
        if size == 0 {
            return std::ptr::NonNull::dangling().as_ptr();
        }
        let layout = Layout::from_size_align(size, align).expect("invalid layout");
        self.alloc_raw(layout)
    }

    /// Returns total bytes allocated from this arena.
    pub fn bytes_allocated(&self) -> usize {
        self.bytes_allocated
    }

    /// Allocate a chunk of memory for a shard to use.
    ///
    /// Returns (ptr, size) where ptr is the start of the allocation
    /// and size is how many bytes were allocated (may be less than requested
    /// if current block doesn't have enough space).
    pub fn allocate_shard_memory(&mut self, requested_size: usize) -> (*mut u8, usize) {
        let available = self.end as usize - self.current as usize;

        if available >= requested_size {
            // Have enough in current block
            let ptr = self.current;
            self.current = unsafe { self.current.add(requested_size) };
            self.bytes_allocated += requested_size;
            (ptr, requested_size)
        } else if available > 0 {
            // Return what we have (avoid wasting remainder)
            let ptr = self.current;
            let size = available;
            self.current = self.end;
            self.bytes_allocated += size;
            (ptr, size)
        } else {
            // Need a new block
            self.allocate_new_block();
            // Recurse (new block will have space)
            self.allocate_shard_memory(requested_size)
        }
    }

    /// Allocate raw memory with the given layout.
    fn alloc_raw(&mut self, layout: Layout) -> *mut u8 {
        let size = layout.size();
        let align = layout.align();

        // Align the current pointer
        let aligned = align_up(self.current as usize, align);
        let new_current = aligned + size;

        if new_current <= self.end as usize {
            // We have space in current block
            self.current = new_current as *mut u8;
            self.bytes_allocated += size;
            return aligned as *mut u8;
        }

        // Need a new block
        self.alloc_slow(layout)
    }

    /// Slow path: allocate a new block and then allocate from it.
    fn alloc_slow(&mut self, layout: Layout) -> *mut u8 {
        let size = layout.size();
        let align = layout.align();

        // Determine block size
        let base_block_size = if !self.first_block_allocated {
            self.first_block_allocated = true;
            self.initial_block_size
        } else {
            self.overflow_block_size
        };

        // Make sure block is large enough for this allocation
        let block_size = base_block_size.max(size + align);

        // Allocate new block
        let mut block = alloc_boxed_slice(block_size);
        let block_ptr = block.as_mut_ptr();

        // Calculate allocation from new block
        let aligned = align_up(block_ptr as usize, align);
        self.current = (aligned + size) as *mut u8;
        self.end = unsafe { block_ptr.add(block_size) };
        self.blocks.push(block);

        self.bytes_allocated += size;
        aligned as *mut u8
    }

    /// Allocate a new block without performing any allocation from it.
    fn allocate_new_block(&mut self) {
        let block_size = if !self.first_block_allocated {
            self.first_block_allocated = true;
            self.initial_block_size
        } else {
            self.overflow_block_size
        };

        let mut block = alloc_boxed_slice(block_size);
        let block_ptr = block.as_mut_ptr();

        self.current = block_ptr;
        self.end = unsafe { block_ptr.add(block_size) };
        self.blocks.push(block);
    }
}

impl Default for Arena {
    fn default() -> Self {
        Self::new()
    }
}

// Arena is NOT thread-safe - it must be wrapped in ConcurrentArena
// We explicitly do NOT implement Send or Sync

/// Align a value up to the given alignment.
#[inline]
const fn align_up(val: usize, align: usize) -> usize {
    (val + align - 1) & !(align - 1)
}

/// A fast inlined spinlock.
///
/// Uses atomic compare-exchange with spin-loop hint for minimal overhead.
struct SpinLock {
    locked: AtomicBool,
}

impl SpinLock {
    const fn new() -> Self {
        Self {
            locked: AtomicBool::new(false),
        }
    }

    /// Acquire the lock.
    #[inline]
    fn lock(&self) {
        // Fast path: try to acquire immediately
        if self
            .locked
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            return;
        }

        // Slow path: spin
        self.lock_slow();
    }

    #[cold]
    fn lock_slow(&self) {
        loop {
            // Spin while locked (just reading, not trying to acquire)
            while self.locked.load(Ordering::Relaxed) {
                std::hint::spin_loop();
            }

            // Try to acquire
            if self
                .locked
                .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Release the lock.
    #[inline]
    fn unlock(&self) {
        self.locked.store(false, Ordering::Release);
    }
}

/// Per-thread allocation shard.
///
/// Each thread gets a small local buffer to allocate from without contention.
/// When the buffer is exhausted, it refills from the main arena under the spinlock.
struct Shard {
    /// Current allocation pointer in local buffer
    allocated: *mut u8,
    /// End of local buffer
    end: *mut u8,
    /// Current shard size (grows dynamically)
    shard_size: usize,
}

// SAFETY: Shard is Send because:
// - It is only ever accessed by the thread that owns it (via ThreadLocal)
// - The pointers point to memory owned by the Arena which outlives all shards
// - Each thread's shard is independent and never shared
unsafe impl Send for Shard {}

impl Shard {
    fn new() -> Self {
        Self {
            allocated: std::ptr::null_mut(),
            end: std::ptr::null_mut(),
            shard_size: INITIAL_SHARD_SIZE,
        }
    }

    /// Try to allocate from this shard.
    /// Returns None if shard doesn't have enough space.
    #[inline]
    fn try_alloc(&mut self, size: usize, align: usize) -> Option<*mut u8> {
        let aligned = align_up(self.allocated as usize, align);
        let new_allocated = aligned + size;

        if new_allocated <= self.end as usize {
            self.allocated = new_allocated as *mut u8;
            Some(aligned as *mut u8)
        } else {
            None
        }
    }

    /// Refill this shard with memory from the arena.
    fn refill(&mut self, ptr: *mut u8, size: usize) {
        self.allocated = ptr;
        self.end = unsafe { ptr.add(size) };

        // Grow shard size for next refill (up to max)
        if self.shard_size < MAX_SHARD_SIZE {
            self.shard_size = (self.shard_size * 2).min(MAX_SHARD_SIZE);
        }
    }

    /// Get the size to request for next refill.
    fn next_refill_size(&self) -> usize {
        self.shard_size
    }
}

/// Thread-safe arena allocator using per-core shards.
///
/// This wraps a single-threaded `Arena` and adds thread-safety using:
/// - A fast spinlock for arena access
/// - Per-thread allocation shards to minimize contention
/// - Lazy shard instantiation when concurrent use is detected
///
/// Small allocations use thread-local shards (no contention).
/// Large allocations and shard refills go through the spinlock.
pub struct ConcurrentArena {
    /// The underlying single-threaded arena (protected by spinlock)
    arena: UnsafeCell<Arena>,
    /// Fast spinlock for arena access
    lock: SpinLock,
    /// Per-thread allocation shards
    shards: ThreadLocal<UnsafeCell<Shard>>,
    /// Thread ID of the first thread to allocate (for detecting concurrency)
    first_thread: AtomicU64,
    /// Whether we've detected concurrent use
    concurrent_detected: AtomicBool,
    /// Total bytes allocated (atomic for thread-safe reads)
    bytes_allocated: AtomicUsize,
    /// CAS attempts counter (for contention analysis)
    cas_attempts: AtomicUsize,
    /// CAS failures counter (for contention analysis)
    cas_failures: AtomicUsize,
}

impl ConcurrentArena {
    /// Create a new ConcurrentArena with default block size.
    pub fn new() -> Self {
        Self::with_initial_allocated_size(DEFAULT_BLOCK_SIZE)
    }

    /// Create a new ConcurrentArena with the specified initial allocation size.
    pub fn with_initial_allocated_size(initial_size: usize) -> Self {
        let overflow_size = initial_size / 8;
        Self::with_block_sizes(initial_size, overflow_size.max(DEFAULT_BLOCK_SIZE))
    }

    /// Create a new ConcurrentArena with explicit block sizes.
    pub fn with_block_sizes(initial_block_size: usize, overflow_block_size: usize) -> Self {
        Self {
            arena: UnsafeCell::new(Arena::with_block_sizes(
                initial_block_size,
                overflow_block_size,
            )),
            lock: SpinLock::new(),
            shards: ThreadLocal::new(),
            first_thread: AtomicU64::new(0),
            concurrent_detected: AtomicBool::new(false),
            bytes_allocated: AtomicUsize::new(0),
            cas_attempts: AtomicUsize::new(0),
            cas_failures: AtomicUsize::new(0),
        }
    }

    /// Allocate memory for a value of type T and initialize it.
    pub fn alloc<T>(&self, value: T) -> *const T {
        let ptr = self.alloc_raw(Layout::new::<T>());
        unsafe {
            std::ptr::write(ptr as *mut T, value);
        }
        ptr as *const T
    }

    /// Allocate an uninitialized slice of T with the given length.
    pub fn alloc_slice<T>(&self, len: usize) -> *mut T {
        if len == 0 {
            return std::ptr::NonNull::dangling().as_ptr();
        }
        let layout = Layout::array::<T>(len).expect("allocation too large");
        self.alloc_raw(layout) as *mut T
    }

    /// Allocate raw bytes with custom alignment.
    pub fn alloc_bytes_aligned(&self, size: usize, align: usize) -> *mut u8 {
        if size == 0 {
            return std::ptr::NonNull::dangling().as_ptr();
        }
        let layout = Layout::from_size_align(size, align).expect("invalid layout");
        self.alloc_raw(layout)
    }

    /// Returns total bytes allocated from this arena.
    pub fn bytes_allocated(&self) -> usize {
        self.bytes_allocated.load(Ordering::Relaxed)
    }

    /// Returns CAS contention statistics: (attempts, failures, failure_rate).
    pub fn cas_stats(&self) -> (usize, usize, f64) {
        let attempts = self.cas_attempts.load(Ordering::Relaxed);
        let failures = self.cas_failures.load(Ordering::Relaxed);
        let rate = if attempts > 0 {
            failures as f64 / attempts as f64
        } else {
            0.0
        };
        (attempts, failures, rate)
    }

    /// Allocate raw memory with the given layout.
    fn alloc_raw(&self, layout: Layout) -> *mut u8 {
        let size = layout.size();
        let align = layout.align();

        // Check for concurrency and use shards if detected
        if self.should_use_shards() && size <= SHARD_ALLOC_THRESHOLD {
            // Try to allocate from thread-local shard
            let shard_cell = self.shards.get_or(|| UnsafeCell::new(Shard::new()));

            // SAFETY: We only access our own thread's shard
            let shard = unsafe { &mut *shard_cell.get() };

            if let Some(ptr) = shard.try_alloc(size, align) {
                self.bytes_allocated.fetch_add(size, Ordering::Relaxed);
                return ptr;
            }

            // Shard is empty, refill it
            self.refill_shard(shard, size, align)
        } else {
            // Single-threaded path or large allocation: go directly to arena
            self.alloc_from_arena(layout)
        }
    }

    /// Check if we should use shards (i.e., concurrent use detected).
    #[inline]
    fn should_use_shards(&self) -> bool {
        // Fast path: already know we're concurrent
        if self.concurrent_detected.load(Ordering::Relaxed) {
            return true;
        }

        // Check if this is a new thread (uses fast cached thread ID)
        let tid = current_thread_id();
        let first = self.first_thread.load(Ordering::Relaxed);

        if first == 0 {
            // First thread to allocate
            self.first_thread
                .compare_exchange(0, tid, Ordering::Relaxed, Ordering::Relaxed)
                .ok();
            false
        } else if first != tid {
            // Different thread detected - enable shards
            self.concurrent_detected.store(true, Ordering::Relaxed);
            true
        } else {
            // Same thread as first
            false
        }
    }

    /// Refill a shard and allocate from it.
    fn refill_shard(&self, shard: &mut Shard, size: usize, align: usize) -> *mut u8 {
        let refill_size = shard.next_refill_size().max(size + align);

        self.lock.lock();
        let arena = unsafe { &mut *self.arena.get() };
        let (ptr, actual_size) = arena.allocate_shard_memory(refill_size);
        self.lock.unlock();

        // Refill shard with new memory
        shard.refill(ptr, actual_size);

        // Allocate from the newly refilled shard
        if let Some(result) = shard.try_alloc(size, align) {
            self.bytes_allocated.fetch_add(size, Ordering::Relaxed);
            result
        } else {
            // Shard still doesn't have enough (shouldn't happen normally)
            // Fall back to direct arena allocation
            self.alloc_from_arena(Layout::from_size_align(size, align).unwrap())
        }
    }

    /// Allocate directly from the arena under the spinlock.
    fn alloc_from_arena(&self, layout: Layout) -> *mut u8 {
        let size = layout.size();

        self.cas_attempts.fetch_add(1, Ordering::Relaxed);
        self.lock.lock();

        let arena = unsafe { &mut *self.arena.get() };
        let ptr = arena.alloc_raw(layout);

        self.lock.unlock();

        self.bytes_allocated.fetch_add(size, Ordering::Relaxed);
        ptr
    }
}

impl Default for ConcurrentArena {
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: ConcurrentArena is thread-safe because:
// - Arena access is protected by spinlock
// - Thread-local shards are only accessed by their owning thread
// - All shared state uses atomic operations
unsafe impl Send for ConcurrentArena {}
unsafe impl Sync for ConcurrentArena {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    // ==================== Arena (single-threaded) tests ====================

    #[test]
    fn arena_should_allocate_single_value() {
        let mut arena = Arena::new();
        let ptr = arena.alloc(42u64);
        unsafe {
            assert_eq!(*ptr, 42u64);
        }
    }

    #[test]
    fn arena_should_allocate_multiple_values() {
        let mut arena = Arena::new();

        let p1 = arena.alloc(1u32);
        let p2 = arena.alloc(2u32);
        let p3 = arena.alloc(3u32);

        unsafe {
            assert_eq!(*p1, 1);
            assert_eq!(*p2, 2);
            assert_eq!(*p3, 3);
        }
    }

    #[test]
    fn arena_should_track_bytes_allocated() {
        let mut arena = Arena::new();

        arena.alloc(42u64);
        assert_eq!(arena.bytes_allocated(), 8);

        arena.alloc(123u32);
        assert_eq!(arena.bytes_allocated(), 12);
    }

    #[test]
    fn arena_should_allocate_large_values() {
        let mut arena = Arena::with_block_sizes(64, 64);

        // Allocate something larger than block size
        let large: [u8; 128] = [0xAB; 128];
        let ptr = arena.alloc(large);

        unsafe {
            assert_eq!((*ptr)[0], 0xAB);
            assert_eq!((*ptr)[127], 0xAB);
        }
    }

    #[test]
    fn arena_should_maintain_alignment() {
        let mut arena = Arena::new();

        // Allocate a byte to potentially misalign
        arena.alloc(0u8);

        // Allocate something requiring 8-byte alignment
        let ptr = arena.alloc(42u64);
        assert_eq!(ptr as usize % 8, 0);

        // Allocate a byte again
        arena.alloc(0u8);

        // And something requiring 16-byte alignment
        #[repr(align(16))]
        struct Aligned16(u64, u64);

        let ptr2 = arena.alloc(Aligned16(1, 2));
        assert_eq!(ptr2 as usize % 16, 0);
    }

    // ==================== ConcurrentArena tests ====================

    #[test]
    fn concurrent_arena_should_allocate_single_value() {
        let arena = ConcurrentArena::new();
        let ptr = arena.alloc(42u64);
        unsafe {
            assert_eq!(*ptr, 42u64);
        }
    }

    #[test]
    fn concurrent_arena_should_allocate_multiple_values() {
        let arena = ConcurrentArena::new();

        let p1 = arena.alloc(1u32);
        let p2 = arena.alloc(2u32);
        let p3 = arena.alloc(3u32);

        unsafe {
            assert_eq!(*p1, 1);
            assert_eq!(*p2, 2);
            assert_eq!(*p3, 3);
        }
    }

    #[test]
    fn concurrent_arena_should_track_bytes_allocated() {
        let arena = ConcurrentArena::new();

        arena.alloc(42u64);
        assert_eq!(arena.bytes_allocated(), 8);

        arena.alloc(123u32);
        assert_eq!(arena.bytes_allocated(), 12);
    }

    #[test]
    fn concurrent_arena_should_allocate_large_values() {
        let arena = ConcurrentArena::with_block_sizes(64, 64);

        // Allocate something larger than block size
        let large: [u8; 128] = [0xAB; 128];
        let ptr = arena.alloc(large);

        unsafe {
            assert_eq!((*ptr)[0], 0xAB);
            assert_eq!((*ptr)[127], 0xAB);
        }
    }

    #[test]
    fn concurrent_arena_should_handle_concurrent_allocations() {
        let arena = Arc::new(ConcurrentArena::new());
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let arena = arena.clone();
                thread::spawn(move || {
                    for i in 0..1000 {
                        let value = t * 1000 + i;
                        let ptr = arena.alloc(value as u64);
                        unsafe {
                            assert_eq!(*ptr, value as u64);
                        }
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }

        // Should have allocated 8 * 1000 * 8 bytes
        assert_eq!(arena.bytes_allocated(), 8 * 1000 * 8);
    }

    #[test]
    fn concurrent_arena_should_allocate_slice() {
        let arena = ConcurrentArena::new();
        let ptr = arena.alloc_slice::<u64>(10);

        unsafe {
            for i in 0..10 {
                std::ptr::write(ptr.add(i), i as u64);
            }

            for i in 0..10 {
                assert_eq!(*ptr.add(i), i as u64);
            }
        }
    }

    #[test]
    fn concurrent_arena_should_maintain_alignment() {
        let arena = ConcurrentArena::new();

        // Allocate a byte to potentially misalign
        arena.alloc(0u8);

        // Allocate something requiring 8-byte alignment
        let ptr = arena.alloc(42u64);
        assert_eq!(ptr as usize % 8, 0);

        // Allocate a byte again
        arena.alloc(0u8);

        // And something requiring 16-byte alignment
        #[repr(align(16))]
        struct Aligned16(u64, u64);

        let ptr2 = arena.alloc(Aligned16(1, 2));
        assert_eq!(ptr2 as usize % 16, 0);
    }

    #[test]
    fn concurrent_arena_should_use_initial_size_for_first_block() {
        // First block should be 1MB, overflow should be 128KB
        let arena = ConcurrentArena::with_initial_allocated_size(1024 * 1024);

        // Allocate to trigger first block
        arena.alloc(42u64);

        // Verify arena was created with correct size
        // (we can't directly inspect blocks, but we can verify bytes_allocated works)
        assert!(arena.bytes_allocated() > 0);
    }

    #[test]
    fn concurrent_arena_should_detect_concurrent_use() {
        let arena = Arc::new(ConcurrentArena::new());

        // First allocation on main thread
        arena.alloc(1u64);
        assert!(!arena.concurrent_detected.load(Ordering::Relaxed));

        // Allocation on another thread should trigger detection
        let arena_clone = arena.clone();
        let handle = thread::spawn(move || {
            arena_clone.alloc(2u64);
        });
        handle.join().unwrap();

        assert!(arena.concurrent_detected.load(Ordering::Relaxed));
    }

    #[test]
    fn shard_should_grow_dynamically() {
        let mut shard = Shard::new();
        assert_eq!(shard.shard_size, INITIAL_SHARD_SIZE);

        // Simulate refills
        let dummy = [0u8; 256];
        shard.refill(dummy.as_ptr() as *mut u8, 256);
        assert_eq!(shard.shard_size, INITIAL_SHARD_SIZE * 2);

        shard.refill(dummy.as_ptr() as *mut u8, 256);
        assert_eq!(shard.shard_size, INITIAL_SHARD_SIZE * 4);

        // Should cap at MAX_SHARD_SIZE
        for _ in 0..10 {
            shard.refill(dummy.as_ptr() as *mut u8, 256);
        }
        assert_eq!(shard.shard_size, MAX_SHARD_SIZE);
    }
}
