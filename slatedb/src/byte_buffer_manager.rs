use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use tokio::sync::Notify;

/// Tracks and enforces a global memory budget for in-flight write data.
///
/// Writers must acquire a [`WriteBufferPermit`] before submitting a batch.
/// The permit is attached to the active memtable and released when that
/// memtable is dropped after being flushed to L0, thereby freeing the
/// budget for new writes.
#[derive(Clone)]
pub struct ByteBufferManager {
    inner: Arc<ByteBudgetSemaphore>,
}

impl ByteBufferManager {
    /// Creates a new write-buffer manager with the given byte budget.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(ByteBudgetSemaphore::new(capacity)),
        }
    }

    /// Reserves `num_bytes` from the write-buffer budget, blocking until
    /// capacity is available. Returns a permit that releases the reservation
    /// on drop.
    pub async fn acquire(&self, num_bytes: usize) -> ByteBufferPermit {
        if num_bytes == 0 {
            return ByteBufferPermit {
                reserved_bytes: AtomicUsize::new(0),
                semaphore: Arc::clone(&self.inner),
            };
        }

        Arc::clone(&self.inner).acquire_permit(num_bytes).await
    }

    /// Unconditionally reserves `num_bytes` without waiting.
    /// The bytes are tracked by the budget (so `available()` reflects them)
    /// but the call never blocks, even if the budget is fully exhausted.
    ///
    /// Use this for paths like WAL replay where the data is already in
    /// memory and must be accounted for, but blocking would deadlock
    /// because forward progress is needed to free the budget.
    pub fn force_acquire(&self, num_bytes: usize) -> ByteBufferPermit {
        self.inner.force_acquire(num_bytes);
        ByteBufferPermit {
            reserved_bytes: AtomicUsize::new(num_bytes),
            semaphore: Arc::clone(&self.inner),
        }
    }

    /// Returns the number of unreserved bytes remaining in the budget.
    pub fn available(&self) -> usize {
        self.inner.available()
    }
}

/// An RAII guard representing a reserved portion of the write-buffer budget.
///
/// Dropping the permit returns its reserved bytes to the parent
/// [`WriteBufferManager`]. Multiple permits can be consolidated via
/// [`merge`](Self::merge) so that a single drop releases the combined
/// reservation.
#[derive(Debug)]
pub struct ByteBufferPermit {
    semaphore: Arc<ByteBudgetSemaphore>,
    reserved_bytes: AtomicUsize,
}

impl ByteBufferPermit {
    /// Returns the number of bytes currently reserved by this permit.
    pub fn size(&self) -> usize {
        self.reserved_bytes.load(Ordering::Relaxed)
    }

    /// Merges another permit into this one, consuming `other` without
    /// releasing its tracked bytes back to the buffer budget. The combined
    /// byte budget is released when `self` is dropped.
    ///
    /// # Panics
    ///
    /// Panics if `self` and `other` were acquired from different
    /// `WriteBufferManager` instances.
    pub fn merge(&self, other: &Self) {
        assert!(
            Arc::ptr_eq(&self.semaphore, &other.semaphore),
            "merging permits from different semaphore instances"
        );

        let mut other_bytes = other.reserved_bytes.load(Ordering::Relaxed);
        loop {
            match other.reserved_bytes.compare_exchange_weak(
                other_bytes,
                0,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    break;
                }
                Err(cur) => {
                    other_bytes = cur;
                }
            }
        }

        self.reserved_bytes
            .fetch_add(other_bytes, Ordering::Relaxed);
    }
}

impl Drop for ByteBufferPermit {
    fn drop(&mut self) {
        let reserved = self.reserved_bytes.load(Ordering::Relaxed);
        if reserved > 0 {
            self.semaphore.release(reserved);
        }
    }
}

#[derive(Debug)]
struct ByteBudgetSemaphore {
    notify: Notify,
    allocated_bytes: AtomicUsize,
    capacity: usize,
}

impl ByteBudgetSemaphore {
    fn new(capacity: usize) -> Self {
        Self {
            notify: Notify::new(),
            allocated_bytes: AtomicUsize::new(0),
            capacity,
        }
    }

    async fn acquire(&self, num_bytes: usize) {
        let mut current = self.allocated_bytes.load(Ordering::Relaxed);
        let notify_fut = self.notify.notified();
        tokio::pin!(notify_fut);
        notify_fut.as_mut().enable();

        loop {
            if current < self.capacity {
                match self.allocated_bytes.compare_exchange_weak(
                    current,
                    current + num_bytes,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        break;
                    }
                    Err(cur) => {
                        current = cur;
                    }
                }
            } else {
                notify_fut.as_mut().await;
                notify_fut.set(self.notify.notified());
                current = self.allocated_bytes.load(Ordering::Relaxed);
            }
        }
    }

    /// Unconditionally adds `num_bytes` to the allocated count without
    /// waiting. This can push `allocated_bytes` above `capacity`.
    fn force_acquire(&self, num_bytes: usize) {
        self.allocated_bytes.fetch_add(num_bytes, Ordering::Relaxed);
    }

    fn release(&self, num_bytes: usize) {
        let mut current = self.allocated_bytes.load(Ordering::Relaxed);
        loop {
            assert!(
                current >= num_bytes,
                "cannot release more bytes than were reserved"
            );
            match self.allocated_bytes.compare_exchange_weak(
                current,
                current - num_bytes,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    break;
                }
                Err(cur) => {
                    current = cur;
                }
            }
        }

        if (current - num_bytes) < self.capacity {
            self.notify.notify_waiters();
        }
    }

    fn available(&self) -> usize {
        let current = self.allocated_bytes.load(Ordering::Relaxed);
        if current < self.capacity {
            self.capacity - current
        } else {
            0
        }
    }

    async fn acquire_permit(self: Arc<Self>, num_bytes: usize) -> ByteBufferPermit {
        self.acquire(num_bytes).await;
        ByteBufferPermit {
            reserved_bytes: AtomicUsize::new(num_bytes),
            semaphore: self,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    // ---------------------------------------------------------------
    // ByteBufferManager tests
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_new_manager_has_full_budget() {
        let mgr = ByteBufferManager::new(1024);
        assert_eq!(mgr.available(), 1024);
    }

    #[tokio::test]
    async fn test_acquire_reduces_available() {
        let mgr = ByteBufferManager::new(1024);
        let _permit = mgr.acquire(100).await;
        assert_eq!(mgr.available(), 924);
    }

    #[tokio::test]
    async fn test_acquire_entire_budget() {
        let mgr = ByteBufferManager::new(256);
        let permit = mgr.acquire(256).await;
        assert_eq!(mgr.available(), 0);
        assert_eq!(permit.size(), 256);
    }

    #[tokio::test]
    async fn test_drop_permit_restores_budget() {
        let mgr = ByteBufferManager::new(1024);
        let permit = mgr.acquire(300).await;
        assert_eq!(mgr.available(), 724);
        drop(permit);
        assert_eq!(mgr.available(), 1024);
    }

    #[tokio::test]
    async fn test_multiple_acquires() {
        let mgr = ByteBufferManager::new(1024);
        let p1 = mgr.acquire(200).await;
        let p2 = mgr.acquire(300).await;
        assert_eq!(mgr.available(), 524);
        assert_eq!(p1.size(), 200);
        assert_eq!(p2.size(), 300);
    }

    #[tokio::test]
    async fn test_acquire_blocks_when_budget_exhausted() {
        let mgr = ByteBufferManager::new(100);
        let _permit = mgr.acquire(100).await;

        // A second acquire should block because the budget is exhausted.
        let result = timeout(Duration::from_millis(50), mgr.acquire(1)).await;
        assert!(result.is_err(), "acquire should have timed out");
    }

    #[tokio::test]
    async fn test_acquire_unblocks_after_drop() {
        let mgr = ByteBufferManager::new(100);
        let permit = mgr.acquire(100).await;

        let mgr_clone = mgr.clone();
        let handle = tokio::spawn(async move { mgr_clone.acquire(50).await });

        // Give the spawned task a moment to park on the semaphore.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Free the budget.
        drop(permit);

        let p2 = timeout(Duration::from_millis(100), handle)
            .await
            .expect("should have completed")
            .expect("task should not panic");
        assert_eq!(p2.size(), 50);
    }

    // ---------------------------------------------------------------
    // WriteBufferPermit::size
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_permit_size() {
        let mgr = ByteBufferManager::new(1024);
        let permit = mgr.acquire(42).await;
        assert_eq!(permit.size(), 42);
    }

    // ---------------------------------------------------------------
    // WriteBufferPermit::merge
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_merge_combines_sizes() {
        let mgr = ByteBufferManager::new(1024);
        let p1 = mgr.acquire(100).await;
        let p2 = mgr.acquire(200).await;

        p1.merge(&p2);
        assert_eq!(p1.size(), 300);
        assert_eq!(mgr.available(), 724);
    }

    #[tokio::test]
    async fn test_merge_drops_release_combined() {
        let mgr = ByteBufferManager::new(1024);
        let p1 = mgr.acquire(100).await;
        let p2 = mgr.acquire(200).await;

        p1.merge(&p2);
        drop(p2);
        drop(p1);
        assert_eq!(mgr.available(), 1024);
    }

    #[tokio::test]
    async fn test_merge_other_drops_without_releasing() {
        let mgr = ByteBufferManager::new(1024);
        let p1 = mgr.acquire(100).await;
        let p2 = mgr.acquire(200).await;

        // After merge, dropping the consumed permit should not double-release.
        p1.merge(&p2);
        // p2's reserved_bytes are zeroed; dropping it won't release anything.
        drop(p2);
        assert_eq!(p1.size(), 300);
        assert_eq!(mgr.available(), 724);
    }

    #[tokio::test]
    #[should_panic(expected = "merging permits from different semaphore instances")]
    async fn test_merge_different_managers_panics() {
        let mgr1 = ByteBufferManager::new(1024);
        let mgr2 = ByteBufferManager::new(1024);
        let p1 = mgr1.acquire(10).await;
        let p2 = mgr2.acquire(10).await;

        p1.merge(&p2);
    }

    // ---------------------------------------------------------------
    // Drop
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_drop_zero_sized_permit_is_safe() {
        let mgr = ByteBufferManager::new(1024);
        let p1 = mgr.acquire(100).await;
        let p2 = mgr.acquire(100).await;

        // Merge p1 into p2, zeroing p1.
        p2.merge(&p1);
        assert_eq!(p1.size(), 0);

        // Dropping a zeroed permit should not affect the budget.
        drop(p1);
        assert_eq!(mgr.available(), 824);

        drop(p2);
        assert_eq!(mgr.available(), 1024);
    }

    #[tokio::test]
    async fn test_drop_after_merge_releases_all() {
        let mgr = ByteBufferManager::new(1024);
        let p1 = mgr.acquire(100).await;
        let p2 = mgr.acquire(200).await;
        let p3 = mgr.acquire(300).await;

        p1.merge(&p2);
        p1.merge(&p3);
        assert_eq!(p1.size(), 600);

        drop(p2);
        drop(p3);
        drop(p1);
        assert_eq!(mgr.available(), 1024);
    }
}
