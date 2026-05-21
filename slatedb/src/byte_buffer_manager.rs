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
    high_watermark: usize,
}

impl ByteBufferManager {
    /// Creates a new write-buffer manager with the given byte budget.
    pub fn new(capacity: usize, high_watermark: usize) -> Self {
        Self {
            inner: Arc::new(ByteBudgetSemaphore::new(capacity)),
            high_watermark,
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

    /// Attempts to reserve `num_bytes` without blocking. Returns `Some(permit)`
    /// if capacity was available, or `None` if the budget is exhausted.
    pub fn try_acquire(&self, num_bytes: usize) -> Option<ByteBufferPermit> {
        if num_bytes == 0 {
            return Some(ByteBufferPermit {
                reserved_bytes: AtomicUsize::new(0),
                semaphore: Arc::clone(&self.inner),
            });
        }

        if self.inner.try_acquire(num_bytes) {
            Some(ByteBufferPermit {
                reserved_bytes: AtomicUsize::new(num_bytes),
                semaphore: Arc::clone(&self.inner),
            })
        } else {
            None
        }
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

    /// Returns the total byte budget capacity.
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    pub fn at_capacity(&self) -> bool {
        self.inner.allocated() >= self.high_watermark
    }

    /// Waits until `allocated_bytes` drops below the high watermark.
    ///
    /// This does **not** reserve any bytes — it only waits for the condition
    /// to be met and then returns. Because no reservation is made, the
    /// caller must be prepared for `allocated_bytes` to climb back above
    /// the high watermark immediately after this future resolves (TOCTOU).
    ///
    /// Use this for backpressure signaling where you want to wait until
    /// memory pressure has eased without holding budget during the wait.
    pub async fn await_capacity(&self) {
        self.inner
            .wait_for_allocated_below(self.high_watermark)
            .await;
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

    /// Attempts to reserve `num_bytes` without blocking. Returns `true` if
    /// the reservation succeeded, `false` if the budget is exhausted.
    fn try_acquire(&self, num_bytes: usize) -> bool {
        let mut current = self.allocated_bytes.load(Ordering::Relaxed);
        loop {
            if current >= self.capacity {
                return false;
            }
            match self.allocated_bytes.compare_exchange_weak(
                current,
                current + num_bytes,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(cur) => current = cur,
            }
        }
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

    fn allocated(&self) -> usize {
        self.allocated_bytes.load(Ordering::Relaxed)
    }

    async fn wait_for_allocated_below(&self, num_bytes: usize) {
        let mut current = self.allocated_bytes.load(Ordering::Relaxed);
        if current < num_bytes {
            return;
        }

        let notify_fut = self.notify.notified();
        tokio::pin!(notify_fut);
        notify_fut.as_mut().enable();

        loop {
            if current < num_bytes {
                break;
            } else {
                notify_fut.as_mut().await;
                notify_fut.set(self.notify.notified());
                current = self.allocated_bytes.load(Ordering::Relaxed);
            }
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
        let mgr = ByteBufferManager::new(1024, 0);
        assert_eq!(mgr.available(), 1024);
    }

    #[tokio::test]
    async fn test_acquire_reduces_available() {
        let mgr = ByteBufferManager::new(1024, 0);
        let _permit = mgr.acquire(100).await;
        assert_eq!(mgr.available(), 924);
    }

    #[tokio::test]
    async fn test_acquire_entire_budget() {
        let mgr = ByteBufferManager::new(256, 0);
        let permit = mgr.acquire(256).await;
        assert_eq!(mgr.available(), 0);
        assert_eq!(permit.size(), 256);
    }

    #[tokio::test]
    async fn test_drop_permit_restores_budget() {
        let mgr = ByteBufferManager::new(1024, 0);
        let permit = mgr.acquire(300).await;
        assert_eq!(mgr.available(), 724);
        drop(permit);
        assert_eq!(mgr.available(), 1024);
    }

    #[tokio::test]
    async fn test_multiple_acquires() {
        let mgr = ByteBufferManager::new(1024, 0);
        let p1 = mgr.acquire(200).await;
        let p2 = mgr.acquire(300).await;
        assert_eq!(mgr.available(), 524);
        assert_eq!(p1.size(), 200);
        assert_eq!(p2.size(), 300);
    }

    #[tokio::test]
    async fn test_acquire_blocks_when_budget_exhausted() {
        let mgr = ByteBufferManager::new(100, 0);
        let _permit = mgr.acquire(100).await;

        // A second acquire should block because the budget is exhausted.
        let result = timeout(Duration::from_millis(50), mgr.acquire(1)).await;
        assert!(result.is_err(), "acquire should have timed out");
    }

    #[tokio::test]
    async fn test_acquire_unblocks_after_drop() {
        let mgr = ByteBufferManager::new(100, 0);
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
        let mgr = ByteBufferManager::new(1024, 0);
        let permit = mgr.acquire(42).await;
        assert_eq!(permit.size(), 42);
    }

    // ---------------------------------------------------------------
    // WriteBufferPermit::merge
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_merge_combines_sizes() {
        let mgr = ByteBufferManager::new(1024, 0);
        let p1 = mgr.acquire(100).await;
        let p2 = mgr.acquire(200).await;

        p1.merge(&p2);
        assert_eq!(p1.size(), 300);
        assert_eq!(mgr.available(), 724);
    }

    #[tokio::test]
    async fn test_merge_drops_release_combined() {
        let mgr = ByteBufferManager::new(1024, 0);
        let p1 = mgr.acquire(100).await;
        let p2 = mgr.acquire(200).await;

        p1.merge(&p2);
        drop(p2);
        drop(p1);
        assert_eq!(mgr.available(), 1024);
    }

    #[tokio::test]
    async fn test_merge_other_drops_without_releasing() {
        let mgr = ByteBufferManager::new(1024, 0);
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
        let mgr1 = ByteBufferManager::new(1024, 0);
        let mgr2 = ByteBufferManager::new(1024, 0);
        let p1 = mgr1.acquire(10).await;
        let p2 = mgr2.acquire(10).await;

        p1.merge(&p2);
    }

    // ---------------------------------------------------------------
    // Drop
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_drop_zero_sized_permit_is_safe() {
        let mgr = ByteBufferManager::new(1024, 0);
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
        let mgr = ByteBufferManager::new(1024, 0);
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

    // ---------------------------------------------------------------
    // await_capacity tests
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_await_capacity_returns_immediately_when_below() {
        let mgr = ByteBufferManager::new(1024, 200);
        let _permit = mgr.acquire(100).await;

        // allocated=100, high_watermark=200 => should return immediately
        let result = timeout(Duration::from_millis(50), mgr.await_capacity()).await;
        assert!(result.is_ok(), "should not have timed out");
    }

    #[tokio::test]
    async fn test_await_capacity_returns_immediately_when_zero() {
        let mgr = ByteBufferManager::new(1024, 1);

        // allocated=0, high_watermark=1 => should return immediately
        let result = timeout(Duration::from_millis(50), mgr.await_capacity()).await;
        assert!(result.is_ok(), "should not have timed out");
    }

    #[tokio::test]
    async fn test_await_capacity_blocks_when_at_threshold() {
        let mgr = ByteBufferManager::new(1024, 500);
        let _permit = mgr.acquire(500).await;

        // allocated=500, high_watermark=500 => should block (not strictly below)
        let result = timeout(Duration::from_millis(50), mgr.await_capacity()).await;
        assert!(result.is_err(), "should have timed out");
    }

    #[tokio::test]
    async fn test_await_capacity_blocks_when_above() {
        let mgr = ByteBufferManager::new(1024, 500);
        let _permit = mgr.acquire(600).await;

        // allocated=600, high_watermark=500 => should block
        let result = timeout(Duration::from_millis(50), mgr.await_capacity()).await;
        assert!(result.is_err(), "should have timed out");
    }

    #[tokio::test]
    async fn test_await_capacity_unblocks_after_release() {
        let mgr = ByteBufferManager::new(1024, 500);
        let permit = mgr.acquire(600).await;

        let mgr_clone = mgr.clone();
        let handle = tokio::spawn(async move {
            mgr_clone.await_capacity().await;
        });

        // Give the spawned task a moment to park.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Release enough to drop below the high watermark.
        drop(permit);

        let result = timeout(Duration::from_millis(100), handle).await;
        assert!(result.is_ok(), "await_capacity should have completed");
    }

    #[tokio::test]
    async fn test_await_capacity_works_with_acquire() {
        let mgr = ByteBufferManager::new(200, 100);
        // Fill the budget completely.
        let permit = mgr.acquire(200).await;

        let mgr_clone = mgr.clone();
        let wait_handle = tokio::spawn(async move {
            // Wait until allocated drops below high_watermark (100).
            mgr_clone.await_capacity().await;
        });

        let mgr_clone2 = mgr.clone();
        let acquire_handle = tokio::spawn(async move {
            // acquire also waits for capacity.
            mgr_clone2.acquire(50).await
        });

        // Both should be blocked.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Release the full permit — allocated goes to 0.
        drop(permit);

        // Both should complete.
        let wait_result = timeout(Duration::from_millis(100), wait_handle).await;
        assert!(wait_result.is_ok(), "await_capacity should resolve");

        let acquire_result = timeout(Duration::from_millis(100), acquire_handle).await;
        let acquired_permit = acquire_result
            .expect("acquire should resolve")
            .expect("task should not panic");
        assert_eq!(acquired_permit.size(), 50);
    }

    #[tokio::test]
    async fn test_await_capacity_does_not_reserve_bytes() {
        let mgr = ByteBufferManager::new(1024, 200);
        let _permit = mgr.acquire(100).await;

        mgr.await_capacity().await;

        // After wait returns, available should be unchanged (no reservation made).
        assert_eq!(mgr.available(), 924);
    }
}
