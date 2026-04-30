use std::{
    collections::{hash_map::Entry, HashMap},
    future::Future,
    hash::Hash,
    sync::Arc,
};

use parking_lot::Mutex as SyncMutex;
use tokio::sync::OnceCell;

type InFlightMap<K, T> = Arc<SyncMutex<HashMap<K, Arc<OnceCell<T>>>>>;

// EntryGuard is an RAII handle that removes the map entry on drop once no other
// callers hold a reference to it. This keeps the map from growing unboundedly and
// ensures that after all callers for a key complete (or are cancelled), subsequent
// calls start fresh rather than observing stale state.
struct EntryGuard<'a, K: Hash + Eq, T> {
    cell: Option<Arc<OnceCell<T>>>,
    key: &'a K,
    in_flight: &'a InFlightMap<K, T>,
}

impl<'a, K, T> Drop for EntryGuard<'a, K, T>
where
    K: Hash + Eq,
{
    fn drop(&mut self) {
        let mut in_flight = self.in_flight.lock();
        // Drop our Arc *before* checking the count so we don't count ourselves.
        drop(self.cell.take());
        // If the only remaining Arc is the one inside the map itself, nobody else
        // is waiting on this cell anymore—safe to clean up.
        if in_flight
            .get(self.key)
            .is_some_and(|cell| Arc::strong_count(cell) == 1)
        {
            in_flight.remove(self.key);
        }
    }
}

/// SingleFlight deduplicates concurrent calls for the same key, ensuring only one
/// execution is in-flight at a time while sharing the result with all waiters.
#[derive(Debug, Clone)]
pub(crate) struct SingleFlight<K, T> {
    in_flight: InFlightMap<K, T>,
}

impl<K, T> Default for SingleFlight<K, T> {
    fn default() -> Self {
        Self {
            in_flight: Default::default(),
        }
    }
}

impl<K, T> SingleFlight<K, T>
where
    K: Hash + Eq + Clone,
{
    /// Create a new SingleFlight group.
    #[inline]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Execute and return the value for a given function, making sure that only one
    /// operation is in-flight at a given moment. If a duplicate call comes in, that caller will
    /// wait until the original call completes and return the same value.
    pub(crate) async fn call<F, Fut, E>(&self, key: K, func: F) -> Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        T: Clone,
    {
        // Acquire (or create) the shared cell for this key while briefly holding the lock.
        // The lock is released before any async work begins.
        let guard = {
            let mut in_flight = self.in_flight.lock();
            match in_flight.entry(key.clone()) {
                // Another caller is already in-flight for this key—share their cell.
                Entry::Occupied(occupied_entry) => EntryGuard {
                    cell: Some(occupied_entry.get().clone()),
                    key: &key,
                    in_flight: &self.in_flight,
                },
                // First caller for this key—insert a fresh OnceCell for others to find.
                Entry::Vacant(vacant_entry) => {
                    let e = Arc::new(OnceCell::new());
                    vacant_entry.insert(e.clone());
                    EntryGuard {
                        cell: Some(e),
                        key: &key,
                        in_flight: &self.in_flight,
                    }
                }
            }
        };

        // get_or_try_init ensures only one caller runs `func`; others await the result.
        // On error, the OnceCell remains uninitialized so the next waiter can retry
        // with its own func (important for transient failures).
        let value = guard
            .cell
            .as_ref()
            .expect("cell is always Some until Drop")
            .get_or_try_init(func)
            .await
            .cloned();

        value
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{
                AtomicUsize,
                Ordering::{AcqRel, Acquire},
            },
            Arc,
        },
        time::Duration,
    };

    use futures::{stream::FuturesUnordered, StreamExt};

    use super::*;

    #[tokio::test]
    async fn direct_call() {
        let group = SingleFlight::new();
        let result = group
            .call("key", || async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok::<_, ()>("Result".to_string())
            })
            .await;
        assert_eq!(result, Ok("Result".to_string()));
    }

    #[tokio::test]
    async fn parallel_call() {
        let call_counter = AtomicUsize::default();

        let group = SingleFlight::new();
        let futures = FuturesUnordered::new();
        for _ in 0..10 {
            futures.push(group.call("key", || async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                call_counter.fetch_add(1, AcqRel);
                Ok::<_, ()>("Result".to_string())
            }));
        }

        assert!(
            futures
                .all(|out| async move { out == Ok("Result".to_string()) })
                .await
        );
        assert_eq!(
            call_counter.load(Acquire),
            1,
            "future should only be executed once"
        );
    }

    #[tokio::test]
    async fn parallel_call_spawned() {
        let call_counter = Arc::new(AtomicUsize::default());

        let group = SingleFlight::<&'static str, String>::new();
        let mut handles = Vec::new();
        for _ in 0..10 {
            let g = group.clone();
            let counter = call_counter.clone();
            handles.push(tokio::spawn(async move {
                g.call("key", || async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    counter.fetch_add(1, AcqRel);
                    Ok::<_, ()>("Result".to_string())
                })
                .await
            }));
        }

        for handle in handles {
            assert_eq!(handle.await.unwrap(), Ok("Result".to_string()));
        }
        assert_eq!(
            call_counter.load(Acquire),
            1,
            "future should only be executed once"
        );
    }

    #[tokio::test]
    async fn different_keys_are_independent() {
        let counter_a = Arc::new(AtomicUsize::default());
        let counter_b = Arc::new(AtomicUsize::default());

        let group = SingleFlight::<&'static str, String>::new();

        let g = group.clone();
        let ca = counter_a.clone();
        let handle_a = tokio::spawn(async move {
            g.call("key_a", || async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                ca.fetch_add(1, AcqRel);
                Ok::<_, ()>("A".to_string())
            })
            .await
        });

        let g = group.clone();
        let cb = counter_b.clone();
        let handle_b = tokio::spawn(async move {
            g.call("key_b", || async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                cb.fetch_add(1, AcqRel);
                Ok::<_, ()>("B".to_string())
            })
            .await
        });

        assert_eq!(handle_a.await.unwrap(), Ok("A".to_string()));
        assert_eq!(handle_b.await.unwrap(), Ok("B".to_string()));
        assert_eq!(counter_a.load(Acquire), 1);
        assert_eq!(counter_b.load(Acquire), 1);
    }

    #[tokio::test]
    async fn sequential_calls_run_fresh() {
        // After a call completes, a new call to the same key should execute its own func.
        let call_counter = AtomicUsize::default();

        let group = SingleFlight::new();

        let result = group
            .call("key", || async {
                call_counter.fetch_add(1, AcqRel);
                Ok::<_, ()>("first".to_string())
            })
            .await;
        assert_eq!(result, Ok("first".to_string()));

        let result = group
            .call("key", || async {
                call_counter.fetch_add(1, AcqRel);
                Ok::<_, ()>("second".to_string())
            })
            .await;
        assert_eq!(result, Ok("second".to_string()));

        assert_eq!(
            call_counter.load(Acquire),
            2,
            "each sequential call should execute independently"
        );
    }

    #[tokio::test]
    async fn error_propagates_and_next_caller_retries() {
        let group = SingleFlight::<&str, String>::new();

        // First call fails.
        let result: Result<String, &str> = group.call("key", || async { Err("oops") }).await;
        assert_eq!(result, Err("oops"));

        // Next call should run its own func (not get the cached error).
        let result = group
            .call("key", || async { Ok::<_, &str>("recovered".to_string()) })
            .await;
        assert_eq!(result, Ok("recovered".to_string()));
    }

    #[tokio::test]
    async fn error_with_concurrent_waiters() {
        // When the initializer fails, a concurrent waiter gets to retry with its own func.
        let group = SingleFlight::<&str, String>::new();
        let call_counter = AtomicUsize::default();

        let fut_1 = group.call("key", || async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            call_counter.fetch_add(1, AcqRel);
            Err::<String, _>("fail")
        });
        let fut_2 = group.call("key", || async {
            call_counter.fetch_add(1, AcqRel);
            Ok::<_, &str>("recovered".to_string())
        });

        let (r1, r2) = tokio::join!(fut_1, fut_2);
        assert_eq!(r1, Err("fail"));
        assert_eq!(r2, Ok("recovered".to_string()));
        assert_eq!(
            call_counter.load(Acquire),
            2,
            "both funcs should have been called since the first failed"
        );
    }

    #[tokio::test]
    async fn call_with_custom_key() {
        #[derive(Clone, PartialEq, Eq, Hash)]
        struct K(i32);
        let group = SingleFlight::new();
        let result = group
            .call(K(1), || async {
                tokio::time::sleep(Duration::from_millis(1)).await;
                Ok::<_, ()>("Result".to_string())
            })
            .await;
        assert_eq!(result, Ok("Result".to_string()));
    }

    #[tokio::test]
    async fn late_joiner_shares_result() {
        let group = SingleFlight::<String, String>::new();

        // Spawn early so it registers in the map while it's running.
        let g = group.clone();
        let early_handle = tokio::spawn(async move {
            g.call("key".to_string(), || async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok::<_, ()>("Result".to_string())
            })
            .await
        });

        // Give early time to register and start sleeping.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Late caller should find the existing entry and share the result.
        let late_result = group
            .call("key".to_string(), || async { panic!("unexpected") })
            .await;
        assert_eq!(late_result, Ok::<_, ()>("Result".to_string()));
        assert_eq!(early_handle.await.unwrap(), Ok("Result".to_string()));
    }

    #[tokio::test]
    async fn cancel_allows_next_caller_to_proceed() {
        let group = SingleFlight::new();

        // Start a slow computation, then cancel it.
        let fut_cancel = group.call("key".to_string(), || async {
            tokio::time::sleep(Duration::from_millis(2000)).await;
            Ok::<_, ()>("cancelled".to_string())
        });
        let _ = tokio::time::timeout(Duration::from_millis(10), fut_cancel).await;

        // The next caller should run fresh since the previous was cancelled.
        let result = group
            .call("key".to_string(), || async {
                Ok::<_, ()>("fresh".to_string())
            })
            .await;
        assert_eq!(result, Ok("fresh".to_string()));
    }

    #[tokio::test]
    async fn cancel_with_concurrent_waiter() {
        // If the initializer is cancelled but another caller is also waiting,
        // the waiter should get to retry.
        let group = SingleFlight::<String, String>::new();

        let g = group.clone();
        let initializer = tokio::spawn(async move {
            g.call("key".to_string(), || async {
                tokio::time::sleep(Duration::from_millis(2000)).await;
                Ok::<_, ()>("slow".to_string())
            })
            .await
        });

        // Let the initializer register.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Start a concurrent waiter on the same key.
        let g = group.clone();
        let waiter = tokio::spawn(async move {
            g.call("key".to_string(), || async {
                // This func runs if the waiter gets to retry after cancellation.
                Ok::<_, ()>("retried".to_string())
            })
            .await
        });

        // Give the waiter time to register and start waiting.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Cancel the initializer.
        initializer.abort();
        let _ = initializer.await;

        // The waiter should retry and succeed.
        let result = waiter.await.unwrap();
        assert_eq!(result, Ok("retried".to_string()));
    }

    #[tokio::test]
    async fn concurrent_callers_share_slow_result() {
        // Two concurrent callers via join! — second should not run its func.
        let group = SingleFlight::new();

        let begin = tokio::time::Instant::now();
        let fut_1 = group.call("key".to_string(), || async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok::<_, ()>("Result1".to_string())
        });
        let fut_2 = group.call("key".to_string(), || async { panic!("should not execute") });
        let (v1, v2) = tokio::join!(fut_1, fut_2);
        assert_eq!(v1, Ok("Result1".to_string()));
        assert_eq!(v2, Ok::<_, ()>("Result1".to_string()));
        assert!(begin.elapsed() >= Duration::from_millis(200));
    }

    #[tokio::test]
    async fn map_is_cleaned_up_after_completion() {
        let group = SingleFlight::new();

        // Run a call to completion.
        let _ = group
            .call("key", || async { Ok::<_, ()>("done".to_string()) })
            .await;

        // The internal map should be empty.
        let in_flight = group.in_flight.lock();
        assert!(
            in_flight.is_empty(),
            "map should be empty after all callers complete"
        );
    }

    #[tokio::test]
    async fn map_is_cleaned_up_after_cancel() {
        let group = SingleFlight::new();

        let fut = group.call("key".to_string(), || async {
            tokio::time::sleep(Duration::from_millis(2000)).await;
            Ok::<_, ()>("never".to_string())
        });
        let _ = tokio::time::timeout(Duration::from_millis(10), fut).await;

        // The internal map should be empty after cancellation.
        let in_flight = group.in_flight.lock();
        assert!(
            in_flight.is_empty(),
            "map should be empty after cancellation"
        );
    }
}
