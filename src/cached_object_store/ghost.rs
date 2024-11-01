use lru::LruCache;

use std::fmt::Debug;
use std::hash::Hash;
use std::num::NonZeroUsize;

/// The weighter is used to calculate the weight of the cache entry.
pub trait Weighter<K, V>: Fn(&K, &V) -> usize + Send + Sync + 'static {}
impl<K, V, T> Weighter<K, V> for T where for<'a> T: Fn(&K, &V) -> usize + Send + Sync + 'static {}

/// A simple wrapper of GhostQueue based on LRU eviction
pub struct GhostQueue<K: Hash + Eq, V> {
    queue: LruCache<K, V>,
    capacity: NonZeroUsize,
    weighter: Box<dyn Weighter<K, V>>,
    usage: usize,
}

impl<K: Hash + Eq, V> GhostQueue<K, V> {
    pub fn new(capacity: NonZeroUsize, weighter: impl Weighter<K, V>) -> Self {
        Self {
            queue: LruCache::unbounded(),
            capacity,
            weighter: Box::new(weighter),
            usage: 0,
        }
    }

    pub fn record(&mut self, key: K, value: V) {
        // try to evict entries in the LRU to create space for the element to be inserted
        let weight = (self.weighter)(&key, &value);
        while self.usage + weight > self.capacity.get() {
            if let Some((k, v)) = self.queue.pop_lru() {
                self.usage -= (self.weighter)(&k, &v);
            }
        }
        self.queue.put(key, value);
        self.usage += weight;
    }

    pub fn remove(&mut self, key: &K) -> Option<(K, V)> {
        self.queue.pop_entry(key).map(|(k, v)| {
            self.usage -= (self.weighter)(&k, &v);
            (k, v)
        })
    }

    #[allow(dead_code)]
    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.queue.get(key)
    }

    #[inline]
    #[allow(dead_code)]
    pub fn usage(&self) -> usize {
        self.usage
    }

    #[inline]
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    #[inline]
    pub fn need_cache(&self, key: &K) -> bool {
        self.queue.contains(key)
    }

    #[inline]
    pub fn capacity(&self) -> NonZeroUsize {
        self.capacity
    }

    #[inline]
    pub fn adjust_capacity(&mut self, capacity: NonZeroUsize) {
        self.capacity = capacity;
    }
}

impl<K: Hash + Eq + Debug, V: Debug> Debug for GhostQueue<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GhostQueue")
            .field("capacity", &self.capacity)
            .field("usage", &self.usage)
            .field("entries num", &self.queue.len())
            .finish()
    }
}

pub struct GhostBuilder<K: Hash + Eq, V> {
    capacity: NonZeroUsize,
    weighter: Box<dyn Weighter<K, V>>,
}

impl<K: Hash + Eq + 'static, V: 'static> GhostBuilder<K, V> {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            capacity,
            weighter: Box::new(|_, _| 1),
        }
    }

    pub fn with_weighter(mut self, weighter: impl Weighter<K, V>) -> Self {
        self.weighter = Box::new(weighter);
        self
    }

    pub fn build(self) -> GhostQueue<K, V> {
        GhostQueue::new(self.capacity, self.weighter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fmt::Debug,
        sync::atomic::{AtomicUsize, Ordering},
    };

    fn assert_opt_eq<V: PartialEq + Debug>(opt: Option<&V>, v: V) {
        assert!(opt.is_some());
        assert_eq!(opt.unwrap(), &v);
    }

    #[test]
    fn test_record() {
        let mut ghost = GhostBuilder::new(NonZeroUsize::new(2).unwrap()).build();
        ghost.record("Saito", "Asuka");
        ghost.record("Nishino", "Nanase");
        assert_eq!(ghost.usage(), 2);
        assert_eq!(ghost.capacity().get(), 2);
        assert_opt_eq(ghost.get(&"Saito"), "Asuka");
        assert_opt_eq(ghost.get(&"Nishino"), "Nanase");
    }

    #[test]
    fn test_remove() {
        let mut ghost = GhostBuilder::new(NonZeroUsize::new(2).unwrap()).build();
        ghost.record("Saito", "Asuka");
        ghost.record("Nishino", "Nanase");
        assert_opt_eq(ghost.remove(&"Nishino").as_ref(), ("Nishino", "Nanase"));
        assert_eq!(ghost.usage(), 1);
        assert!(ghost.get(&"Nishino").is_none());
        assert_opt_eq(ghost.get(&"Saito"), "Asuka");
    }

    #[test]
    fn test_weighter() {
        let mut cache = GhostBuilder::new(NonZeroUsize::new(10).unwrap())
            .with_weighter(|k: &Vec<i32>, v: &Vec<i32>| k.len() + v.len())
            .build();

        cache.record(vec![1, 2, 3], vec![4, 5, 6]);
        // Current usage should be 6
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.usage(), 6);
        // Cache internal should trigger eviction and <vec![1, 2, 3], vec![4, 5, 6]> is not exist
        cache.record(vec![7, 8, 9], vec![10, 11, 12]);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.usage(), 6);
        assert!(cache.get(&vec![1, 2, 3]).is_none());
    }

    #[test]
    fn test_send() {
        use std::thread;

        let mut ghost = GhostBuilder::new(NonZeroUsize::new(3).unwrap()).build();
        ghost.record("Erik ten Hag", "sacked");

        let handle = thread::spawn(move || {
            assert_eq!(ghost.get(&"Erik ten Hag"), Some(&"sacked"));
        });

        assert!(handle.join().is_ok());
    }

    #[test]
    fn test_no_memory_leaks() {
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        struct DropCounter;

        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        let n = 100;
        for _ in 0..n {
            let mut ghost = GhostBuilder::new(NonZeroUsize::new(1).unwrap()).build();
            for i in 0..n {
                ghost.record(i, DropCounter {});
            }
        }
        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), n * n);
    }
}
