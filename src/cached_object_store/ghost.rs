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

    #[inline]
    #[allow(dead_code)]
    pub fn usage(&self) -> usize {
        self.usage
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
