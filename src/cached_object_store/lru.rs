#![allow(clippy::needless_lifetimes)]
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unwrap_used)]
#![allow(unused)]
//! An implementation of a generic LRU cache. It supports `get`, `put`, `remove`,
//! and `remove_entry`, etc. Allow us to hold multiple immutable references to values in LruCache.
//! Also support dynamical adjustment of the capacity and size aware eviction like moka and foyer.
//! For the use of hold multiple immutable references to values at the same time, see unit tests.
//! Heavily influenced by the [Design safe collection API with compile-time reference stability in Rust]
//! (<https://blog.cocl2.com/posts/rust-ref-stable-collection/>)

// SipHash is too slow, even rustc does not choose it, and we don't need to worry about DoS attacks
// Details: https://github.com/rust-lang/rustc-hash
use hashbrown::HashMap;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::num::NonZeroUsize;
use std::ptr::{self, NonNull};

/// The weighter is used to calculate the weight of the cache entry.
pub trait Weighter<K, V>: Fn(&K, &V) -> usize + Send + Sync + 'static {}
impl<K, V, T> Weighter<K, V> for T where for<'a> T: Fn(&K, &V) -> usize + Send + Sync + 'static {}

// Phantom lifetime type that can only be subtyped by exactly the same lifetime `'brand`.
// So we can’t create other variables with 'brand lifetime anywhere in safe rust.
// The 'brand can be viewed as a unique ID, and variables that were annotated with
// the lifetime and created together are associated uniquely.
#[allow(unused)]
type InvariantLifetime<'brand> = PhantomData<fn(&'brand ()) -> &'brand ()>;

// The operating permissions of the LruCache value itself
// Holding '&self' represents having read permission to the structure of `LruCache`
// Holding '&mut self' represents having write permission to the structure of `LruCache`
// Holding '&perm' represents having read permission to the `value` of `LruCache`
// Holding '&mut perm' represents having write permission to the `value` of `LruCache`
pub struct ValuePerm<'brand> {
    _lifetime: InvariantLifetime<'brand>,
}

// Reference to a key
struct KeyRef<K> {
    key: *const K,
}

impl<K: Hash> Hash for KeyRef<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe { (*self.key).hash(state) }
    }
}

impl<K: PartialEq> PartialEq for KeyRef<K> {
    fn eq(&self, other: &KeyRef<K>) -> bool {
        unsafe { (*self.key).eq(&*other.key) }
    }
}

impl<K: Eq> Eq for KeyRef<K> {}

impl<K: Debug> Debug for KeyRef<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyRef").field("key", &self.key).finish()
    }
}

// Hold LRU key-value pair
struct LruEntry<K, V> {
    key: MaybeUninit<K>,
    value: MaybeUninit<V>,
    prev: *mut LruEntry<K, V>,
    next: *mut LruEntry<K, V>,
}

impl<K, V> LruEntry<K, V> {
    fn new(key: K, value: V) -> LruEntry<K, V> {
        LruEntry {
            key: MaybeUninit::new(key),
            value: MaybeUninit::new(value),
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }
    }

    fn new_empty() -> LruEntry<K, V> {
        LruEntry {
            key: MaybeUninit::uninit(),
            value: MaybeUninit::uninit(),
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }
    }
}

pub struct CacheHandle<'cache, 'brand, K, V> {
    _lifetime: InvariantLifetime<'brand>,
    cache: &'cache mut LruCache<K, V>,
}

impl<'cache, 'brand, K: Hash + Eq, V> CacheHandle<'cache, 'brand, K, V> {
    #[inline]
    pub fn len<'handle, 'perm>(&'handle self) -> usize {
        self.cache.len()
    }

    #[inline]
    pub fn is_empty<'handle, 'perm>(&'handle self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn capacity<'handle, 'perm>(&'handle self) -> NonZeroUsize {
        self.cache.capacity
    }

    pub fn put<'handle, 'perm>(
        &'handle mut self,
        key: K,
        value: V,
        _perm: &'perm mut ValuePerm<'brand>,
    ) {
        // Since we convert update to delete + insert, we only need to create new nodes
        let weight = (self.cache.weighter)(&key, &value);

        // Make sure there is enough space to contain the new entry
        self.cache.try_evict(weight);

        let new_node =
            unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(LruEntry::new(key, value)))) };
        let node_ptr: *mut LruEntry<K, V> = new_node.as_ptr();
        self.cache.attach(node_ptr);
        let key_ptr = unsafe { (*node_ptr).key.as_ptr() };
        self.cache.table.insert(KeyRef { key: key_ptr }, new_node);
        self.cache.usage += weight;
    }

    pub fn get<'handle, 'perm>(
        &mut self,
        key: &K,
        _perm: &'perm ValuePerm<'brand>,
    ) -> Option<&'perm V> {
        let cache = &mut self.cache;
        if let Some(node) = cache.table.get_mut(&KeyRef { key }) {
            let node_ptr: *mut LruEntry<K, V> = node.as_ptr();

            cache.detach(node_ptr);
            cache.attach(node_ptr);

            Some(unsafe { &(*(*node_ptr).value.as_ptr()) })
        } else {
            None
        }
    }

    /// Removes and returns the value corresponding to the key from the `cache` or
    /// `None` if it does not exist
    pub fn remove<'handle, 'perm>(
        &'handle mut self,
        key: &K,
        _perm: &'perm mut ValuePerm<'brand>,
    ) -> Option<V> {
        let cache = &mut self.cache;
        match cache.table.remove(&KeyRef { key }) {
            None => None,
            Some(node_ptr) => {
                let old_node = unsafe { *Box::from_raw(node_ptr.as_ptr()) };
                cache.detach(node_ptr.as_ptr());
                let (k, v) = unsafe { (old_node.key.assume_init(), old_node.value.assume_init()) };
                let weight = (cache.weighter)(&k, &v);
                cache.usage -= weight;
                Some(v)
            }
        }
    }

    /// Removes and returns the key and the value corresponding to the key from the cache or
    /// `None` if it does not exist.
    pub fn remove_entry<'handle, 'perm>(
        &'handle mut self,
        key: &K,
        _perm: &'perm mut ValuePerm<'brand>,
    ) -> Option<(K, V)> {
        let cache = &mut self.cache;
        match cache.table.remove(&KeyRef { key }) {
            None => None,
            Some(node_ptr) => {
                let old_node = unsafe { *Box::from_raw(node_ptr.as_ptr()) };
                cache.detach(node_ptr.as_ptr());
                let LruEntry { key, value, .. } = old_node;
                let (k, v) = unsafe { (key.assume_init(), value.assume_init()) };
                let weight = (cache.weighter)(&k, &v);
                cache.usage -= weight;
                Some((k, v))
            }
        }
    }
}

pub struct LruCache<K, V> {
    table: HashMap<KeyRef<K>, NonNull<LruEntry<K, V>>>,
    capacity: NonZeroUsize,
    weighter: Box<dyn Weighter<K, V>>,
    usage: usize,

    // Sentinel nodes
    head: *mut LruEntry<K, V>,
    tail: *mut LruEntry<K, V>,
}

impl<K: Eq + Hash, V> LruCache<K, V> {
    fn new(capacity: NonZeroUsize, weighter: impl Weighter<K, V>) -> Self {
        let cache = LruCache::<K, V> {
            table: HashMap::default(),
            capacity,
            weighter: Box::new(weighter),
            usage: 0,

            head: Box::into_raw(Box::new(LruEntry::new_empty())),
            tail: Box::into_raw(Box::new(LruEntry::new_empty())),
        };

        unsafe {
            (*cache.head).next = cache.tail;
            (*cache.tail).prev = cache.head;
        }

        cache
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn adjust_capacity(&mut self, new_capacity: NonZeroUsize) {
        self.capacity = new_capacity;
    }

    #[inline]
    pub fn capacity(&self) -> NonZeroUsize {
        self.capacity
    }

    fn detach(&mut self, node: *mut LruEntry<K, V>) {
        unsafe {
            (*(*node).prev).next = (*node).next;
            (*(*node).next).prev = (*node).prev;
        }
    }

    // Attach node after the head
    fn attach(&mut self, node: *mut LruEntry<K, V>) {
        unsafe {
            (*node).next = (*self.head).next;
            (*node).prev = self.head;
            (*self.head).next = node;
            (*(*node).next).prev = node;
        }
    }

    pub fn scope<'cache, F, R>(&'cache mut self, func: F) -> R
    where
        for<'brand> F: FnOnce(CacheHandle<'cache, 'brand, K, V>, ValuePerm<'brand>) -> R,
    {
        let handle = CacheHandle {
            _lifetime: Default::default(),
            cache: self,
        };
        let perm = ValuePerm {
            _lifetime: InvariantLifetime::default(),
        };
        func(handle, perm)
    }

    // Possible evict multiple entries in the LRU to create space for the element to be inserted
    fn try_evict(&mut self, weight: usize) {
        while self.usage + weight > self.capacity.get() {
            // We place the oldest entry at the tail of the list
            let old_key = KeyRef {
                key: unsafe { &(*(*(*self.tail).prev).key.as_ptr()) },
            };
            let node_ptr = self.table.remove(&old_key).unwrap();
            let mut node = unsafe { *Box::from_raw(node_ptr.as_ptr()) };
            self.detach(&mut node);
            let LruEntry { key, value, .. } = node;
            let (k, v) = unsafe { (key.assume_init(), value.assume_init()) };
            self.usage -= (self.weighter)(&k, &v);
        }
    }

    // Puts a key-value pair into cache. If the key already exists in the cache, then it updates
    // the key's value and returns the old value. Otherwise, `None` is returned.
    pub fn put(&mut self, key: K, value: V) -> Option<V> {
        // For simplicity, we convert the update operation into delete + insert
        let old_value = self.scope(|mut cache, mut perm| cache.remove(&key, &mut perm));
        self.scope(|mut cache, mut perm| cache.put(key, value, &mut perm));
        old_value
    }

    /// Returns a reference to the value of the key in the cache or `None` if it is not
    /// present in the cache. Moves the key to the head of the LRU list if it exists.
    pub fn get<'cache>(&'cache mut self, key: &K) -> Option<&'cache V> {
        self.scope(|mut cache, perm| unsafe {
            std::mem::transmute::<_, Option<&'cache V>>(cache.get(key, &perm))
        })
    }

    /// Removes and returns the value corresponding to the key from the cache or
    /// `None` if it does not exist.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.scope(|mut cache, mut perm| cache.remove(key, &mut perm))
    }

    /// Removes and returns the key and the value corresponding to the key from the cache or
    /// `None` if it does not exist.
    pub fn remove_entry(&mut self, key: &K) -> Option<(K, V)> {
        self.scope(|mut cache, mut perm| cache.remove_entry(key, &mut perm))
    }
}

impl<K, V> Drop for LruCache<K, V> {
    fn drop(&mut self) {
        self.table.drain().for_each(|(_, node)| unsafe {
            // Manually drop the key and value
            let mut node = *Box::from_raw(node.as_ptr());
            std::ptr::drop_in_place((node).key.as_mut_ptr());
            std::ptr::drop_in_place((node).value.as_mut_ptr());
        });
        // Due to MaybeUninit, we don't need to worry drop twice
        let _head = unsafe { *Box::from_raw(self.head) };
        let _tail = unsafe { *Box::from_raw(self.tail) };
    }
}

impl<K: Eq + Hash + Debug, V: Debug> Debug for LruCache<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LruCache")
            .field("capacity", &self.capacity)
            .field("usage", &self.usage)
            .field("len", &self.len())
            .finish()
    }
}

// The compiler does not automatically derive Send and Sync for LruCache because it contains
// raw pointers. The raw pointers are safely encapsulated by LruCache though so we can
// implement Send and Sync for it below.
unsafe impl<K: Send, V: Send> Send for LruCache<K, V> {}
unsafe impl<K: Sync, V: Sync> Sync for LruCache<K, V> {}

pub struct LruBuilder<K: Eq + Hash, V> {
    capacity: NonZeroUsize,
    weighter: Box<dyn Weighter<K, V>>,
}

impl<K: Eq + Hash + 'static, V: 'static> LruBuilder<K, V> {
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

    pub fn build(self) -> LruCache<K, V> {
        LruCache::new(self.capacity, self.weighter)
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
    fn test_put_get() {
        let mut cache = LruBuilder::new(NonZeroUsize::new(2).unwrap()).build();
        cache.scope(|mut cache, mut perm| {
            cache.put("Saito", "Asuka", &mut perm);
            cache.put("Nishino", "Nanase", &mut perm);

            assert_eq!(cache.capacity().get(), 2);
            assert_eq!(cache.len(), 2);
            assert!(!cache.is_empty());
            assert_opt_eq(cache.get(&"Saito", &perm), "Asuka");
            assert_opt_eq(cache.get(&"Nishino", &perm), "Nanase");
        });
    }

    #[test]
    fn test_remove() {
        let mut cache = LruBuilder::new(NonZeroUsize::new(2).unwrap()).build();
        cache.scope(|mut cache, mut perm| {
            cache.put("Saito", "Asuka", &mut perm);
            assert_eq!(cache.capacity().get(), 2);
            assert_eq!(cache.len(), 1);
            assert_eq!(cache.remove(&"Saito", &mut perm), Some("Asuka"));
            assert!(cache.remove(&"Saito", &mut perm).is_none());
        });
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
        assert!(cache.remove_entry(&"Saito").is_none());
    }

    #[test]
    fn test_weighter() {
        let mut cache = LruBuilder::new(NonZeroUsize::new(10).unwrap())
            .with_weighter(|k: &Vec<i32>, v: &Vec<i32>| k.len() + v.len())
            .build();
        cache.scope(|mut cache, mut perm| {
            cache.put(vec![1, 2, 3], vec![4, 5, 6], &mut perm);
            // Current usage should be 6
            assert_eq!(cache.len(), 1);
            // Cache internal should trigger eviction and <vec![1, 2, 3], vec![4, 5, 6]> is not exist
            cache.put(vec![7, 8, 9], vec![10, 11, 12], &mut perm);
            assert_eq!(cache.len(), 1);
            assert!(cache.get(&vec![1, 2, 3], &perm).is_none());
        });
    }

    #[test]
    fn concurrent_test() {
        let mut cache: LruCache<&'static str, String> =
            LruBuilder::new(NonZeroUsize::new(3).unwrap()).build();
        let out = cache.scope(|mut handle, mut perm| {
            handle.put("a", "bb".to_string(), &mut perm);
            handle.put("b", "cc".to_string(), &mut perm);
            handle.put("c", "dd".to_string(), &mut perm);

            let futs = ["a", "b", "c"].iter().map(|k| {
                let v = handle.get(k, &perm).unwrap();

                async {
                    let v: &String = v;
                    v.get(..1).unwrap().to_string()
                }
            });

            let fut = async {
                let out = futures::future::join_all(futs).await;
                out.join(" ")
            };

            futures::executor::block_on(fut)
        });

        assert_eq!(out, "b c d".to_string());
    }

    #[test]
    fn test_send() {
        use std::thread;

        let mut cache = LruBuilder::new(NonZeroUsize::new(3).unwrap()).build();
        cache.put(1, "a");

        let handle = thread::spawn(move || {
            assert_eq!(cache.get(&1), Some(&"a"));
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
            let mut cache = LruBuilder::new(NonZeroUsize::new(1).unwrap()).build();
            for i in 0..n {
                cache.put(i, DropCounter {});
            }
        }
        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), n * n);
    }
}
