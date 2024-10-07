//! An implementation of a LRU cache. The cache supports `get`, `put`, `remove`,
//! and `remove_entry` operations. Allow us to hold multiple immutable references to values in LruCache.
//! Heavily influenced by the [Design safe collection API with compile-time reference stability in Rust](https://blog.cocl2.com/posts/rust-ref-stable-collection/)

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::num::NonZeroUsize;
use std::ptr::{self, NonNull};

// Phantom lifetime type that can only be subtyped by exactly the same lifetime `'brand`.
// So we can’t create other variables with 'brand lifetime anywhere in safe rust.
// The 'brand can be viewed as a unique ID, and variables that were annotated with
// the lifetime and created together are associated uniquely.
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
        mut value: V,
        _perm: &'perm mut ValuePerm<'brand>,
    ) -> Option<V> {
        let node_ref = self.cache.table.get_mut(&KeyRef { key: &key });

        match node_ref {
            // Key already exists, update the value and move it to the front of the queue
            Some(node_ref) => {
                let node_ptr: *mut LruEntry<K, V> = node_ref.as_ptr();
                // gets a reference to the node to perform a swap and drops it right after
                let node_ref = unsafe { &mut (*(*node_ptr).value.as_mut_ptr()) };
                std::mem::swap(&mut value, node_ref);
                let _ = node_ref;
                self.cache.detach(node_ptr);
                self.cache.attach(node_ptr);
                Some(value)
            }
            None => {
                let (replaced, new_node) = self.cache.replace_or_create_node(key, value);
                let node_ptr: *mut LruEntry<K, V> = new_node.as_ptr();
                self.cache.attach(node_ptr);
                let key_ptr = unsafe { (*node_ptr).key.as_mut_ptr() };
                self.cache.table.insert(KeyRef { key: key_ptr }, new_node);
                replaced.map(|(_, v)| v)
            }
        }
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
                let old_node = unsafe {
                    let mut old_node = *Box::from_raw(node_ptr.as_ptr());
                    std::ptr::drop_in_place((old_node).key.as_mut_ptr());
                    old_node
                };
                cache.detach(node_ptr.as_ptr());
                let LruEntry { key: _, value, .. } = old_node;
                unsafe { Some(value.assume_init()) }
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
                unsafe { Some((key.assume_init(), value.assume_init())) }
            }
        }
    }
}

pub struct LruCache<K, V> {
    table: HashMap<KeyRef<K>, NonNull<LruEntry<K, V>>>,
    capacity: NonZeroUsize,

    // Sentinel nodes
    head: *mut LruEntry<K, V>,
    tail: *mut LruEntry<K, V>,
}

impl<K: Eq + Hash, V> LruCache<K, V> {
    pub fn new(capacity: NonZeroUsize) -> Self {
        let cache = LruCache::<K, V> {
            table: HashMap::with_capacity(capacity.get()),
            capacity,

            head: Box::into_raw(Box::new(LruEntry::new_empty())),
            tail: Box::into_raw(Box::new(LruEntry::new_empty())),
        };

        unsafe {
            (*cache.head).next = cache.tail;
            (*cache.tail).prev = cache.head;
        }

        cache
    }

    fn len(&self) -> usize {
        self.table.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    fn capacity(&self) -> NonZeroUsize {
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

    // If cache if full, evict the oldest entry and get an pointer to the new key-value pair.
    // Otherwise, we create a new entry and get a pointer to the new key-value pair.
    // ## Returns
    // - `None` if the cache is not full
    // - `Some((K, V))` which includes the evicted key-value pair
    fn replace_or_create_node(
        &mut self,
        key: K,
        value: V,
    ) -> (Option<(K, V)>, NonNull<LruEntry<K, V>>) {
        if self.len() == self.capacity.get() {
            let old_key = KeyRef {
                key: unsafe { &(*(*(*self.tail).prev).key.as_ptr()) },
            };
            let old_entry = self.table.remove(&old_key).unwrap();
            let node_ptr: *mut LruEntry<K, V> = old_entry.as_ptr();

            let replaced = unsafe {
                (
                    std::mem::replace(&mut (*node_ptr).key, MaybeUninit::new(key)).assume_init(),
                    std::mem::replace(&mut (*node_ptr).value, MaybeUninit::new(value))
                        .assume_init(),
                )
            };
            self.detach(node_ptr);
            (Some(replaced), old_entry)
        } else {
            (None, unsafe {
                NonNull::new_unchecked(Box::into_raw(Box::new(LruEntry::new(key, value))))
            })
        }
    }

    // Puts a key-value pair into cache. If the key already exists in the cache, then it updates
    // the key's value and returns the old value. Otherwise, `None` is returned.
    pub fn put(&mut self, key: K, value: V) -> Option<V> {
        self.scope(|mut cache, mut perm| cache.put(key, value, &mut perm))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Debug;

    fn assert_opt_eq<V: PartialEq + Debug>(opt: Option<&V>, v: V) {
        assert!(opt.is_some());
        assert_eq!(opt.unwrap(), &v);
    }

    #[test]
    fn test_put_get() {
        let mut cache = LruCache::new(NonZeroUsize::new(2).unwrap());
        cache.scope(|mut cache, mut perm| {
            assert_eq!(cache.put("Saito", "Asuka", &mut perm), None);
            assert_eq!(cache.put("Nishino", "Nanase", &mut perm), None);

            assert_eq!(cache.capacity().get(), 2);
            assert_eq!(cache.len(), 2);
            assert!(!cache.is_empty());
            assert_opt_eq(cache.get(&"Saito", &perm), "Asuka");
            assert_opt_eq(cache.get(&"Nishino", &perm), "Nanase");
        });
    }

    #[test]
    fn test_remove() {
        let mut cache = LruCache::new(NonZeroUsize::new(2).unwrap());
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
    fn concurrent_test() {
        let mut cache: LruCache<&'static str, String> =
            LruCache::new(NonZeroUsize::new(3).unwrap());
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
}
