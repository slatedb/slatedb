use std::{
    cell::RefCell,
    collections::{BTreeMap, HashSet},
    ops::{Deref, DerefMut, RangeBounds},
};

/// DstState keeps track of the expected state of the database.
///
/// It keeps a copy of the data that is expected to be in the database using a [SizedBTreeMap].
///
/// It also keeps a set of keys that are expected to be committed. This is used to track the
/// expected state of the database after a flush operation.
pub struct DstState {
    data: SizedBTreeMap<Vec<u8>, Vec<u8>>,
    committed: RefCell<HashSet<Vec<u8>>>,
}

impl Default for DstState {
    fn default() -> Self {
        Self::new()
    }
}

impl DstState {
    pub fn new() -> Self {
        Self {
            data: SizedBTreeMap::new(),
            committed: RefCell::new(HashSet::new()),
        }
    }

    /// Check if a key is expected to be committed.
    ///
    /// This is used to track the expected state of the database after a flush operation.
    ///
    /// If the key is not present in the database, it is not expected to be committed.
    pub fn is_committed<K: AsRef<[u8]>>(&self, key: K) -> bool {
        let key = key.as_ref();
        self.committed.borrow().contains(key)
    }

    /// Commit the current state of the database.
    ///
    /// This is called after a flush operation to update the expected state of the database.
    ///
    /// It removes any keys that are no longer present in the database and adds any keys that are
    /// now present in the database.
    pub fn commit(&self) {
        let mut deleted = Vec::new();
        let mut committed = self.committed.borrow_mut();

        for key in committed.iter() {
            if !self.data.contains(key) {
                deleted.push(key.clone());
            }
        }

        for key in deleted {
            committed.remove(&key);
        }

        for key in self.data.keys() {
            committed.insert(key.clone());
        }
    }
}

impl Deref for DstState {
    type Target = SizedBTreeMap<Vec<u8>, Vec<u8>>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl DerefMut for DstState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

/// A [BTreeMap] that tracks the total size of the map in bytes. This helps
/// us keep an upper-bound on the memory usage.
pub struct SizedBTreeMap<K, V>
where
    K: Ord + AsRef<[u8]>,
    V: Ord + AsRef<[u8]>,
{
    inner: BTreeMap<K, V>,
    pub(crate) size_bytes: usize,
}

impl<K, V> Default for SizedBTreeMap<K, V>
where
    K: Ord + AsRef<[u8]>,
    V: Ord + AsRef<[u8]>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> SizedBTreeMap<K, V>
where
    K: Ord + AsRef<[u8]>,
    V: Ord + AsRef<[u8]>,
{
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
            size_bytes: 0,
        }
    }

    pub fn insert(&mut self, key: K, val: V) {
        self.size_bytes += key.as_ref().len() + val.as_ref().len();
        if let Some(old_value) = self.inner.insert(key, val) {
            self.size_bytes -= old_value.as_ref().len();
        }
    }

    pub fn remove(&mut self, key: &K) {
        if let Some(val) = self.inner.remove(key) {
            self.size_bytes -= key.as_ref().len() + val.as_ref().len();
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    pub fn range(
        &self,
        range: impl RangeBounds<K>,
    ) -> std::collections::btree_map::Range<'_, K, V> {
        self.inner.range(range)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn keys(&self) -> std::collections::btree_map::Keys<'_, K, V> {
        self.inner.keys()
    }

    pub fn contains(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }
}
