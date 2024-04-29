use std::sync::Arc;

use crate::error::SlateDBError;
use crate::iter::{KeyValue, KeyValueIterator};
use bytes::Bytes;
use crossbeam_skiplist::map::Iter;
use crossbeam_skiplist::SkipMap;
use tokio::sync::Notify;

pub(crate) struct MemTable {
    pub(crate) map: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) flush_notify: Arc<Notify>,
}

pub struct MemTableIterator<'a>(Iter<'a, Bytes, Bytes>);

impl<'a> KeyValueIterator for MemTableIterator<'a> {
    async fn next(&mut self) -> Result<Option<KeyValue>, SlateDBError> {
        Ok(self.0.next().map(|entry| KeyValue {
            key: entry.key().clone(),
            value: entry.value().clone(),
        }))
    }
}

impl MemTable {
    pub(crate) fn new() -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            flush_notify: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.map.get(key).map(|entry| entry.value().clone())
    }

    pub(crate) fn iter(&self) -> MemTableIterator {
        MemTableIterator(self.map.iter())
    }

    pub(crate) async fn put(&self, key: &[u8], value: &[u8]) {
        self.map
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        self.flush_notify.notified().await;
    }

    pub(crate) async fn delete(&self, key: &[u8]) {
        self.map.insert(Bytes::copy_from_slice(key), Bytes::new());
        self.flush_notify.notified().await;
    }
}
