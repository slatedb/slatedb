use crate::iter::KeyValueIterator;
use crate::types::{KeyValue, ValueDeletable};
use bytes::Bytes;

pub(crate) async fn assert_iterator<T: KeyValueIterator>(
    iterator: &mut T,
    entries: &[(&'static [u8], ValueDeletable)],
) {
    for (expected_k, expected_v) in entries.iter() {
        if let Some(kv) = iterator.next_entry().await.unwrap() {
            assert_eq!(kv.key, Bytes::from(*expected_k));
            assert_eq!(kv.value, *expected_v);
        } else {
            panic!("expected next_entry to return a value")
        }
    }
    assert!(iterator.next_entry().await.unwrap().is_none());
}

pub fn assert_kv(kv: &KeyValue, key: &[u8], val: &[u8]) {
    assert_eq!(kv.key, key);
    assert_eq!(kv.value, val);
}
