use crate::config::Clock;
use crate::iter::KeyValueIterator;
use crate::types::{KeyValue, RowAttributes, RowEntry};
use bytes::Bytes;
use rand::Rng;
use std::sync::atomic::{AtomicI64, Ordering};

/// Asserts that the iterator returns the exact set of expected values in correct order.
pub(crate) async fn assert_iterator<T: KeyValueIterator>(iterator: &mut T, entries: Vec<RowEntry>) {
    for expected_entry in entries.iter() {
        assert_next_entry(iterator, expected_entry).await;
    }
    assert!(iterator
        .next_entry()
        .await
        .expect("iterator next_entry failed")
        .is_none());
}

pub(crate) async fn assert_next_entry<T: KeyValueIterator>(
    iterator: &mut T,
    expected_entry: &RowEntry,
) {
    let actual_entry = iterator
        .next_entry()
        .await
        .expect("iterator next_entry failed")
        .expect("expected iterator to return a value");
    assert_eq!(actual_entry, expected_entry.clone())
}

pub fn assert_kv(kv: &KeyValue, key: &[u8], val: &[u8]) {
    assert_eq!(kv.key, key);
    assert_eq!(kv.value, val);
}

pub(crate) fn gen_attrs(ts: i64) -> RowAttributes {
    RowAttributes {
        ts: Some(ts),
        expire_ts: None,
    }
}

pub(crate) fn gen_empty_attrs() -> RowAttributes {
    RowAttributes {
        ts: None,
        expire_ts: None,
    }
}

pub(crate) struct TestClock {
    pub(crate) ticker: AtomicI64,
}

impl TestClock {
    pub(crate) fn new() -> TestClock {
        TestClock {
            ticker: AtomicI64::new(0),
        }
    }
}

impl Clock for TestClock {
    fn now(&self) -> i64 {
        self.ticker.load(Ordering::SeqCst)
    }
}

pub(crate) fn gen_rand_bytes(n: usize) -> Bytes {
    let mut rng = rand::thread_rng();
    let random_bytes: Vec<u8> = (0..n).map(|_| rng.gen()).collect();
    Bytes::from(random_bytes)
}

// it seems that insta still does not allow to customize the snapshot path in insta.yaml,
// we can remove this macro once insta supports it.
macro_rules! assert_debug_snapshot {
    ($name:expr, $output:expr) => {
        let mut settings = insta::Settings::clone_current();
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata/snapshots");
        settings.set_snapshot_path(path);
        settings.bind(|| insta::assert_debug_snapshot!($name, $output));
    };
}

pub(crate) use assert_debug_snapshot;
