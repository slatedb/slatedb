use crate::config::Clock;
use crate::iter::KeyValueIterator;
use crate::types::{KeyValue, RowAttributes, ValueDeletable};
use bytes::Bytes;
use rand::Rng;
use std::sync::atomic::{AtomicI64, Ordering};

pub(crate) async fn assert_iterator<T: KeyValueIterator>(
    iterator: &mut T,
    entries: &[(Vec<u8>, ValueDeletable, RowAttributes)],
) {
    // We use Vec<u8> instead of &[u8] for the keys in the entries for several reasons:
    // 1. Ownership and Lifetime: Vec<u8> owns its data, while &[u8] is a borrowed slice.
    //    Using Vec<u8> allows us to store and manipulate the data without worrying about lifetimes.

    // 2. Flexibility: Vec<u8> can be easily cloned, extended, or modified if needed.
    //    This is particularly useful when working with keys that might need to be adjusted or compared.

    // 3. Consistency with Bytes: The iterator returns keys as Bytes, which is similar to Vec<u8> in that
    //    it owns its data. Using Vec<u8> in our test data maintains this consistency.

    // 4. Avoid Slice Referencing Issues: Using &[u8] could lead to complicated lifetime issues,
    //    especially if the original data the slice refers to goes out of scope.

    // 5. Performance: While Vec<u8> has a slight overhead compared to &[u8], the difference is
    //    negligible for most use cases, especially in tests where convenience and clarity are prioritized.

    for (expected_k, expected_v, expected_attr) in entries.iter() {
        let kv = iterator
            .next_entry()
            .await
            .expect("iterator next_entry failed")
            .expect("expected iterator to return a value");
        assert_eq!(kv.key, Bytes::from(expected_k.clone()));
        assert_eq!(kv.value, *expected_v);
        assert_eq!(
            kv.expire_ts, expected_attr.expire_ts,
            "Attribute expire_ts mismatch at key {:?}",
            kv.key
        );
    }
    assert!(iterator
        .next_entry()
        .await
        .expect("iterator next_entry failed")
        .is_none());
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
