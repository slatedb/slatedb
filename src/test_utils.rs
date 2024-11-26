use crate::config::Clock;
use crate::iter::KeyValueIterator;
use crate::types::{KeyValue, RowAttributes, ValueDeletable};
use bytes::{BufMut, Bytes, BytesMut};
use std::sync::atomic::{AtomicI64, Ordering};

// this complains because we include these in the bencher feature but they are only
// used for cfg(test)
#[allow(dead_code)]
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

    for expected in entries.iter() {
        assert_next_entry(iterator, expected).await;
    }
    assert!(iterator
        .next_entry()
        .await
        .expect("iterator next_entry failed")
        .is_none());
}

// this complains because we include these in the bencher feature but they are only
// used for cfg(test)
#[allow(dead_code)]
pub(crate) async fn assert_next_entry<T: KeyValueIterator>(
    iterator: &mut T,
    expected: &(Vec<u8>, ValueDeletable, RowAttributes),
) {
    let (expected_k, expected_v, expected_attr) = expected;
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

// this complains because we include these in the bencher feature but they are only
// used for cfg(test)
#[allow(dead_code)]
pub fn assert_kv(kv: &KeyValue, key: &[u8], val: &[u8]) {
    assert_eq!(kv.key, key);
    assert_eq!(kv.value, val);
}

#[allow(dead_code)]
pub(crate) fn gen_attrs(ts: i64) -> RowAttributes {
    RowAttributes {
        ts: Some(ts),
        expire_ts: None,
    }
}

#[allow(dead_code)]
pub(crate) fn gen_empty_attrs() -> RowAttributes {
    RowAttributes {
        ts: None,
        expire_ts: None,
    }
}

pub(crate) struct TestClock {
    pub(crate) ticker: AtomicI64,
}

#[allow(dead_code)]
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

#[derive(Clone)]
pub(crate) struct OrderedBytesGenerator {
    suffix: Bytes,
    bytes: Vec<u8>,
    min: u8,
    max: u8,
}

impl OrderedBytesGenerator {
    #[allow(dead_code)]
    pub(crate) fn new_with_suffix(suffix: &[u8], bytes: &[u8]) -> Self {
        Self::new(suffix, bytes, u8::MIN, u8::MAX)
    }

    // this complains because we include these in the bencher feature but they are only
    // used for cfg(test)
    #[allow(dead_code)]
    pub(crate) fn new_with_byte_range(bytes: &[u8], min: u8, max: u8) -> Self {
        Self::new(&[], bytes, min, max)
    }

    pub(crate) fn new(suffix: &[u8], bytes: &[u8], min: u8, max: u8) -> Self {
        let bytes = Vec::from(bytes);
        Self {
            suffix: Bytes::copy_from_slice(suffix),
            bytes,
            min,
            max,
        }
    }

    pub(crate) fn next(&mut self) -> Bytes {
        let mut result = BytesMut::with_capacity(self.bytes.len() + std::mem::size_of::<u32>());
        result.put_slice(self.bytes.as_slice());
        result.put(self.suffix.as_ref());
        self.increment();
        result.freeze()
    }

    fn increment(&mut self) {
        let mut pos = self.bytes.len() - 1;
        while self.bytes[pos] == self.max {
            self.bytes[pos] = self.min;
            pos -= 1;
        }
        self.bytes[pos] += 1;
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes};

    use crate::test_utils::OrderedBytesGenerator;

    #[test]
    fn test_should_generate_ordered_bytes() {
        let mut suffix = Vec::<u8>::new();
        suffix.put_u32(3735928559);
        let start = [0u8, 0u8, 0u8];
        let mut gen = OrderedBytesGenerator::new(suffix.as_ref(), &start, 0, 2);

        let expected = [
            [0u8, 0u8, 0u8, 0xde, 0xad, 0xbe, 0xef],
            [0u8, 0u8, 1u8, 0xde, 0xad, 0xbe, 0xef],
            [0u8, 0u8, 2u8, 0xde, 0xad, 0xbe, 0xef],
            [0u8, 1u8, 0u8, 0xde, 0xad, 0xbe, 0xef],
            [0u8, 1u8, 1u8, 0xde, 0xad, 0xbe, 0xef],
            [0u8, 1u8, 2u8, 0xde, 0xad, 0xbe, 0xef],
            [0u8, 2u8, 0u8, 0xde, 0xad, 0xbe, 0xef],
            [0u8, 2u8, 1u8, 0xde, 0xad, 0xbe, 0xef],
            [0u8, 2u8, 2u8, 0xde, 0xad, 0xbe, 0xef],
            [1u8, 0u8, 0u8, 0xde, 0xad, 0xbe, 0xef],
            [1u8, 0u8, 1u8, 0xde, 0xad, 0xbe, 0xef],
            [1u8, 0u8, 2u8, 0xde, 0xad, 0xbe, 0xef],
            [1u8, 1u8, 0u8, 0xde, 0xad, 0xbe, 0xef],
            [1u8, 1u8, 1u8, 0xde, 0xad, 0xbe, 0xef],
            [1u8, 1u8, 2u8, 0xde, 0xad, 0xbe, 0xef],
            [1u8, 2u8, 0u8, 0xde, 0xad, 0xbe, 0xef],
            [1u8, 2u8, 1u8, 0xde, 0xad, 0xbe, 0xef],
            [1u8, 2u8, 2u8, 0xde, 0xad, 0xbe, 0xef],
            [2u8, 0u8, 0u8, 0xde, 0xad, 0xbe, 0xef],
            [2u8, 0u8, 1u8, 0xde, 0xad, 0xbe, 0xef],
            [2u8, 0u8, 2u8, 0xde, 0xad, 0xbe, 0xef],
            [2u8, 1u8, 0u8, 0xde, 0xad, 0xbe, 0xef],
            [2u8, 1u8, 1u8, 0xde, 0xad, 0xbe, 0xef],
            [2u8, 1u8, 2u8, 0xde, 0xad, 0xbe, 0xef],
            [2u8, 2u8, 0u8, 0xde, 0xad, 0xbe, 0xef],
            [2u8, 2u8, 1u8, 0xde, 0xad, 0xbe, 0xef],
        ];
        for e in expected.iter() {
            assert_eq!(gen.next(), Bytes::copy_from_slice(e))
        }
    }
}
