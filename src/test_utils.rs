use bytes::{BufMut, Bytes, BytesMut};

use crate::iter::KeyValueIterator;
use crate::types::{KeyValue, ValueDeletable};

// this complains because we include these in the db_bench feature but they are only
// used for cfg(test)
#[allow(dead_code)]
pub(crate) async fn assert_iterator<T: KeyValueIterator>(
    iterator: &mut T,
    entries: &[(&'static [u8], ValueDeletable)],
) {
    for (expected_k, expected_v) in entries.iter() {
        let kv = iterator
            .next_entry()
            .await
            .expect("iterator next_entry failed")
            .expect("expected iterator to return a value");
        assert_eq!(kv.key, Bytes::from(*expected_k));
        assert_eq!(kv.value, *expected_v);
    }
    assert!(iterator
        .next_entry()
        .await
        .expect("iterator next_entry failed")
        .is_none());
}

// this complains because we include these in the db_bench feature but they are only
// used for cfg(test)
#[allow(dead_code)]
pub fn assert_kv(kv: &KeyValue, key: &[u8], val: &[u8]) {
    assert_eq!(kv.key, key);
    assert_eq!(kv.value, val);
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

    // this complains because we include these in the db_bench feature but they are only
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
