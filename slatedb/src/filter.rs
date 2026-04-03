use std::sync::Arc;

use crate::filter_policy::{Filter, FilterBuilder, FilterQuery, FilterTarget};
use crate::types::RowEntry;
use crate::utils::clamp_allocated_size_bytes;
use bytes::{Buf, BufMut, Bytes};
use siphasher::sip::SipHasher13;

pub struct BloomFilterBuilder {
    bits_per_key: u32,
    key_hashes: Vec<u64>,
}

#[derive(PartialEq, Eq)]
pub struct BloomFilter {
    num_probes: u16,
    buffer: Bytes,
}

impl BloomFilterBuilder {
    pub fn new(bits_per_key: u32) -> Self {
        Self {
            bits_per_key,
            key_hashes: Vec::new(),
        }
    }

    #[cfg(test)]
    fn add_key(&mut self, key: &[u8]) {
        self.key_hashes.push(filter_hash(key))
    }

    fn filter_size_bytes(num_keys: u32, bits_per_key: u32) -> usize {
        let filter_bits = num_keys * bits_per_key;
        // compute filter bytes rounded up to the number of bytes required to fit the filter
        filter_bits.div_ceil(8) as usize
    }

    fn build_filter(&self) -> BloomFilter {
        let num_probes = optimal_num_probes(self.bits_per_key);
        let filter_bytes =
            BloomFilterBuilder::filter_size_bytes(self.key_hashes.len() as u32, self.bits_per_key);
        let filter_bits = (filter_bytes * 8) as u32;
        let mut buffer = vec![0x00; filter_bytes];
        for k in self.key_hashes.iter() {
            let probes = probes_for_key(*k, num_probes, filter_bits);
            for p in probes {
                set_bit(p as usize, &mut buffer)
            }
        }
        BloomFilter {
            num_probes,
            buffer: Bytes::from(buffer),
        }
    }
}

impl BloomFilter {
    pub fn decode(mut buf: &[u8]) -> BloomFilter {
        let num_probes = buf.get_u16();
        BloomFilter {
            num_probes,
            buffer: Bytes::copy_from_slice(buf),
        }
    }

    /// estimate the size of BloomFilter encoded in SST
    pub fn estimate_encoded_size(num_keys: u32, filter_bits_per_key: u32) -> usize {
        let filter_bytes = BloomFilterBuilder::filter_size_bytes(num_keys, filter_bits_per_key);
        let num_probes_size = std::mem::size_of::<u16>();
        let checksum_len = std::mem::size_of::<u32>();
        filter_bytes + num_probes_size + checksum_len
    }

    fn filter_bits(&self) -> u32 {
        (self.buffer.len() * 8) as u32
    }

    pub fn might_contain(&self, hash: u64) -> bool {
        for p in probes_for_key(hash, self.num_probes, self.filter_bits()) {
            if !check_bit(p as usize, &self.buffer) {
                return false;
            }
        }
        true
    }
}

impl FilterBuilder for BloomFilterBuilder {
    fn add_entry(&mut self, entry: &RowEntry) {
        self.key_hashes.push(filter_hash(&entry.key));
    }

    fn build(&self) -> Arc<dyn Filter> {
        Arc::new(self.build_filter())
    }
}

impl Filter for BloomFilter {
    fn might_match(&self, query: &FilterQuery) -> bool {
        match &query.target {
            FilterTarget::Point(key) => self.might_contain(filter_hash(key.as_ref())),
            // No prefix extractor configured — cannot answer prefix queries
            FilterTarget::Prefix(_) => true,
        }
    }

    fn encode(&self, writer: &mut dyn BufMut) {
        writer.put_u16(self.num_probes);
        writer.put_slice(&self.buffer);
    }

    fn size(&self) -> usize {
        self.buffer.len()
    }

    fn clamp_allocated_size(&self) -> Arc<dyn Filter> {
        Arc::new(BloomFilter {
            num_probes: self.num_probes,
            buffer: clamp_allocated_size_bytes(&self.buffer),
        })
    }
}

pub fn filter_hash(key: &[u8]) -> u64 {
    // sip hash is the default rust hash function, however its only
    // accessible by creating DefaultHasher. Direct use of SipHasher13 in
    // std is deprecated. We don't want to use DefaultHasher because the
    // underlying algorithm could change. Therefore, we use SipHasher13 from
    // the siphasher crate
    let hasher = SipHasher13::new();
    hasher.hash(key)
}

fn probes_for_key(key_hash: u64, num_probes: u16, filter_bits: u32) -> Vec<u32> {
    // implements enhanced double hashing from:
    // https://www.khoury.northeastern.edu/~pete/pub/bloom-filters-verification.pdf
    // as suggested by the author P. Dillinger for RocksDB's legacy filters here:
    // https://github.com/facebook/rocksdb/issues/4120
    let mut probes = vec![0u32; num_probes as usize];
    let filter_bits = filter_bits as u64;
    let mut h = ((key_hash << 32) >> 32) % filter_bits; // lower 32 bits of hash
    let mut delta = (key_hash >> 32) % filter_bits; // higher 32 bits of hash
    for i in 0..num_probes {
        delta = (delta + i as u64) % filter_bits;
        probes[i as usize] = h as u32;
        h = (h + delta) % filter_bits;
    }
    probes
}

fn check_bit(bit: usize, buf: &[u8]) -> bool {
    let byte = bit / 8;
    let bit_in_byte = bit % 8;
    (buf[byte] & (1 << bit_in_byte)) != 0
}

fn set_bit(bit: usize, buf: &mut [u8]) {
    let byte = bit / 8;
    let bit_in_byte = bit % 8;
    buf[byte] |= 1 << bit_in_byte;
}

fn optimal_num_probes(bits_per_key: u32) -> u16 {
    // bits_per_key * ln(2)
    // https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions
    (bits_per_key as f32 * 0.69) as u16
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_set_specified_bit_only() {
        // some hard-coded test cases
        let cases = [
            (
                vec![0xF0u8, 0xABu8, 0x9Cu8],
                vec![0xF8u8, 0xABu8, 0x9Cu8],
                3,
            ),
            (
                vec![0xF0u8, 0xABu8, 0x9Cu8],
                vec![0xF0u8, 0xAFu8, 0x9Cu8],
                10,
            ),
        ];
        for (buf, expected, bit) in cases.iter() {
            let mut updated = buf.clone();
            set_bit(*bit, &mut updated);
            assert_eq!(updated, *expected);
        }
        // more of a property style test
        let nbytes = 4;
        for byte in 0..nbytes {
            for i in 0..8 {
                let mut buf = vec![0u8; nbytes];
                let bit = byte * 8 + i;
                set_bit(bit, &mut buf);
                for unset in 0..nbytes {
                    if unset != byte {
                        assert_eq!(buf[unset], 0)
                    } else {
                        assert_eq!(buf[byte], 1 << i);
                    }
                }
            }
        }
    }

    #[test]
    fn test_set_bits_doesnt_unset_bits() {
        let mut buf = vec![0xFFu8; 3];
        for i in 0..24 {
            set_bit(i, &mut buf);
            assert_eq!(buf, vec![0xFFu8; 3]);
        }
    }

    #[test]
    fn test_check_bits() {
        let num_bytes = 4;
        for i in 0..num_bytes {
            for b in 0..8 {
                let bit = i * 8 + b;
                let mut buf = vec![0u8; num_bytes];
                buf[i] = 1 << b;
                for checked in 0..num_bytes * 8 {
                    let bit_on = check_bit(checked, buf.as_slice());
                    assert_eq!(bit_on, bit == checked);
                }
            }
        }
    }

    #[test]
    fn test_compute_probes() {
        // h1 = 0xDEADBEEF, h2 = 0xDF77EF56
        let hash = 0xDF77EF56DEADBEEFu64;
        let probes = probes_for_key(hash, 7, 1000000);
        assert_eq!(
            probes,
            vec![
                928559, // h1
                107781, // h1 + h2
                287004, // h1 + h2 + h2 + 1
                466229, // h1 + h2 + h2 + 1 + h2 + 1 + 2
                645457, // h1 + h2 + h2 + 1 + h2 + 1 + 2 + h2 + 1 + 2 + 3
                824689, 3926,
            ]
        );
    }

    #[test]
    fn test_filter_effective() {
        let keys_to_test = 100000;
        let key_sz = size_of::<u32>();
        let mut builder = BloomFilterBuilder::new(10);
        for i in 0..keys_to_test {
            let mut bytes = BytesMut::with_capacity(key_sz);
            bytes.reserve(key_sz);
            bytes.put_u32(i);
            builder.add_key(bytes.freeze().as_ref());
        }
        let filter = builder.build_filter();

        // check all entries in filter
        for i in 0..keys_to_test {
            let mut bytes = BytesMut::with_capacity(key_sz);
            bytes.reserve(key_sz);
            bytes.put_u32(i);
            let hash = filter_hash(bytes.freeze().as_ref());
            assert!(filter.might_contain(hash));
        }

        // check false positives
        let mut fp = 0;
        for i in keys_to_test..2 * keys_to_test {
            let mut bytes = BytesMut::with_capacity(key_sz);
            bytes.reserve(key_sz);
            bytes.put_u32(i);
            let hash = filter_hash(bytes.freeze().as_ref());
            if filter.might_contain(hash) {
                fp += 1;
            }
        }

        // observed fp is .0087
        assert!((fp as f32 / keys_to_test as f32) < 0.01);
    }

    #[test]
    fn test_bloom_filter_size() {
        let mut builder = BloomFilterBuilder::new(10);
        builder.add_key(b"test_key");
        let filter = builder.build_filter();

        // The exact size may vary, so we'll check if it's greater than zero
        assert!(
            filter.size() > 0,
            "Bloom filter size should be greater than zero"
        );

        assert_eq!(
            filter.size(),
            filter.buffer.len(),
            "Size should match buffer length"
        );
    }

    #[test]
    fn test_should_clamp_allocated_bytes() {
        let mut builder = BloomFilterBuilder::new(10);
        for i in 0..100 {
            builder.add_key(format!("{}", i).as_bytes());
        }
        let filter = builder.build_filter();
        let original_size = Filter::size(&filter);
        let mut extended_buf = BytesMut::with_capacity(original_size + 100);
        extended_buf.put(filter.buffer.as_ref());
        extended_buf.put_bytes(0u8, 100);
        let filter = BloomFilter {
            buffer: extended_buf.freeze().slice(0..filter.buffer.len()),
            ..filter
        };

        let clamped = Filter::clamp_allocated_size(&filter);

        assert_eq!(clamped.size(), Filter::size(&filter));
        // Encode both and verify they produce the same bytes
        let mut original_bytes = Vec::new();
        Filter::encode(&filter, &mut original_bytes);
        let mut clamped_bytes = Vec::new();
        clamped.encode(&mut clamped_bytes);
        assert_eq!(original_bytes, clamped_bytes);
    }

    #[test]
    fn test_estimate_encoded_size() {
        // Test with zero keys
        assert_eq!(BloomFilter::estimate_encoded_size(0, 10), 6); // 0 bytes + 2 bytes probes + 4 bytes checksum

        // Test with one key
        let bits_per_key = 10;
        let filter_bytes = BloomFilterBuilder::filter_size_bytes(1, bits_per_key);
        let expected_size = filter_bytes + 2 + 4; // filter_bytes + probes + checksum
        assert_eq!(
            BloomFilter::estimate_encoded_size(1, bits_per_key),
            expected_size
        );

        // Test with multiple keys
        let num_keys = 100;
        let bits_per_key = 10;
        let filter_bytes = BloomFilterBuilder::filter_size_bytes(num_keys, bits_per_key);
        let expected_size = filter_bytes + 2 + 4; // filter_bytes + probes + checksum
        assert_eq!(
            BloomFilter::estimate_encoded_size(num_keys, bits_per_key),
            expected_size
        );

        // Test with large number of keys
        let num_keys = 100_000_000;
        let bits_per_key = 10;
        let filter_bytes = BloomFilterBuilder::filter_size_bytes(num_keys, bits_per_key);
        let expected_size = filter_bytes + 2 + 4; // filter_bytes + probes + checksum
        assert_eq!(
            BloomFilter::estimate_encoded_size(num_keys, bits_per_key),
            expected_size
        );
    }
}
