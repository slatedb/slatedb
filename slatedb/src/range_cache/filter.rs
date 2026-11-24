//! Split-block Bloom filters (the layout of Parquet's): one in each run file, copied to the heap
//! when the run is mapped, and one in each large write buffer.
//!
//! A filter is an array of 256-bit blocks. A key's hash picks one block and sets one bit in each
//! of its eight 32-bit words, so a probe reads one cache line. At [`BITS_PER_KEY`] bits per key
//! about 1.25% of absent keys pass.
//!
//! A read hashes its key once, with [`hash_key`], and probes every filter it meets with that hash.

use std::sync::atomic::{AtomicU32, Ordering};

/// The bits per key of every filter of the range cache.
pub(super) const BITS_PER_KEY: usize = 10;

/// The bytes of one block.
pub(super) const BLOCK_BYTES: usize = 32;

const SALT: [u32; 8] = [
    0x47b6_137b,
    0x4497_4d91,
    0x8824_ad5b,
    0xa2b7_289d,
    0x7054_95c7,
    0x2df1_424b,
    0x9efc_4947,
    0x5c6b_fb31,
];

#[inline]
fn fold_mul(a: u64, b: u64) -> u64 {
    let r = (a as u128) * (b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

/// 64-bit wyhash-style key hash mixing key length first. High 32 bits pick the block; low 32
/// bits pick the per-word bits.
#[inline]
pub(super) fn hash_key(key: &[u8]) -> u64 {
    let mut h = fold_mul(
        0x9E37_79B9_7F4A_7C15 ^ key.len() as u64,
        0xD6E8_FEB8_6659_FD93,
    );
    let mut chunks = key.chunks_exact(8);
    for chunk in &mut chunks {
        let mut word = [0u8; 8];
        word.copy_from_slice(chunk);
        h = fold_mul(h ^ u64::from_le_bytes(word), 0xA076_1D64_78BD_642F);
    }
    let rest = chunks.remainder();
    if !rest.is_empty() {
        let mut word = [0u8; 8];
        word[..rest.len()].copy_from_slice(rest);
        h = fold_mul(h ^ u64::from_le_bytes(word), 0xE703_7ED1_A0B4_28DB);
    }
    fold_mul(h, 0x8EBC_6AF0_9C88_C6E3)
}

/// The block of `hash` among `blocks` blocks.
#[inline]
fn block_of(hash: u64, blocks: usize) -> usize {
    (((hash >> 32) * blocks as u64) >> 32) as usize
}

/// The bit that `hash` sets in word `i` of its block.
#[inline]
fn bit(hash: u64, i: usize) -> u32 {
    1u32 << ((hash as u32).wrapping_mul(SALT[i]) >> 27)
}

/// The number of blocks for `keys` keys.
pub(super) fn blocks_for(keys: usize) -> usize {
    (keys.saturating_mul(BITS_PER_KEY)).div_ceil(256).max(1)
}

/// An immutable filter, the filter of a run.
pub(super) struct Filter {
    blocks: Box<[[u32; 8]]>,
}

impl Filter {
    /// A filter of the keys with these hashes.
    pub(super) fn build(hashes: &[u64]) -> Self {
        let mut blocks = vec![[0u32; 8]; blocks_for(hashes.len())].into_boxed_slice();
        let n = blocks.len();
        for &hash in hashes {
            let block = &mut blocks[block_of(hash, n)];
            for (i, word) in block.iter_mut().enumerate() {
                *word |= bit(hash, i);
            }
        }
        Self { blocks }
    }

    /// Returns false if no key with this hash was added.
    #[inline]
    pub(super) fn may_contain(&self, hash: u64) -> bool {
        let block = &self.blocks[block_of(hash, self.blocks.len())];
        let mut all = true;
        for (i, word) in block.iter().enumerate() {
            all &= word & bit(hash, i) != 0;
        }
        all
    }

    /// The bytes of the filter, on the heap and in its file.
    pub(super) fn bytes(&self) -> usize {
        self.blocks.len() * BLOCK_BYTES
    }

    /// The encoding of the filter in a run file: its words in order, little endian.
    pub(super) fn encode(&self, out: &mut Vec<u8>) {
        out.reserve(self.bytes());
        for word in self.blocks.iter().flatten() {
            out.extend_from_slice(&word.to_le_bytes());
        }
    }

    /// Decodes [`Filter::encode`]. Returns `None` if `bytes` is not a whole number of blocks.
    pub(super) fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() || !bytes.len().is_multiple_of(BLOCK_BYTES) {
            return None;
        }
        let blocks = bytes
            .chunks_exact(BLOCK_BYTES)
            .map(|block| {
                let mut words = [0u32; 8];
                for (word, raw) in words.iter_mut().zip(block.chunks_exact(4)) {
                    *word = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
                }
                words
            })
            .collect();
        Some(Self { blocks })
    }
}

/// Single-writer, multi-reader atomic Bloom filter for write buffers.
///
/// The batch writer sets bits with relaxed stores before linking nodes and committing the batch.
/// Bits are never cleared, so concurrent readers only ever see false positives.
pub(super) struct SharedFilter {
    blocks: Box<[[AtomicU32; 8]]>,
}

impl SharedFilter {
    /// An empty filter for about `keys` keys.
    pub(super) fn with_capacity(keys: usize) -> Self {
        let blocks = (0..blocks_for(keys))
            .map(|_| std::array::from_fn(|_| AtomicU32::new(0)))
            .collect();
        Self { blocks }
    }

    /// Adds a key. Only the batch writer calls it.
    #[inline]
    pub(super) fn insert(&self, hash: u64) {
        let block = &self.blocks[block_of(hash, self.blocks.len())];
        for (i, word) in block.iter().enumerate() {
            let mask = bit(hash, i);
            let old = word.load(Ordering::Relaxed);
            if old & mask == 0 {
                word.store(old | mask, Ordering::Relaxed);
            }
        }
    }

    /// Returns false if no key with this hash was added before the caller's sequence number was
    /// committed.
    #[inline]
    pub(super) fn may_contain(&self, hash: u64) -> bool {
        let block = &self.blocks[block_of(hash, self.blocks.len())];
        let mut all = true;
        for (i, word) in block.iter().enumerate() {
            all &= word.load(Ordering::Relaxed) & bit(hash, i) != 0;
        }
        all
    }

    /// The bytes of the filter's blocks.
    pub(super) fn bytes(&self) -> usize {
        self.blocks.len() * BLOCK_BYTES
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(i: u64) -> Vec<u8> {
        format!("key{i:012}").into_bytes()
    }

    /// Every added key passes, in both filters and after an encoding round trip, and about one
    /// absent key in a hundred does.
    #[test]
    fn should_never_reject_an_added_key() {
        for n in [0u64, 1, 7, 100, 5_000, 60_000] {
            let hashes: Vec<u64> = (0..n).map(|i| hash_key(&key(i))).collect();
            let filter = Filter::build(&hashes);
            let shared = SharedFilter::with_capacity(n as usize);
            for &h in &hashes {
                shared.insert(h);
            }
            let mut encoded = Vec::new();
            filter.encode(&mut encoded);
            assert_eq!(encoded.len(), filter.bytes());
            let decoded = Filter::decode(&encoded).unwrap();
            for &h in &hashes {
                assert!(filter.may_contain(h), "n={n}");
                assert!(shared.may_contain(h), "n={n}");
                assert!(decoded.may_contain(h), "n={n}");
            }
            if n >= 5_000 {
                let probes = 200_000u64;
                let passed = (0..probes)
                    .filter(|i| filter.may_contain(hash_key(&key(n + 1 + i))))
                    .count();
                let rate = passed as f64 / probes as f64;
                assert!(rate < 0.03, "false positive rate {rate} at n={n}");
                assert_eq!(shared.bytes(), filter.bytes());
            }
        }
        // An empty filter passes nothing.
        let empty = Filter::build(&[]);
        assert!(!(0..1000).any(|i| empty.may_contain(hash_key(&key(i)))));
    }

    #[test]
    fn should_reject_a_bad_encoding() {
        assert!(Filter::decode(&[]).is_none());
        assert!(Filter::decode(&[0; 33]).is_none());
        assert!(Filter::decode(&[0; 64]).is_some());
    }

    /// Keys that differ only in their last bytes, or only in their length, hash apart, and keys
    /// that count up spread evenly over the blocks.
    #[test]
    fn should_spread_similar_keys() {
        let mut seen = std::collections::HashSet::new();
        for len in 0..40usize {
            for last in 0..=255u8 {
                let mut k = vec![b'k'; len];
                k.push(last);
                assert!(
                    seen.insert(hash_key(&k)),
                    "collision at len {len} last {last}"
                );
            }
        }
        assert!(seen.insert(hash_key(b"")));
        let blocks = 1000;
        let mut counts = vec![0u32; blocks];
        for i in 0..200_000u64 {
            let mut k = b"tenant/".to_vec();
            k.extend_from_slice(&i.to_be_bytes());
            counts[block_of(hash_key(&k), blocks)] += 1;
        }
        let (min, max) = (counts.iter().min().unwrap(), counts.iter().max().unwrap());
        assert!(
            *min > 120 && *max < 280,
            "blocks hold {min} to {max} keys of 200 on average"
        );
    }
}
