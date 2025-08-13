use std::cmp;

use chrono::{DateTime, Utc};
use serde::de::{self};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::utils::{sign_extend, BitReader, BitWriter};

#[derive(PartialEq, Clone, Copy)]
pub(crate) struct TrackedSeq {
    pub(crate) seq: u64,
    pub(crate) ts: DateTime<Utc>,
}

/// Rounding behavior for non-exact matches in sequence-timestamp lookups.
#[allow(dead_code)]
#[derive(PartialEq)]
pub(crate) enum FindOption {
    /// Round up to the next higher value when no exact match is found.
    RoundUp,
    /// Round down to the next lower value when no exact match is found.
    RoundDown,
}

/// Tracks sequence number ↔ timestamp relationships with bounded memory using tiered storage.
///
/// Uses a multi-tiered approach where each tier stores data at different resolutions (step sizes).
/// When a tier reaches capacity, it downsamples by doubling the step size and keeping every other
/// timestamp. This maintains bounded memory while preserving recent data at higher resolution.
///
/// Timestamps are compressed using Gorilla encoding (delta-of-deltas) for efficient storage.
/// Supports bidirectional queries with rounding options for approximate matches.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub(crate) struct TieredSequenceTracker {
    /// The data, tiered by step size where the step size
    /// indicates the granularity at which the sequence
    /// numbers are tracked. The tiers are ordered by the
    /// granularity (i.e. `tiers[0]` has the lowest step)
    pub(crate) tiers: Vec<Tier>,
}

#[allow(dead_code)]
impl TieredSequenceTracker {
    pub(crate) fn new(num_tiers: u32, capacity: u32) -> Self {
        // only support a single tier for now because the
        // downsampling algorithm for fixed size multi-tiered
        // sequence tracking is a little more complicated, but
        // we model it with multiple tiers so that the serialization
        // format will be compatible with a multi-tiered tracker
        assert!(num_tiers == 1, "todo! implement multi-tiers");

        let tier_capacity = capacity / num_tiers;
        let tiers = (0..num_tiers)
            .map(|i| Tier::new(1 << i, tier_capacity))
            .collect();

        Self { tiers }
    }

    pub(crate) fn insert(&mut self, seq: TrackedSeq) {
        self.tiers[0].insert(seq.seq, seq.ts.timestamp());
    }

    pub(crate) fn find_ts(&self, seq: u64, find_opt: FindOption) -> Option<DateTime<Utc>> {
        if let Some(ts) = self.tiers[0].find_ts(seq, find_opt) {
            DateTime::from_timestamp(ts, 0)
        } else {
            None
        }
    }

    pub(crate) fn find_seq(&self, ts: DateTime<Utc>, find_opt: FindOption) -> Option<u64> {
        self.tiers[0].find_seq(ts.timestamp(), find_opt)
    }
}

/// A tier contains a mapping from sequence numbers to
/// timestamps at a given "resolution", defined by a
/// step parameter which is how spaced apart the timestamps
/// are. A tier with step `4` and a start sequence number
/// of 10, for example, would contain the timestamps for
/// sequence numbers 10, 14, 16, ... until the capacity
/// of the tier
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub(crate) struct Tier {
    /// distance between stored sequence numbers
    pub(crate) step: u64,
    /// the first sequence number for which a timestamp is stored
    pub(crate) first_seq: u64,
    /// the last sequence number stored in this tier
    pub(crate) last_seq: u64,
    /// how many elements can fit in this tier
    pub(crate) capacity: u32,
    /// the actual timestamps, the value at `timestamps[i]` is the
    /// timestamp for the sequence number `first_seq + i * step`
    #[serde(
        serialize_with = "serialize_timestamps",
        deserialize_with = "deserialize_timestamps"
    )]
    pub(crate) timestamps: Vec<i64>,
}

impl Tier {
    fn new(step: u64, capacity: u32) -> Self {
        Self {
            step,
            first_seq: 0,
            last_seq: 0,
            capacity,
            timestamps: Vec::with_capacity(capacity as usize),
        }
    }

    fn insert(&mut self, seq: u64, ts: i64) {
        assert!(seq >= self.last_seq);
        assert!(ts >= *self.timestamps.last().unwrap_or(&ts));

        // if we are at capacity then downsample the
        // sequence numbers and double the step size
        if self.timestamps.len() as u32 == self.capacity {
            let old = std::mem::take(&mut self.timestamps);
            let mut downsampled = Vec::with_capacity(self.capacity as usize);

            let first_seq = self.first_seq;
            for (i, &ts) in old.iter().enumerate() {
                // only keep the timestamps that are divisible by the new step size
                // to maintain the invariant that seq % step == 0
                let seq = first_seq + i as u64 * self.step;

                if seq % (self.step * 2) == 0 {
                    if downsampled.is_empty() {
                        self.first_seq = seq;
                    }
                    downsampled.push(ts);
                }
            }

            self.step *= 2;
            self.timestamps = downsampled;
        }

        if seq % self.step != 0 {
            return;
        }

        if self.timestamps.is_empty() {
            self.first_seq = seq;
        }

        self.last_seq = seq;
        self.timestamps.push(ts);
    }

    /// finds the timestamp associated with `seq`, rounding up or down
    /// as specified by the find option if the sequence number does not
    /// exist in this tier
    fn find_ts(&self, seq: u64, find_opt: FindOption) -> Option<i64> {
        let len = (self.timestamps.len()) as u64;
        let max = self.first_seq + self.step * len;

        if let Some(offset) = match find_opt {
            FindOption::RoundUp if seq <= self.last_seq => {
                if seq >= self.first_seq {
                    Some(cmp::max(0, (seq - self.first_seq) as i64))
                } else {
                    Some(0)
                }
            }
            FindOption::RoundDown if seq >= self.first_seq => {
                Some(cmp::min((seq - self.first_seq) as i64, max as i64))
            }
            _ => None,
        } {
            let mut idx = offset.div_euclid(self.step as i64);
            let remainder = offset.rem_euclid(self.step as i64);

            if remainder != 0 && find_opt == FindOption::RoundUp {
                idx += 1;
            }

            idx = idx.clamp(0, len as i64 - 1);
            return Some(self.timestamps[idx as usize]);
        }

        None
    }

    /// finds the sequence number associated with `ts`
    fn find_seq(&self, ts: i64, find_opt: FindOption) -> Option<u64> {
        if self.timestamps.is_empty() {
            return None;
        }

        match self.timestamps.binary_search(&ts) {
            Ok(idx) => Some(self.first_seq + (idx as u64) * self.step),
            Err(idx) => match find_opt {
                FindOption::RoundUp if idx < self.timestamps.len() => {
                    Some(self.first_seq + (idx as u64) * self.step)
                }
                FindOption::RoundDown if idx > 0 => {
                    Some(self.first_seq + ((idx - 1) as u64) * self.step)
                }
                _ => None,
            },
        }
    }
}

/// Custom serialization function for timestamps that uses the Gorilla
/// encoding scheme, which is appropriate for timeseries data
fn serialize_timestamps<S>(timestamps: &[i64], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let encoded = encode_timestamps_to_bytes(timestamps);
    serializer.serialize_bytes(&encoded)
}

pub(crate) fn encode_timestamps_to_bytes(timestamps: &[i64]) -> Vec<u8> {
    let mut w = BitWriter::new();
    w.push32(timestamps.len() as u32, 32);

    if !timestamps.is_empty() {
        // first encode the full initial timestamp
        w.push64(timestamps[0] as u64, 64);

        // for each subsequent encode the delta-of-deltas
        let mut prev_ts = timestamps[0];
        let mut prev_delta: i64 = 0;

        for &ts in &timestamps[1..] {
            let delta = ts - prev_ts;
            let dod = delta - prev_delta;

            // this is the gorilla encoding scheme, you can see
            // [this paper](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf)
            // on page 1820 for the full algorithm
            match dod {
                0 => w.push(false),
                -63..=63 => {
                    w.push32(0b10, 2);
                    w.push32((dod as u32) & 0x7F, 7);
                }
                -255..=255 => {
                    w.push32(0b110, 3);
                    w.push32((dod as u32) & 0x1FF, 9);
                }
                -2047..=2047 => {
                    w.push32(0b1110, 4);
                    w.push32((dod as u32) & 0xFFF, 12);
                }
                _ => {
                    w.push32(0b1111, 4);
                    w.push32(dod as u32, 32);
                }
            }

            prev_ts = ts;
            prev_delta = delta;
        }
    }

    w.finish()
}

/// Indicates how many bytes to read given the Gorilla prefix,
/// where the valid prefixes are 0, 10, 110, 1110, 1111
const GORILLA_PREFIX_BYTES: [u8; 5] = [0, 7, 9, 12, 32];

/// Custom serialization function for timestamps that uses the Gorilla
/// encoding scheme, which is appropriate for timeseries data
fn deserialize_timestamps<'de, D>(deserializer: D) -> Result<Vec<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    let buf: Vec<u8> = Vec::deserialize(deserializer)?;
    decode_timestamps_from_bytes(&buf).map_err(<D::Error as de::Error>::custom)
}

pub(crate) fn decode_timestamps_from_bytes(buf: &[u8]) -> Result<Vec<i64>, String> {
    let mut timestamps = Vec::new();
    let mut reader = BitReader::new(buf);

    let mut remaining = reader.read32(32).ok_or("Expected count first")?;

    if remaining > 0 {
        let first_ts_bits = reader
            .read64(64)
            .ok_or("Unexpected EOF: first ts should be 64 bit")?;
        let first = first_ts_bits as i64;

        remaining -= 1;
        timestamps.push(first);

        let mut prev_ts = first;
        let mut prev_delta: i32 = 0;

        while let Some(prefix) = reader.read_bit() {
            let dod = if !prefix {
                0
            } else {
                let mut count = 1;
                while count < 4 {
                    match reader.read_bit() {
                        Some(true) => count += 1,
                        Some(false) => break,
                        None => {
                            return Err("Unexpected EOF decoding Gorilla prefix".to_string());
                        }
                    }
                }

                let bits = GORILLA_PREFIX_BYTES[count];
                let raw = reader
                    .read32(bits)
                    .ok_or_else(|| format!("Unexpected EOF reading {}-bit delta-of-delta", bits))?;

                sign_extend(raw, bits)
            };

            let delta = prev_delta + dod;
            let ts = prev_ts + (delta as i64);
            timestamps.push(ts);

            prev_delta = delta;
            prev_ts = ts;

            // there may be some padding at the end of the [u8]
            // so we track how many we've read / still need to read
            remaining -= 1;

            if remaining == 0 {
                break;
            }
        }
    }

    Ok(timestamps)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    #[case::exact_match(8, FindOption::RoundDown, Some(200))]
    #[case::exact_match_round_up(8, FindOption::RoundUp, Some(200))]
    #[case::round_up_to_next(6, FindOption::RoundUp, Some(200))]
    #[case::round_down_to_previous(6, FindOption::RoundDown, Some(100))]
    #[case::before_first_seq(0, FindOption::RoundDown, None)]
    #[case::after_last_seq(16, FindOption::RoundUp, None)]
    #[case::at_first_seq(4, FindOption::RoundDown, Some(100))]
    #[case::at_last_seq(12, FindOption::RoundUp, Some(300))]
    fn test_find_ts(#[case] seq: u64, #[case] find_opt: FindOption, #[case] expected: Option<i64>) {
        // given
        let mut tier = Tier::new(4, 10);
        tier.insert(4, 100);
        tier.insert(8, 200);
        tier.insert(12, 300);

        // when
        let result = tier.find_ts(seq, find_opt);

        // then
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case::exact_match(200, FindOption::RoundDown, Some(8))]
    #[case::exact_match_round_up(200, FindOption::RoundUp, Some(8))]
    #[case::round_up_to_next(150, FindOption::RoundUp, Some(8))]
    #[case::round_down_to_previous(150, FindOption::RoundDown, Some(4))]
    #[case::before_first_ts(50, FindOption::RoundDown, None)]
    #[case::after_last_ts(350, FindOption::RoundUp, None)]
    #[case::at_first_ts(100, FindOption::RoundDown, Some(4))]
    #[case::at_last_ts(300, FindOption::RoundUp, Some(12))]
    #[case::empty_tier(1, FindOption::RoundDown, None)]
    fn test_find_seq(#[case] ts: i64, #[case] find_opt: FindOption, #[case] expected: Option<u64>) {
        // given
        let mut tier = Tier::new(4, 10);
        // (ts == 1 is the empty tier case)
        if ts != 1 || find_opt != FindOption::RoundDown {
            tier.insert(4, 100);
            tier.insert(8, 200);
            tier.insert(12, 300);
        }

        // when
        let result = tier.find_seq(ts, find_opt);

        // then
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case::empty_timestamps(vec![])]
    #[case::single_timestamp(vec![1234567890])]
    #[case::multiple_timestamps(vec![100, 200, 300, 400, 500])]
    #[case::zero_dod(vec![1000, 1000, 1000, 1000])] // Tests dod = 0 branch
    #[case::small_dod(vec![1000, 1064, 1128, 1192])] // Tests dod = 64 branch (max for 7-bit)
    #[case::medium_dod(vec![1000, 1256, 1512, 1768])] // Tests dod = 256 branch (max for 9-bit)
    #[case::large_dod(vec![1000, 3048, 5096, 7144])] // Tests dod = 2048 branch (max for 12-bit)
    #[case::huge_dod(vec![1000, 1000000, 2000000, 3000000])] // Tests 32-bit fallback branch
    #[case::negative_dod(vec![1000, 937, 874, 811])] // Tests negative dod values
    #[case::negative_timestamps(vec![-1000, -500, 0, 500, 1000])] // Tests negative timestamp values
    #[case::mixed_dod(vec![1000, 1000, 1032, 1128, 1512, 50000])] // Tests all branches with values within ranges
    #[case::mixed_dod_boundaries(vec![1000, 1000, 1028, 1255, 2047, 1000000])] // Tests all branches at boundaries
    #[case::large_numbers(vec![i64::MAX - 1000, i64::MAX - 500, i64::MAX])]
    fn test_serialize_deserialize_round_trip(#[case] timestamps: Vec<i64>) {
        // when
        let encoded = encode_timestamps_to_bytes(&timestamps);
        let decoded = decode_timestamps_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, timestamps);
    }

    #[test]
    fn test_serialize_deserialize_round_trip_proptest() {
        use proptest::collection::vec;
        use proptest::prelude::*;

        // Generate timestamp sequences for testing
        let timestamp_strategy = vec(any::<i32>(), 0..=20).prop_map(|deltas| {
            let base_timestamp = 1_600_000_000i64; // Unix timestamp around 2020
            let mut timestamps = Vec::new();
            let mut current_ts = base_timestamp;

            for delta in deltas {
                timestamps.push(current_ts);
                current_ts =
                    current_ts.saturating_add(delta.clamp(i32::MIN / 2, i32::MAX / 2) as i64);
            }

            timestamps
        });

        proptest!(|(timestamps in timestamp_strategy)| {
            let encoded = encode_timestamps_to_bytes(&timestamps);
            let decoded = decode_timestamps_from_bytes(&encoded).unwrap();
            assert_eq!(decoded, timestamps);
        });
    }

    #[test]
    fn should_downsample_when_capacity_exceeded() {
        // given
        let mut tier = Tier::new(2, 3);

        // Insert step-aligned sequences (step=2)
        tier.insert(0, 100);
        tier.insert(1, 150); // Should be ignored (not divisible by step=2)
        tier.insert(2, 200);
        tier.insert(3, 250); // Should be ignored (not divisible by step=2)
        tier.insert(4, 300);

        // when - this should trigger downsampling
        tier.insert(5, 400);

        // then
        assert_eq!(tier.step, 4); // step doubled
        assert_eq!(tier.timestamps, vec![100, 300]); // downsampled (only step-aligned sequences)
        assert_eq!(tier.first_seq, 0);
        assert_eq!(tier.last_seq, 4);
    }

    #[test]
    fn should_downsample_unaligned_sequences() {
        // given
        let mut tier = Tier::new(1, 4);

        // Insert step-aligned sequences (step=2)
        tier.insert(1, 100);
        tier.insert(2, 150);
        tier.insert(3, 200);
        tier.insert(4, 250);

        // when - this should trigger downsampling
        tier.insert(5, 300);

        // then
        assert_eq!(tier.step, 2); // step doubled
        assert_eq!(tier.timestamps, vec![150, 250]); // downsampled (only step-aligned sequences)
        assert_eq!(tier.first_seq, 2);
        assert_eq!(tier.last_seq, 4);
    }
}
