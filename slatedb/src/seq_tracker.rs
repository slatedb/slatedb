use std::cmp;

use chrono::{DateTime, Utc};
use serde::de::{self};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::utils::{sign_extend, BitReader, BitWriter};

#[derive(PartialEq)]
pub(crate) enum FindOption {
    RoundUp,
    RoundDown,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub(crate) struct TieredSequenceTracker {
    /// The data, tiered by step size where the step size
    /// indicates the granularity at which the sequence
    /// numbers are tracked. The tiers are ordered by the
    /// granularity (i.e. `tiers[0]` has the lowest step)
    tiers: Vec<Tier>,
}

impl TieredSequenceTracker {
    pub(crate) fn new(num_tiers: usize, capacity: usize) -> Self {
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

    pub(crate) fn insert(&mut self, seq: u64, ts: DateTime<Utc>) {
        self.tiers[0].insert(seq, ts.timestamp());
    }

    pub(crate) fn find_ts(&self, seq: u64, find_opt: FindOption) -> Option<DateTime<Utc>> {
        if let Some(ts) = self.tiers[0].find_ts(seq, find_opt) {
            DateTime::from_timestamp(ts, 0)
        } else {
            None
        }
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
struct Tier {
    /// distance between stored sequence numbers
    step: u64,
    /// the first sequence number for which a timestamp is stored
    first_seq: u64,
    /// the last sequence number stored in this tier
    last_seq: u64,
    /// how many elements can fit in this tier
    #[serde(skip)]
    capacity: usize,
    /// the actual timestamps, the value at `timestamps[i]` is the
    /// timestamp for the sequence number `first_seq + i * step`
    #[serde(
        serialize_with = "serialize_timestamps",
        deserialize_with = "deserialize_timestamps"
    )]
    timestamps: Vec<i64>,
}

impl Tier {
    fn new(step: u64, capacity: usize) -> Self {
        Self {
            step,
            first_seq: 0,
            last_seq: 0,
            capacity,
            timestamps: Vec::with_capacity(capacity),
        }
    }

    fn insert(&mut self, seq: u64, ts: i64) {
        assert!(seq >= self.last_seq);
        assert!(ts >= *self.timestamps.last().unwrap_or(&ts));

        if !(seq % self.step == 0) {
            return;
        }

        if (self.timestamps.is_empty()) {
            self.first_seq = seq;
        }

        self.last_seq = seq;
        self.timestamps.push(ts);

        // if we've exceeded the capacity then downsample the
        // sequence numbers and double the step size
        if self.timestamps.len() > self.capacity {
            let old = std::mem::take(&mut self.timestamps);
            let mut downsampled = Vec::with_capacity((old.len() + 1) / 2);
            for (i, &ts) in old.iter().enumerate() {
                if i % 2 == 0 {
                    downsampled.push(ts);
                }
            }
            self.timestamps = downsampled;
            self.step *= 2;
        }
    }

    /// finds the timestamp associated with `seq`, rounding up or down
    /// as specified by the find option if the sequence number does not
    /// exist in this tier
    fn find_ts(&self, seq: u64, find_opt: FindOption) -> Option<i64> {
        let len = (self.timestamps.len()) as u64;
        let max = self.first_seq + self.step * len;
        let diff = seq - self.first_seq;
        if let Some(offset) = match find_opt {
            FindOption::RoundUp if seq <= self.last_seq => Some(cmp::max(0, diff)),
            FindOption::RoundDown if seq >= self.first_seq => Some(cmp::min(diff, max)),
            _ => None,
        } {
            let mut idx = offset.div_euclid(self.step);
            let remainder = offset.rem_euclid(self.step);

            if remainder != 0 && find_opt == FindOption::RoundUp {
                idx += 1;
            }

            idx = idx.clamp(0, len - 1);
            return Some(self.timestamps[idx as usize]);
        }

        None
    }
}

/// Custom serialization function for timestamps that uses the Gorilla
/// encoding scheme, which is appropriate for timeseries data
fn serialize_timestamps<S>(timestamps: &Vec<i64>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let encoded = encode_timestamps_to_bytes(timestamps);
    serializer.serialize_bytes(&encoded)
}

fn encode_timestamps_to_bytes(timestamps: &[i64]) -> Vec<u8> {
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
                -63..=64 => {
                    w.push32(0b10, 2);
                    w.push32((dod as u32) & 0x7F, 7);
                }
                -255..=256 => {
                    w.push32(0b110, 3);
                    w.push32((dod as u32) & 0x1FF, 9);
                }
                -2047..=2048 => {
                    w.push32(0b1110, 4);
                    w.push32((dod as u32) & 0xFFF, 12);
                }
                _ => {
                    w.push32(0b1111, 4);
                    w.push32((dod as u32) & 0xFFFFFFFF, 32);
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
    decode_timestamps_from_bytes(&buf).map_err(|e| <D::Error as de::Error>::custom(e))
}

fn decode_timestamps_from_bytes(buf: &[u8]) -> Result<Vec<i64>, String> {
    let mut timestamps = Vec::new();
    let mut reader = BitReader::new(buf);

    let mut remaining = reader.read32(32).ok_or("Expected count first")? as u32;

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

    #[test]
    fn should_ignore_insert_for_non_step_aligned_sequence_number() {
        // given
        let mut tier = Tier::new(4, 10);

        // when
        tier.insert(5, 100);

        // then
        assert!(tier.timestamps.is_empty());
        assert_eq!(tier.last_seq, 0);
    }

    #[test]
    fn should_find_exact_timestamp_for_sequence_number() {
        // given
        let mut tier = Tier::new(4, 10);
        tier.insert(4, 100);
        tier.insert(8, 200);
        tier.insert(12, 300);

        // when
        let result_down = tier.find_ts(8, FindOption::RoundDown);
        let result_up = tier.find_ts(8, FindOption::RoundUp);

        // then
        assert_eq!(result_down, Some(200));
        assert_eq!(result_up, Some(200));
    }

    #[test]
    fn should_round_up_to_next_timestamp() {
        // given
        let mut tier = Tier::new(4, 10);
        tier.insert(4, 100);
        tier.insert(8, 200);

        // when
        let result = tier.find_ts(6, FindOption::RoundUp);

        // then
        assert_eq!(result, Some(200));
    }

    #[test]
    fn should_round_down_to_previous_timestamp() {
        // given
        let mut tier = Tier::new(4, 10);
        tier.insert(4, 100);
        tier.insert(8, 200);

        // when
        let result = tier.find_ts(6, FindOption::RoundDown);

        // then
        assert_eq!(result, Some(100));
    }

    #[test]
    fn should_downsample_when_capacity_exceeded() {
        // given
        let mut tier = Tier::new(2, 3);
        tier.insert(2, 100);
        tier.insert(4, 200);
        tier.insert(6, 300);

        // when
        tier.insert(8, 400);

        // then
        assert_eq!(tier.step, 4); // step doubled
        assert_eq!(tier.timestamps, vec![100, 300]); // downsampled
        assert_eq!(tier.last_seq, 8);
    }

    #[test]
    fn round_trip_empty_timestamps() {
        let timestamps: Vec<i64> = vec![];
        let encoded = encode_timestamps_to_bytes(&timestamps);
        let decoded = decode_timestamps_from_bytes(&encoded).unwrap();
        assert_eq!(decoded, timestamps);
    }

    #[test]
    fn round_trip_single_timestamp() {
        let timestamps = vec![1234567890];
        let encoded = encode_timestamps_to_bytes(&timestamps);
        let decoded = decode_timestamps_from_bytes(&encoded).unwrap();
        assert_eq!(decoded, timestamps);
    }

    #[test]
    fn round_trip_timestamps_with_various_deltas() {
        // Given timestamps that will exercise each branch of the Gorilla encoding:
        // Using real Unix timestamps (seconds since epoch) that don't fit in i32
        // to test the encoding dods using i32s
        let timestamps = vec![2145916800, 2145916900, 2145916900, 2145916950, 2145918950, 2146018950];
        let encoded = encode_timestamps_to_bytes(&timestamps);

        // When:
        let decoded = decode_timestamps_from_bytes(&encoded).unwrap();

        // Then:
        assert_eq!(decoded, timestamps);

        // a naive encoding of 8 bytes per timestamp would be 48 bytes
        // but this should fit in 23
        assert_eq!(encoded.len(), 23);
    }
}
