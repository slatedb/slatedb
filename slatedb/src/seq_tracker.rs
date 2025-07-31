use std::cmp;

use serde::{Deserialize, Serialize};

#[derive(PartialEq)]
pub(crate) enum FindOption {
    RoundUp,
    RoundDown,
}

#[derive(Serialize, Deserialize, Debug)]
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

    pub(crate) fn insert(&mut self, seq: u64, ts: i64) {
        self.tiers[0].insert(seq, ts);
    }

    pub(crate) fn find_ts(&self, seq: u64, find_opt: FindOption) -> Option<i64> {
        self.tiers[0].find_ts(seq, find_opt)
    }
}

/// A tier contains a mapping from sequence numbers to
/// timestamps at a given "resolution", defined by a
/// step parameter which is how spaced apart the timestamps
/// are. A tier with step `4` and a start sequence number
/// of 10, for example, would contain the timestamps for
/// sequence numbers 10, 14, 16, ... until the capacity
/// of the tier
#[derive(Serialize, Deserialize, Debug)]
struct Tier {
    /// distance between stored sequence numbers
    step: u64,
    /// the first sequence number for which a timestamp is stored
    first_seq: u64,
    /// the last sequence number stored in this tier, used as a
    /// convenience to round across tiers
    #[serde(skip)]
    last_seq: u64,
    #[serde(skip)]
    /// how many elements can fit in this tier
    capacity: usize,
    /// the actual timestamps, the value at `timestamps[i]` is the
    /// timestamp for the sequence number `first_seq + i * step`
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
            return
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
}
