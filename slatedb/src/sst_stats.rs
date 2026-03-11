use bytes::Bytes;
use flatbuffers::FlatBufferBuilder;

use crate::error::SlateDBError;
use crate::flatbuffer_types::{FbBlockStats, FbBlockStatsArgs, FbSstStats, FbSstStatsArgs};

/// Per-block statistics.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct BlockStats {
    pub(crate) num_puts: u16,
    pub(crate) num_deletes: u16,
    pub(crate) num_merges: u16,
}

/// Per-SST statistics collected during SST building.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SstStats {
    pub(crate) num_puts: u64,
    pub(crate) num_deletes: u64,
    pub(crate) num_merges: u64,
    pub(crate) raw_key_size: u64,
    pub(crate) raw_val_size: u64,
    pub(crate) block_stats: Vec<BlockStats>,
}

impl SstStats {
    /// Returns the total number of rows (puts + deletes + merges).
    pub(crate) fn num_rows(&self) -> u64 {
        self.num_puts + self.num_deletes + self.num_merges
    }

    /// Returns the in-memory size in bytes (5 u64 fields).
    pub(crate) fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    /// Returns a clone (no heap allocations to clamp).
    pub(crate) fn clamp_allocated_size(&self) -> Self {
        self.clone()
    }

    /// Encode stats to bytes via FlatBuffers.
    pub(crate) fn encode(&self) -> Bytes {
        let mut builder = FlatBufferBuilder::new();
        // Create nested block_stats vector before the root table.
        let block_stats_vec: Vec<_> = self
            .block_stats
            .iter()
            .map(|bs| {
                FbBlockStats::create(
                    &mut builder,
                    &FbBlockStatsArgs {
                        num_puts: bs.num_puts,
                        num_deletes: bs.num_deletes,
                        num_merges: bs.num_merges,
                    },
                )
            })
            .collect();
        let block_stats = if block_stats_vec.is_empty() {
            None
        } else {
            Some(builder.create_vector(&block_stats_vec))
        };
        let stats = FbSstStats::create(
            &mut builder,
            &FbSstStatsArgs {
                num_puts: self.num_puts,
                num_deletes: self.num_deletes,
                num_merges: self.num_merges,
                raw_key_size: self.raw_key_size,
                raw_val_size: self.raw_val_size,
                block_stats,
            },
        );
        builder.finish(stats, None);
        Bytes::from(builder.finished_data().to_vec())
    }

    // Used by SsTableFormat::decode_stats (RFC 0020 Phase 2)
    #[allow(dead_code)]
    pub(crate) fn decode(data: Bytes) -> Result<Self, SlateDBError> {
        let fb_stats = flatbuffers::root::<FbSstStats>(&data)?;
        let block_stats = fb_stats
            .block_stats()
            .map(|v| {
                v.iter()
                    .map(|bs| BlockStats {
                        num_puts: bs.num_puts(),
                        num_deletes: bs.num_deletes(),
                        num_merges: bs.num_merges(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(SstStats {
            num_puts: fb_stats.num_puts(),
            num_deletes: fb_stats.num_deletes(),
            num_merges: fb_stats.num_merges(),
            raw_key_size: fb_stats.raw_key_size(),
            raw_val_size: fb_stats.raw_val_size(),
            block_stats,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_round_trip() {
        let stats = SstStats {
            num_puts: 100,
            num_deletes: 20,
            num_merges: 5,
            raw_key_size: 4096,
            raw_val_size: 8192,
            block_stats: vec![],
        };
        let encoded = stats.encode();
        let decoded = SstStats::decode(encoded).unwrap();
        assert_eq!(stats, decoded);
    }

    #[test]
    fn test_default_is_all_zeros() {
        let stats = SstStats::default();
        assert_eq!(stats.num_puts, 0);
        assert_eq!(stats.num_deletes, 0);
        assert_eq!(stats.num_merges, 0);
        assert_eq!(stats.raw_key_size, 0);
        assert_eq!(stats.raw_val_size, 0);
        assert!(stats.block_stats.is_empty());

        let encoded = stats.encode();
        let decoded = SstStats::decode(encoded).unwrap();
        assert_eq!(stats, decoded);
    }

    #[test]
    fn test_encode_decode_with_block_stats() {
        let stats = SstStats {
            num_puts: 10,
            num_deletes: 2,
            num_merges: 1,
            raw_key_size: 100,
            raw_val_size: 200,
            block_stats: vec![
                BlockStats {
                    num_puts: 7,
                    num_deletes: 1,
                    num_merges: 0,
                },
                BlockStats {
                    num_puts: 3,
                    num_deletes: 1,
                    num_merges: 1,
                },
            ],
        };
        let encoded = stats.encode();
        let decoded = SstStats::decode(encoded).unwrap();
        assert_eq!(stats, decoded);
    }

    #[test]
    fn test_backward_compat_missing_block_stats() {
        // Encode an old-style SstStats without block_stats (empty vec),
        // then verify decode returns block_stats: vec![].
        let old_stats = SstStats {
            num_puts: 5,
            num_deletes: 0,
            num_merges: 0,
            raw_key_size: 50,
            raw_val_size: 100,
            block_stats: vec![],
        };
        let encoded = old_stats.encode();
        let decoded = SstStats::decode(encoded).unwrap();
        assert!(decoded.block_stats.is_empty());
        assert_eq!(decoded.num_puts, 5);
    }
}
