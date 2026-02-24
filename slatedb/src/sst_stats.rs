use bytes::Bytes;
use flatbuffers::FlatBufferBuilder;

use crate::error::SlateDBError;
use crate::flatbuffer_types::{FbSstStats, FbSstStatsArgs};

/// Per-SST statistics collected during SST building.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SstStats {
    pub(crate) num_puts: u64,
    pub(crate) num_deletes: u64,
    pub(crate) num_merges: u64,
    pub(crate) raw_key_size: u64,
    pub(crate) raw_val_size: u64,
}

impl SstStats {
    /// Encode stats to bytes via FlatBuffers.
    pub(crate) fn encode(&self) -> Bytes {
        let mut builder = FlatBufferBuilder::new();
        let stats = FbSstStats::create(
            &mut builder,
            &FbSstStatsArgs {
                num_puts: self.num_puts,
                num_deletes: self.num_deletes,
                num_merges: self.num_merges,
                raw_key_size: self.raw_key_size,
                raw_val_size: self.raw_val_size,
            },
        );
        builder.finish(stats, None);
        Bytes::from(builder.finished_data().to_vec())
    }

    // Used by SsTableFormat::decode_stats (RFC 0020 Phase 2)
    #[allow(dead_code)]
    pub(crate) fn decode(data: Bytes) -> Result<Self, SlateDBError> {
        let fb_stats = flatbuffers::root::<FbSstStats>(&data)?;
        Ok(SstStats {
            num_puts: fb_stats.num_puts(),
            num_deletes: fb_stats.num_deletes(),
            num_merges: fb_stats.num_merges(),
            raw_key_size: fb_stats.raw_key_size(),
            raw_val_size: fb_stats.raw_val_size(),
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

        let encoded = stats.encode();
        let decoded = SstStats::decode(encoded).unwrap();
        assert_eq!(stats, decoded);
    }
}
