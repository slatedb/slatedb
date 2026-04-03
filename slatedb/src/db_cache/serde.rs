//! Serialize/Deserialize impls for CachedKey and CachedEntry to support DbCache impls that
//! can store cache items on-disk. Internally, the Serialize/Deserialize impls work by
//! converting these types to representations that derive Serialize/Deserialize. The purpose
//! of the indirection is to decouple the serialized format from the in-memory representation
//! used by the rest of the codebase.

use crate::db_cache::{CachedEntry, CachedItem, CachedKey};
use crate::db_state::SsTableId;
use crate::error::SlateDBError;
use crate::filter_policy::{BloomFilterPolicy, NamedFilter};
use crate::flatbuffer_types::SsTableIndexOwned;
use crate::format::block::Block;
use crate::sst_stats::SstStats;
use bytes::Bytes;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;
use ulid::Ulid;

#[derive(Serialize, Deserialize)]
enum SerializedSsTableId {
    Wal(u64),
    Compacted(Ulid),
}

impl From<SerializedSsTableId> for SsTableId {
    fn from(value: SerializedSsTableId) -> Self {
        match value {
            SerializedSsTableId::Wal(id) => SsTableId::Wal(id),
            SerializedSsTableId::Compacted(id) => SsTableId::Compacted(id),
        }
    }
}

impl From<SsTableId> for SerializedSsTableId {
    fn from(value: SsTableId) -> Self {
        match value {
            SsTableId::Wal(id) => SerializedSsTableId::Wal(id),
            SsTableId::Compacted(id) => SerializedSsTableId::Compacted(id),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum SerializedCachedKey {
    V1(SerializedSsTableId, u64),
    V2(u64, SerializedSsTableId, u64),
}

impl From<SerializedCachedKey> for CachedKey {
    fn from(value: SerializedCachedKey) -> Self {
        match value {
            SerializedCachedKey::V1(sst_id, block_id) => CachedKey {
                scope_id: 0,
                sst_id: sst_id.into(),
                block_id,
            },
            SerializedCachedKey::V2(scope_id, sst_id, block_id) => CachedKey {
                scope_id,
                sst_id: sst_id.into(),
                block_id,
            },
        }
    }
}

impl From<CachedKey> for SerializedCachedKey {
    fn from(value: CachedKey) -> Self {
        SerializedCachedKey::V2(value.scope_id, value.sst_id.into(), value.block_id)
    }
}

impl Serialize for CachedKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let serialized_key: SerializedCachedKey = self.clone().into();
        serialized_key.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CachedKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let serialized_key = SerializedCachedKey::deserialize(deserializer)?;
        Ok(serialized_key.into())
    }
}

#[derive(Serialize, Deserialize)]
enum SerializedCachedEntryV1 {
    Block(Bytes),
    SsTableIndex(Bytes),
    BloomFilter(Bytes),
    SstStats(Bytes),
}

impl SerializedCachedEntryV1 {
    fn into_cached_entry(self) -> Result<CachedEntry, SlateDBError> {
        let item = match self {
            SerializedCachedEntryV1::Block(encoded) => {
                let block = Block::decode(encoded);
                CachedItem::Block(Arc::new(block))
            }
            SerializedCachedEntryV1::SsTableIndex(encoded) => {
                let index = SsTableIndexOwned::new(encoded)?;
                CachedItem::SsTableIndex(Arc::new(index))
            }
            SerializedCachedEntryV1::BloomFilter(encoded) => {
                // Produce a raw NamedFilter; decoding is deferred until TableStore
                // resolves it against the configured filter policies.
                CachedItem::Filters(vec![NamedFilter::raw(
                    BloomFilterPolicy::NAME.to_string(),
                    encoded,
                )])
            }
            SerializedCachedEntryV1::SstStats(encoded) => {
                let stats = SstStats::decode(encoded)?;
                CachedItem::SstStats(Arc::new(stats))
            }
        };
        Ok(CachedEntry { item })
    }
}

/// Serialized representation of a composite filter block.
/// Format: list of (name, encoded_data) pairs.
#[derive(Serialize, Deserialize)]
struct SerializedCompositeFilters(Vec<(String, Bytes)>);

#[derive(Serialize, Deserialize)]
enum SerializedCachedEntryV2 {
    Filters(SerializedCompositeFilters),
}

#[derive(Serialize, Deserialize)]
enum SerializedCachedEntry {
    V1(SerializedCachedEntryV1),
    V2(SerializedCachedEntryV2),
}

impl SerializedCachedEntry {
    fn into_cached_entry(self) -> Result<CachedEntry, SlateDBError> {
        match self {
            SerializedCachedEntry::V1(entry) => entry.into_cached_entry(),
            SerializedCachedEntry::V2(entry) => match entry {
                SerializedCachedEntryV2::Filters(composite) => {
                    // Produce raw NamedFilters; decoding is deferred until TableStore
                    // resolves them against the configured filter policies.
                    let filters = composite
                        .0
                        .into_iter()
                        .map(|(name, data)| NamedFilter::raw(name, data))
                        .collect();
                    Ok(CachedEntry {
                        item: CachedItem::Filters(filters),
                    })
                }
            },
        }
    }
}

impl From<CachedEntry> for SerializedCachedEntry {
    fn from(value: CachedEntry) -> Self {
        match value.item {
            CachedItem::Block(block) => {
                let encoded = block.encode();
                SerializedCachedEntry::V1(SerializedCachedEntryV1::Block(encoded))
            }
            CachedItem::SsTableIndex(index) => {
                let encoded = index.data();
                SerializedCachedEntry::V1(SerializedCachedEntryV1::SsTableIndex(encoded))
            }
            CachedItem::Filters(filters) => {
                let pairs = filters
                    .iter()
                    .map(|nf| (nf.name().to_string(), nf.encode_data()))
                    .collect();
                SerializedCachedEntry::V2(SerializedCachedEntryV2::Filters(
                    SerializedCompositeFilters(pairs),
                ))
            }
            CachedItem::SstStats(stats) => {
                let encoded = stats.encode();
                SerializedCachedEntry::V1(SerializedCachedEntryV1::SstStats(encoded))
            }
        }
    }
}

impl Serialize for CachedEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let serialized_entry: SerializedCachedEntry = self.clone().into();
        serialized_entry.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CachedEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let serialized_entry = SerializedCachedEntry::deserialize(deserializer)?;
        serialized_entry
            .into_cached_entry()
            // the error returned by deserialize must be lowercase and not end in a .
            .map_err(|e| D::Error::custom(format!("slatedb error ({})", e).to_lowercase()))
    }
}

#[cfg(test)]
mod tests {
    use crate::block_iterator::BlockIteratorLatest;
    use crate::db_cache::{CachedEntry, CachedItem, CachedKey};
    use crate::db_state::SsTableId;
    use crate::filter_policy::{BloomFilterPolicy, FilterPolicy, NamedFilter};
    use crate::flatbuffer_types::{
        BlockMeta, BlockMetaArgs, SsTableIndex, SsTableIndexArgs, SsTableIndexOwned,
    };
    use crate::format::sst::BlockBuilder;
    use crate::iter::IterationOrder;
    use crate::sst_stats::SstStats;
    use crate::test_utils::assert_iterator;
    use crate::types::RowEntry;
    use bytes::Bytes;
    use std::sync::Arc;
    use ulid::Ulid;

    #[test]
    fn test_should_serialize_deserialize_compacted_sst_key() {
        let key = CachedKey {
            scope_id: 0,
            sst_id: SsTableId::Compacted(Ulid::from((123, 456))),
            block_id: 99,
        };

        let encoded = bincode::serialize(&key).unwrap();
        let decoded: CachedKey = bincode::deserialize(&encoded).unwrap();

        assert_eq!(decoded, key);
    }

    #[test]
    fn test_should_serialize_deserialize_wal_sst_key() {
        let key = CachedKey {
            scope_id: 5,
            sst_id: SsTableId::Wal(123),
            block_id: 99,
        };

        let encoded = bincode::serialize(&key).unwrap();
        let decoded: CachedKey = bincode::deserialize(&encoded).unwrap();

        assert_eq!(decoded, key);
    }

    #[tokio::test]
    async fn test_should_serialize_deserialize_block() {
        let rows = vec![
            RowEntry::new_value(b"foo", b"bar", 0),
            RowEntry::new_merge(b"biz", b"baz", 1),
            RowEntry::new_tombstone(b"bla", 2),
        ];
        let mut builder = BlockBuilder::new_latest(4096);
        for row in rows.iter() {
            assert!(builder.add(row.clone()).unwrap());
        }
        let block = Arc::new(builder.build().unwrap());
        let entry = CachedEntry {
            item: CachedItem::Block(block.clone()),
        };

        let encoded = bincode::serialize(&entry).unwrap();
        let decoded: CachedEntry = bincode::deserialize(&encoded).unwrap();

        let decoded_block = decoded.block().unwrap();
        assert!(block.as_ref() == decoded_block.as_ref());
        let mut iter = BlockIteratorLatest::new(decoded_block, IterationOrder::Ascending);
        assert_iterator(&mut iter, rows).await;
    }

    #[test]
    fn test_should_serialize_deserialize_index() {
        let first_keys = vec![b"foo".as_slice(), b"bar".as_slice(), b"baz".as_slice()];
        let index = Arc::new(build_index_with_first_keys(&first_keys));
        let entry = CachedEntry {
            item: CachedItem::SsTableIndex(index.clone()),
        };

        let encoded = bincode::serialize(&entry).unwrap();
        let decoded: CachedEntry = bincode::deserialize(&encoded).unwrap();

        let decoded_index = decoded.sst_index().unwrap();
        assert!(index.as_ref() == decoded_index.as_ref());
    }

    #[test]
    fn test_should_serialize_deserialize_filter() {
        let policy = BloomFilterPolicy::new(10);
        let mut builder = policy.builder();
        for k in [b"foo", b"bar", b"baz"] {
            builder.add_entry(&crate::types::RowEntry::new(
                bytes::Bytes::copy_from_slice(k),
                crate::types::ValueDeletable::Value(bytes::Bytes::new()),
                0,
                None,
                None,
            ));
        }
        let filter = builder.build();
        let named = NamedFilter::new(BloomFilterPolicy::NAME.to_string(), filter.clone());
        let entry = CachedEntry {
            item: CachedItem::Filters(vec![named]),
        };

        let encoded = bincode::serialize(&entry).unwrap();
        let decoded: CachedEntry = bincode::deserialize(&encoded).unwrap();

        let decoded_filters = decoded.filters().unwrap();
        assert_eq!(1, decoded_filters.len());
        let decoded_nf = &decoded_filters[0];
        assert_eq!(decoded_nf.name(), "slatedb.BloomFilter");
        // Deserialized from disk cache, so it should be raw (not yet decoded)
        assert!(!decoded_nf.is_decoded());
        // Verify encode_data produces the same bytes as the original
        let mut original_bytes = Vec::new();
        filter.encode(&mut original_bytes);
        assert_eq!(Bytes::from(original_bytes), decoded_nf.encode_data());
    }

    #[test]
    fn test_should_serialize_deserialize_stats() {
        let stats = Arc::new(SstStats {
            num_puts: 100,
            num_deletes: 10,
            num_merges: 5,
            raw_key_size: 2048,
            raw_val_size: 8192,
            block_stats: vec![],
        });
        let entry = CachedEntry {
            item: CachedItem::SstStats(stats.clone()),
        };

        let encoded = bincode::serialize(&entry).unwrap();
        let decoded: CachedEntry = bincode::deserialize(&encoded).unwrap();

        let decoded_stats = decoded.sst_stats().unwrap();
        assert_eq!(stats.as_ref(), decoded_stats.as_ref());
    }

    fn build_index_with_first_keys(first_keys: &[&[u8]]) -> SsTableIndexOwned {
        let mut index_builder = flatbuffers::FlatBufferBuilder::new();
        let mut block_metas = Vec::new();
        for fk in first_keys {
            let fk = index_builder.create_vector(fk);
            let block_meta = BlockMeta::create(
                &mut index_builder,
                &BlockMetaArgs {
                    first_key: Some(fk),
                    offset: 0u64,
                },
            );
            block_metas.push(block_meta);
        }
        let block_metas = index_builder.create_vector(&block_metas);
        let index_wip = SsTableIndex::create(
            &mut index_builder,
            &SsTableIndexArgs {
                block_meta: Some(block_metas),
            },
        );
        index_builder.finish(index_wip, None);
        let index_bytes = Bytes::copy_from_slice(index_builder.finished_data());
        SsTableIndexOwned::new(index_bytes).unwrap()
    }
}
