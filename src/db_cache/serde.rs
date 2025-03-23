//! Serialize/Deserialize impls for CachedKey and CachedEntry to support DbCache impls that
//! can store cache items on-disk. Internally, the Serialize/Deserialize impls work by
//! converting these types to representations that derive Serialize/Deserialize. The purpose
//! of the indirection is to decouple the serialized format from the in-memory representation
//! used by the rest of the codebase.

use crate::block::Block;
use crate::db_cache::{CachedEntry, CachedItem, CachedKey};
use crate::db_state::SsTableId;
use crate::filter::BloomFilter;
use crate::flatbuffer_types::SsTableIndexOwned;
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
    V1(SerializedSsTableId, u64 /*#[default] String*/),
}

impl From<SerializedCachedKey> for CachedKey {
    fn from(value: SerializedCachedKey) -> Self {
        match value {
            SerializedCachedKey::V1(sst_id, block_id) => CachedKey(sst_id.into(), block_id),
        }
    }
}

impl From<CachedKey> for SerializedCachedKey {
    fn from(value: CachedKey) -> Self {
        SerializedCachedKey::V1(value.0.into(), value.1)
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
}

impl SerializedCachedEntryV1 {
    fn into_cached_entry(self) -> Result<CachedEntry, String> {
        let item = match self {
            SerializedCachedEntryV1::Block(encoded) => {
                let block = Block::decode(encoded);
                CachedItem::Block(Arc::new(block))
            }
            SerializedCachedEntryV1::SsTableIndex(encoded) => {
                let index = SsTableIndexOwned::new(encoded).map_err(|e| e.to_string())?;
                CachedItem::SsTableIndex(Arc::new(index))
            }
            SerializedCachedEntryV1::BloomFilter(encoded) => {
                let filter = BloomFilter::decode(encoded.as_ref());
                CachedItem::BloomFilter(Arc::new(filter))
            }
        };
        Ok(CachedEntry { item })
    }
}

#[derive(Serialize, Deserialize)]
enum SerializedCachedEntry {
    V1(SerializedCachedEntryV1),
}

impl SerializedCachedEntry {
    fn into_cached_entry(self) -> Result<CachedEntry, String> {
        match self {
            SerializedCachedEntry::V1(entry) => entry.into_cached_entry(),
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
            CachedItem::BloomFilter(filter) => {
                let encoded = filter.encode();
                SerializedCachedEntry::V1(SerializedCachedEntryV1::BloomFilter(encoded))
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
            .map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use crate::block::BlockBuilder;
    use crate::block_iterator::BlockIterator;
    use crate::db_cache::{CachedEntry, CachedItem, CachedKey};
    use crate::db_state::SsTableId;
    use crate::filter::BloomFilterBuilder;
    use crate::flatbuffer_types::{
        BlockMeta, BlockMetaArgs, SsTableIndex, SsTableIndexArgs, SsTableIndexOwned,
    };
    use crate::iter::IterationOrder;
    use crate::test_utils::assert_iterator;
    use crate::types::RowEntry;
    use bytes::Bytes;
    use std::sync::Arc;
    use ulid::Ulid;

    #[test]
    fn test_should_serialize_deserialize_compacted_sst_key() {
        let key = CachedKey(SsTableId::Compacted(Ulid::from((123, 456))), 99);

        let encoded = bincode::serialize(&key).unwrap();
        let decoded: CachedKey = bincode::deserialize(&encoded).unwrap();

        assert_eq!(decoded, key);
    }

    #[test]
    fn test_should_serialize_deserialize_wal_sst_key() {
        let key = CachedKey(SsTableId::Wal(123), 99);

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
        let mut builder = BlockBuilder::new(4096);
        for row in rows.iter() {
            assert!(builder.add(row.clone()));
        }
        let block = Arc::new(builder.build().unwrap());
        let entry = CachedEntry {
            item: CachedItem::Block(block.clone()),
        };

        let encoded = bincode::serialize(&entry).unwrap();
        let decoded: CachedEntry = bincode::deserialize(&encoded).unwrap();

        let decoded_block = decoded.block().unwrap();
        assert!(block.as_ref() == decoded_block.as_ref());
        let mut iter = BlockIterator::new(decoded_block, IterationOrder::Ascending);
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
        let keys = vec![b"foo", b"bar", b"baz"];
        let mut builder = BloomFilterBuilder::new(10);
        for k in keys {
            builder.add_key(k);
        }
        let filter = Arc::new(builder.build());
        let entry = CachedEntry {
            item: CachedItem::BloomFilter(filter.clone()),
        };

        let encoded = bincode::serialize(&entry).unwrap();
        let decoded: CachedEntry = bincode::deserialize(&encoded).unwrap();

        let decoded_filter = decoded.bloom_filter().unwrap();
        assert!(filter.as_ref() == decoded_filter.as_ref());
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
