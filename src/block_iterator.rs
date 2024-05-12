use crate::{
    block::{Block, TOMBSTONE},
    error::SlateDBError,
    iter::KeyValueIterator,
    types::{KVEntry, KVValue},
};
use bytes::{Buf, Bytes};

pub trait BlockLike {
    fn data(&self) -> &Bytes;
    fn offsets(&self) -> &[u16];
}

impl BlockLike for Block {
    fn data(&self) -> &Bytes {
        &self.data
    }

    fn offsets(&self) -> &[u16] {
        &self.offsets
    }
}

impl BlockLike for &Block {
    fn data(&self) -> &Bytes {
        &self.data
    }

    fn offsets(&self) -> &[u16] {
        &self.offsets
    }
}

pub struct BlockIterator<B: BlockLike> {
    block: B,
    off_off: usize,
}

impl<B: BlockLike> KeyValueIterator for BlockIterator<B> {
    async fn next_entry(&mut self) -> Result<Option<KVEntry>, SlateDBError> {
        let Some(key_value) = self.load_at_current_off() else {
            return Ok(None);
        };
        self.advance();
        Ok(Some(key_value))
    }
}

impl<B: BlockLike> BlockIterator<B> {
    pub fn from_first_key(block: B) -> BlockIterator<B> {
        BlockIterator { block, off_off: 0 }
    }

    /// Construct a BlockIterator that starts at the given key, or at the first
    /// key greater than the given key if the exact key given is not in the block.
    #[allow(dead_code)] // will be used in #8
    pub fn from_key(block: B, key: &[u8]) -> BlockIterator<B> {
        let idx = block.offsets().partition_point(|offset| {
            let mut cursor = &block.data()[*offset as usize..];
            let key_len = cursor.get_u16() as usize;
            let cursor_key = &cursor[..key_len];
            cursor_key < key
        });

        BlockIterator {
            block,
            off_off: idx,
        }
    }

    fn advance(&mut self) {
        self.off_off += 1;
    }

    fn load_at_current_off(&self) -> Option<KVEntry> {
        if self.off_off >= self.block.offsets().len() {
            return None;
        }
        let off = self.block.offsets()[self.off_off];
        let off_usz = off as usize;
        // TODO: bounds checks to avoid panics? (paulgb)
        let mut cursor = self.block.data().slice(off_usz..);
        let key_len = cursor.get_u16() as usize;
        let key = cursor.slice(..key_len);
        cursor.advance(key_len);
        let value_len = cursor.get_u32();

        let v = if value_len == TOMBSTONE {
            KVValue::Tombstone
        } else {
            let value = cursor.slice(..value_len as usize);
            KVValue::Value(value)
        };

        Some(KVEntry { key, value: v })
    }
}

#[cfg(test)]
mod tests {
    use crate::block::BlockBuilder;
    use crate::block_iterator::BlockIterator;
    use crate::iter::KeyValueIterator;

    #[tokio::test]
    async fn test_iterator() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add("donkey".as_ref(), Some("kong".as_ref())));
        assert!(block_builder.add("kratos".as_ref(), Some("atreus".as_ref())));
        assert!(block_builder.add("super".as_ref(), Some("mario".as_ref())));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::from_first_key(&block);
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"donkey".as_slice());
        assert_eq!(kv.value, b"kong".as_slice());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"kratos".as_slice());
        assert_eq!(kv.value, b"atreus".as_slice());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"super".as_slice());
        assert_eq!(kv.value, b"mario".as_slice());
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_iter_from_existing_key() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add("donkey".as_ref(), Some("kong".as_ref())));
        assert!(block_builder.add("kratos".as_ref(), Some("atreus".as_ref())));
        assert!(block_builder.add("super".as_ref(), Some("mario".as_ref())));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::from_key(&block, b"kratos".as_ref());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"kratos".as_slice());
        assert_eq!(kv.value, b"atreus".as_slice());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"super".as_slice());
        assert_eq!(kv.value, b"mario".as_slice());
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_iter_from_nonexisting_key() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add("donkey".as_ref(), Some("kong".as_ref())));
        assert!(block_builder.add("kratos".as_ref(), Some("atreus".as_ref())));
        assert!(block_builder.add("super".as_ref(), Some("mario".as_ref())));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::from_key(&block, b"ka".as_ref());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"kratos".as_slice());
        assert_eq!(kv.value, b"atreus".as_slice());
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key, b"super".as_slice());
        assert_eq!(kv.value, b"mario".as_slice());
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_iter_from_end() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add("donkey".as_ref(), Some("kong".as_ref())));
        assert!(block_builder.add("kratos".as_ref(), Some("atreus".as_ref())));
        assert!(block_builder.add("super".as_ref(), Some("mario".as_ref())));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::from_key(&block, b"zzz".as_ref());
        assert!(iter.next().await.unwrap().is_none());
    }
}
