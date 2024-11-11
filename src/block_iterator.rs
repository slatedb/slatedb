use std::sync::Arc;

use bytes::{Buf, Bytes, BytesMut};

use crate::row_codec::SstRowCodecV1;
use crate::{block::Block, error::SlateDBError, iter::KeyValueIterator, types::RowEntry};

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

impl BlockLike for Arc<Block> {
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
    // first key in the block, because slateDB does not support multi version of keys
    // so we use `Bytes` temporarily
    first_key: Bytes,
}

impl<B: BlockLike> KeyValueIterator for BlockIterator<B> {
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        let result = self.load_at_current_off();
        match result {
            Ok(None) => Ok(None),
            Ok(key_value) => {
                self.advance();
                Ok(key_value)
            }
            Err(e) => Err(e),
        }
    }
}

impl<B: BlockLike> BlockIterator<B> {
    pub fn from_first_key(block: B) -> BlockIterator<B> {
        BlockIterator {
            first_key: BlockIterator::decode_first_key(&block),
            block,
            off_off: 0,
        }
    }

    /// Construct a BlockIterator that starts at the given key, or at the first
    /// key greater than the given key if the exact key given is not in the block.
    pub fn from_key(block: B, key: &[u8]) -> BlockIterator<B> {
        let first_key = BlockIterator::decode_first_key(&block);

        let idx = block.offsets().partition_point(|offset| {
            let mut cursor = &block.data()[*offset as usize..];
            let overlap_len = cursor.get_u16() as usize;
            let rest_len = cursor.get_u16() as usize;
            let rest_key = &cursor[..rest_len];
            let mut cursor_key = BytesMut::with_capacity(overlap_len + rest_len);
            cursor_key.extend_from_slice(&first_key[..overlap_len]);
            cursor_key.extend_from_slice(rest_key);
            cursor_key < key
        });

        BlockIterator {
            block,
            off_off: idx,
            first_key,
        }
    }

    fn advance(&mut self) {
        self.off_off += 1;
    }

    fn load_at_current_off(&self) -> Result<Option<RowEntry>, SlateDBError> {
        if self.off_off >= self.block.offsets().len() {
            return Ok(None);
        }
        let off = self.block.offsets()[self.off_off];
        let off_usz = off as usize;
        // TODO: bounds checks to avoid panics? (paulgb)
        let mut cursor = self.block.data().slice(off_usz..);
        let codec = SstRowCodecV1::new();
        let row = codec.decode(&self.first_key, &mut cursor)?;
        Ok(Some(row.into()))
    }

    pub fn decode_first_key(block: &B) -> Bytes {
        let mut buf = block.data().slice(..);
        let overlap_len = buf.get_u16() as usize;
        assert_eq!(overlap_len, 0, "first key overlap should be 0");
        let key_len = buf.get_u16() as usize;
        let first_key = &buf[..key_len];
        Bytes::copy_from_slice(first_key)
    }
}

#[cfg(test)]
mod tests {
    use crate::block::BlockBuilder;
    use crate::block_iterator::BlockIterator;
    use crate::iter::KeyValueIterator;
    use crate::test_utils;
    use crate::test_utils::gen_attrs;

    #[tokio::test]
    async fn test_iterator() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_kv("donkey".as_ref(), Some("kong".as_ref()), gen_attrs(1)));
        assert!(block_builder.add_kv("kratos".as_ref(), Some("atreus".as_ref()), gen_attrs(2)));
        assert!(block_builder.add_kv("super".as_ref(), Some("mario".as_ref()), gen_attrs(3)));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::from_first_key(&block);
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"donkey", b"kong");
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"kratos", b"atreus");
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"super", b"mario");
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_iter_from_existing_key() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_kv("donkey".as_ref(), Some("kong".as_ref()), gen_attrs(1)));
        assert!(block_builder.add_kv("kratos".as_ref(), Some("atreus".as_ref()), gen_attrs(2)));
        assert!(block_builder.add_kv("super".as_ref(), Some("mario".as_ref()), gen_attrs(3)));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::from_key(&block, b"kratos".as_ref());
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"kratos", b"atreus");
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"super", b"mario");
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_iter_from_nonexisting_key() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_kv("donkey".as_ref(), Some("kong".as_ref()), gen_attrs(1)));
        assert!(block_builder.add_kv("kratos".as_ref(), Some("atreus".as_ref()), gen_attrs(2)));
        assert!(block_builder.add_kv("super".as_ref(), Some("mario".as_ref()), gen_attrs(3)));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::from_key(&block, b"ka".as_ref());
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"kratos", b"atreus");
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"super", b"mario");
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_iter_from_end() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_kv("donkey".as_ref(), Some("kong".as_ref()), gen_attrs(1)));
        assert!(block_builder.add_kv("kratos".as_ref(), Some("atreus".as_ref()), gen_attrs(2)));
        assert!(block_builder.add_kv("super".as_ref(), Some("mario".as_ref()), gen_attrs(3)));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::from_key(&block, b"zzz".as_ref());
        assert!(iter.next().await.unwrap().is_none());
    }
}
