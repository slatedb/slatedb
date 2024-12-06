use std::sync::Arc;

use bytes::{Buf, Bytes};

use crate::db_iter::SeekToKey;
use crate::row_codec::SstRowCodecV0;
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

impl<B: BlockLike> SeekToKey for BlockIterator<B> {
    async fn seek(&mut self, next_key: &Bytes) -> Result<(), SlateDBError> {
        loop {
            let result = self.load_at_current_off();
            match result {
                Ok(None) => return Ok(()),
                Ok(Some(kv)) => {
                    if kv.key < next_key {
                        self.advance();
                    } else {
                        return Ok(());
                    }
                }
                Err(e) => return Err(e),
            }
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
    pub async fn from_key(block: B, key: &[u8]) -> Result<BlockIterator<B>, SlateDBError> {
        let mut iter = Self::from_first_key(block);
        let seek_key = Bytes::copy_from_slice(key);
        iter.seek(&seek_key).await?;
        Ok(iter)
    }

    fn advance(&mut self) {
        self.off_off += 1;
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.off_off >= self.block.offsets().len()
    }

    fn load_at_current_off(&self) -> Result<Option<RowEntry>, SlateDBError> {
        if self.is_empty() {
            return Ok(None);
        }
        let off = self.block.offsets()[self.off_off];
        let off_usz = off as usize;
        // TODO: bounds checks to avoid panics? (paulgb)
        let mut cursor = self.block.data().slice(off_usz..);
        let codec = SstRowCodecV0::new();
        let sst_row = codec.decode(&mut cursor)?;
        Ok(Some(RowEntry::new(
            sst_row.restore_full_key(&self.first_key),
            sst_row.value.into_option(),
            sst_row.seq,
            sst_row.create_ts,
            sst_row.expire_ts,
        )))
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
    use crate::db_iter::SeekToKey;
    use crate::iter::KeyValueIterator;
    use crate::test_utils;
    use crate::test_utils::{assert_iterator, assert_next_entry, gen_attrs};
    use crate::types::ValueDeletable;
    use bytes::Bytes;

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
        let mut iter = BlockIterator::from_key(&block, b"kratos".as_ref())
            .await
            .unwrap();
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
        let mut iter = BlockIterator::from_key(&block, b"ka".as_ref())
            .await
            .unwrap();
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
        let mut iter = BlockIterator::from_key(&block, b"zzz".as_ref())
            .await
            .unwrap();
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_seek_to_key_skips_records_prior_to_next_key() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_kv("donkey".as_ref(), Some("kong".as_ref()), gen_attrs(1)));
        assert!(block_builder.add_kv("kratos".as_ref(), Some("atreus".as_ref()), gen_attrs(2)));
        assert!(block_builder.add_kv("super".as_ref(), Some("mario".as_ref()), gen_attrs(3)));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::from_first_key(block);
        assert_next_entry(
            &mut iter,
            &(
                "donkey".into(),
                ValueDeletable::Value(Bytes::from("kong")),
                gen_attrs(1),
            ),
        )
        .await;
        iter.seek(&Bytes::from_static(b"s")).await.unwrap();
        assert_iterator(
            &mut iter,
            &[(
                "super".into(),
                ValueDeletable::Value(Bytes::from("mario")),
                gen_attrs(3),
            )],
        )
        .await;
    }

    #[tokio::test]
    async fn test_seek_to_key_with_iterator_at_seek_point() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_kv("donkey".as_ref(), Some("kong".as_ref()), gen_attrs(1)));
        assert!(block_builder.add_kv("kratos".as_ref(), Some("atreus".as_ref()), gen_attrs(2)));
        assert!(block_builder.add_kv("super".as_ref(), Some("mario".as_ref()), gen_attrs(3)));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::from_first_key(block);
        assert_next_entry(
            &mut iter,
            &(
                "donkey".into(),
                ValueDeletable::Value(Bytes::from("kong")),
                gen_attrs(1),
            ),
        )
        .await;
        iter.seek(&Bytes::from_static(b"kratos")).await.unwrap();
        assert_iterator(
            &mut iter,
            &[
                (
                    "kratos".into(),
                    ValueDeletable::Value(Bytes::from("atreus")),
                    gen_attrs(2),
                ),
                (
                    "super".into(),
                    ValueDeletable::Value(Bytes::from("mario")),
                    gen_attrs(3),
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_seek_to_key_beyond_last_key_in_block() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_kv("donkey".as_ref(), Some("kong".as_ref()), gen_attrs(1)));
        assert!(block_builder.add_kv("kratos".as_ref(), Some("atreus".as_ref()), gen_attrs(2)));
        assert!(block_builder.add_kv("super".as_ref(), Some("mario".as_ref()), gen_attrs(3)));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::from_first_key(block);
        iter.seek(&Bytes::from_static(b"zelda")).await.unwrap();
        assert_iterator(&mut iter, &[]).await;
    }
}
