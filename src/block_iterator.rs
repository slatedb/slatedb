use crate::{
    block::Block,
    iter::{KeyValue, KeyValueIterator},
};
use bytes::Buf;

pub struct BlockIterator<'a> {
    block: &'a Block,
    off_off: usize,
}

impl<'a> KeyValueIterator for BlockIterator<'a> {
    fn next(&mut self) -> Option<KeyValue> {
        let key_value = self.load_at_current_off()?;
        self.advance();
        Some(key_value)
    }
}

impl<'a> BlockIterator<'a> {
    pub fn from_first_key(block: &'a Block) -> BlockIterator {
        let i = BlockIterator { block, off_off: 0 };
        i
    }

    fn advance(&mut self) {
        self.off_off += 1;
        self.load_at_current_off();
    }

    fn load_at_current_off(&self) -> Option<KeyValue> {
        if self.off_off >= self.block.offsets.len() {
            return None;
        }
        let off = self.block.offsets[self.off_off];
        let off_usz = off as usize;
        // TODO: bounds checks to avoid panics? (paulgb)
        let mut cursor = self.block.data.slice(off_usz..);
        let key_len = cursor.get_u16() as usize;
        let key = cursor.slice(..key_len);
        cursor.advance(key_len);
        let value_len = cursor.get_u32() as usize;
        let value = cursor.slice(..value_len);
        Some(KeyValue { key, value })
    }
}

#[cfg(test)]
mod tests {
    use crate::block::BlockBuilder;
    use crate::block_iterator::BlockIterator;
    use crate::iter::KeyValueIterator;

    #[test]
    fn test_iterator() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add("super".as_ref(), "mario".as_ref()));
        assert!(block_builder.add("donkey".as_ref(), "kong".as_ref()));
        assert!(block_builder.add("kratos".as_ref(), "atreus".as_ref()));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::from_first_key(&block);
        let kv = iter.next().unwrap();
        assert_eq!(kv.key, b"super".as_slice());
        assert_eq!(kv.value, b"mario".as_slice());
        let kv = iter.next().unwrap();
        assert_eq!(kv.key, b"donkey".as_slice());
        assert_eq!(kv.value, b"kong".as_slice());
        let kv = iter.next().unwrap();
        assert_eq!(kv.key, b"kratos".as_slice());
        assert_eq!(kv.value, b"atreus".as_slice());
        assert!(iter.next().is_none());
    }
}
