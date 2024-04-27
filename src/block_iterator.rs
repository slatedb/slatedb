use crate::block::Block;
use bytes::{Buf, Bytes};

pub struct BlockIterator<'a> {
    block: &'a Block,
    key: Option<Bytes>,
    val: Option<Bytes>,
    off: u16,
    off_off: usize,
}

impl<'a> BlockIterator<'a> {
    pub fn from_first_key(block: &'a Block) -> BlockIterator {
        let mut i = BlockIterator {
            block,
            key: None,
            val: None,
            off: 0,
            off_off: 0,
        };
        i.load_at_current_off();
        i
    }

    pub fn key(&self) -> Option<Bytes> {
        if self.off_off < self.block.offsets.len() {
            return self.key.clone();
        }
        None
    }

    pub fn val(&self) -> Option<Bytes> {
        if self.off_off < self.block.offsets.len() {
            return self.val.clone();
        }
        None
    }

    pub fn advance(&mut self) {
        self.off_off += 1;
        self.load_at_current_off();
    }

    fn load_at_current_off(&mut self) {
        if self.off_off >= self.block.offsets.len() {
            return;
        }
        self.off = self.block.offsets[self.off_off];
        let off_usz = self.off as usize;
        // TODO: bounds checks to avoid panics? (paulgb)
        let mut cursor = self.block.data.slice(off_usz..);
        let key_len = cursor.get_u16() as usize;
        let key = cursor.slice(..key_len);
        cursor.advance(key_len);
        let val_len = cursor.get_u32() as usize;
        let val = cursor.slice(..val_len);
        self.key = Some(key);
        self.val = Some(val);
    }
}

#[cfg(test)]
mod tests {
    use crate::block::BlockBuilder;
    use crate::block_iterator::BlockIterator;

    #[test]
    fn test_iterator() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add("super".as_ref(), "mario".as_ref()));
        assert!(block_builder.add("donkey".as_ref(), "kong".as_ref()));
        assert!(block_builder.add("kratos".as_ref(), "atreus".as_ref()));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::from_first_key(&block);
        assert_eq!(iter.key().unwrap().to_vec(), b"super".to_vec());
        assert_eq!(iter.val().unwrap().to_vec(), b"mario");
        iter.advance();
        assert_eq!(iter.key().unwrap().to_vec(), b"donkey".to_vec());
        assert_eq!(iter.val().unwrap().to_vec(), b"kong".to_vec());
        iter.advance();
        assert_eq!(iter.key().unwrap().to_vec(), b"kratos".to_vec());
        assert_eq!(iter.val().unwrap().to_vec(), b"atreus".to_vec());
        iter.advance();
        assert_eq!(iter.key(), None);
        assert_eq!(iter.val(), None);
    }
}
