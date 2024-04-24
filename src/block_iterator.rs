use crate::block::Block;
use bytes::Buf;

pub struct BlockIterator<'a> {
    block: &'a Block,
    key: Option<&'a [u8]>,
    val: Option<&'a [u8]>,
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

    pub fn key(&self) -> Option<&'a [u8]> {
        if self.off_off < self.block.offsets.len() {
            return self.key;
        }
        None
    }

    pub fn val(&self) -> Option<&'a [u8]> {
        if self.off_off < self.block.offsets.len() {
            return self.val;
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
        // TODO: how do we do this without having to get a mutable ref to the data?
        let off_usz = self.off as usize;
        let mut data_from_off: &[u8] = &self.block.data[off_usz..];
        let key_len = data_from_off.get_u16() as usize;
        self.key = Some(&data_from_off[..key_len]);
        data_from_off.advance(key_len);
        let val_len = data_from_off.get_u32() as usize;
        self.val = Some(&data_from_off[..val_len]);
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
        let block = block_builder.build();
        let mut iter = BlockIterator::from_first_key(&block);
        assert_eq!(iter.key().unwrap(), <str as AsRef<[u8]>>::as_ref("super"));
        assert_eq!(iter.val().unwrap(), <str as AsRef<[u8]>>::as_ref("mario"));
        iter.advance();
        assert_eq!(iter.key().unwrap(), <str as AsRef<[u8]>>::as_ref("donkey"));
        assert_eq!(iter.val().unwrap(), <str as AsRef<[u8]>>::as_ref("kong"));
        iter.advance();
        assert_eq!(iter.key().unwrap(), <str as AsRef<[u8]>>::as_ref("kratos"));
        assert_eq!(iter.val().unwrap(), <str as AsRef<[u8]>>::as_ref("atreus"));
        iter.advance();
        assert_eq!(iter.key(), None);
        assert_eq!(iter.val(), None);
    }
}
