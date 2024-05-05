use bytes::Bytes;

pub(crate) struct BloomFilterBuilder {
    bits_per_key: u32,
    keys: Vec<Bytes>,
}

#[derive(PartialEq, Eq)]
pub(crate) struct BloomFilter {
    buffer: Vec<u8>,
}

impl BloomFilterBuilder {
    pub(crate) fn new(bits_per_key: u32) -> Self {
        Self {
            bits_per_key,
            keys: Vec::new(),
        }
    }

    pub(crate) fn add_key(&mut self, key: &[u8]) {
        self.keys.push(Bytes::copy_from_slice(key))
    }

    fn filter_bytes(&self, num_keys: u32, bits_per_key: u32) -> usize {
        let filter_bits = num_keys * bits_per_key;
        // compute filter bytes rounded up to the number of bytes required to fit the filter
        ((filter_bits + 7) / 8) as usize
    }

    pub(crate) fn build(&self) -> BloomFilter {
        let filter_size = self.filter_bytes(self.keys.len() as u32, self.bits_per_key);
        let buffer = vec![0xFF; filter_size];
        BloomFilter { buffer }
    }
}

impl BloomFilter {
    pub(crate) fn decode(buf: &[u8]) -> BloomFilter {
        BloomFilter {
            buffer: Vec::from(buf),
        }
    }

    pub(crate) fn encode(&self) -> Bytes {
        return Bytes::copy_from_slice(self.buffer.as_slice());
    }

    pub(crate) fn has_key(&self, _key: &[u8]) -> bool {
        true
    }
}
