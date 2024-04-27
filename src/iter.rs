use bytes::Bytes;

#[derive(Debug)]
pub struct KeyValue {
    pub key: Bytes,
    pub value: Bytes,
}

pub trait KeyValueIterator {
    fn next(&mut self) -> Option<KeyValue>;
}
