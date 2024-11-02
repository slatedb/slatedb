use crate::error::SlateDBError;
use crate::flatbuffer_types::{SstRowExtra, SstRowExtraArgs};
use crate::types::ValueDeletable;
use bitflags::bitflags;
use bytes::{Buf, BufMut, Bytes, BytesMut};

bitflags! {
    #[derive(Debug, Clone, PartialEq, Default)]
    pub(crate) struct RowFlags: u8 {
        const Tombstone = 0b00000001;
    }
}

/// Encodes key and value using the binary codec for SlateDB row representation
/// using the `v1` encoding scheme.
///
/// The `v1` codec for the key is (for non-tombstones):
///
/// ```txt
///  |-------------------------------------------------------------------------------------------------------------|
///  |       u16      |      u16       |  var        | u64     | u8        | u16       | var   |    u32    |  var  |
///  |----------------|----------------|-------------|---------|-----------|-----------|-------|-----------|-------|
///  | key_prefix_len | key_suffix_len |  key_suffix | seq     | flags     | extra_len | extra | value_len | value |
///  |-------------------------------------------------------------------------------------------------------------|
/// ```
///
/// And for tombstones (flags & Tombstone == 1):
///
///  ```txt
///  |----------------------------------------------------------|-----------|
///  |       u16      |      u16       |  var        | u64      | u8        |
///  |----------------|----------------|-------------|----------|-----------|
///  | key_prefix_len | key_suffix_len |  key_suffix | seq      | flags     |
///  |----------------------------------------------------------------------|
///  ```
///
/// | Field            | Type | Description                                   |
/// |------------------|------|-----------------------------------------------|
/// | `key_prefix_len` | `u16` | Length of the key prefix                     |
/// | `key_suffix_len` | `u16` | Length of the key suffix                     |
/// | `key_suffix`     | `var` | Suffix of the key                            |
/// | `meta`           | `var` | Metadata                                     |
/// | `value_len`      | `u32` | Length of the value                          |
/// | `value`          | `var` | Value bytes                                  |

#[derive(Debug)]
pub(crate) enum SstRowKey {
    Full(Bytes),
    SuffixOnly { prefix_len: usize, suffix: Bytes },
}

pub(crate) struct SstRow {
    key: SstRowKey,
    seq: u64,
    flags: RowFlags,
    created_ts: Option<i64>,
    expire_ts: Option<i64>,
    value: ValueDeletable,
}

impl SstRow {
    fn new(key_prefix_len: usize, key_suffix: Bytes, seq: u64) -> Self {
        Self {
            key: SstRowKey::SuffixOnly {
                prefix_len: key_prefix_len,
                suffix: key_suffix,
            },
            seq,
            created_ts: None,
            expire_ts: None,
            value: ValueDeletable::Tombstone,
            flags: RowFlags::Tombstone,
        }
    }

    fn key(&self) -> &[u8] {
        match &self.key {
            SstRowKey::Full(key) => key.as_ref(),
            _ => unreachable!("the full key should be reconstructed with prefix while decoding"),
        }
    }

    fn value(&self) -> &ValueDeletable {
        &self.value
    }

    fn with_create_ts(mut self, create_at: i64) -> Self {
        self.created_ts = Some(create_at);
        self
    }

    fn with_expire_ts(mut self, expire_at: i64) -> Self {
        self.expire_ts = Some(expire_at);
        self
    }

    fn with_value(mut self, val: Bytes) -> Self {
        self.value = ValueDeletable::Value(val);
        self.flags.remove(RowFlags::Tombstone);
        self
    }

    fn with_tombstone(mut self) -> Self {
        self.value = ValueDeletable::Tombstone;
        self.flags.insert(RowFlags::Tombstone);
        self
    }
}

pub(crate) struct SstRowCodecV1 {}

impl SstRowCodecV1 {
    pub fn new() -> Self {
        Self {}
    }

    pub fn encode(&self, output: &mut Vec<u8>, row: &SstRow) {
        match &row.key {
            SstRowKey::SuffixOnly { prefix_len, suffix } => {
                output.put_u16(*prefix_len as u16);
                output.put_u16(suffix.len() as u16);
                output.put(suffix.as_ref());
            }
            _ => unreachable!("only suffix only key is supported on encode"),
        }

        // encode seq & flags
        output.put_u64(row.seq);
        output.put_u8(row.flags.bits());
        if row.flags.contains(RowFlags::Tombstone) {
            return;
        }

        // encode extra
        // TODO: maybe moved to flatbuffer_types.rs
        let mut extra_builder = flatbuffers::FlatBufferBuilder::new();
        let extra = SstRowExtra::create(
            &mut extra_builder,
            &SstRowExtraArgs {
                created_ts: row.created_ts.clone(),
                expire_ts: row.expire_ts.clone(),
            },
        );
        extra_builder.finish(extra, None);
        output.put_u16(extra_builder.finished_data().len() as u16);
        output.put(extra_builder.finished_data());

        // encode value
        let val = row
            .value
            .as_option()
            .expect("value is not set with no tombstone");
        output.put_u32(val.len() as u32);
        output.put(val.as_ref());
    }

    pub fn decode<'a>(&self, first_key: &Bytes, data: &mut Bytes) -> Result<SstRow, SlateDBError> {
        let key_prefix_len = data.get_u16() as usize;
        let key_suffix_len = data.get_u16() as usize;
        let key_suffix = data.slice(..key_suffix_len);
        data.advance(key_suffix_len);

        // reconstruct full key
        let mut full_key = BytesMut::with_capacity(key_prefix_len + key_suffix_len);
        full_key.extend_from_slice(&first_key[..key_prefix_len]);
        full_key.extend_from_slice(&key_suffix);

        // decode seq & flags
        let seq = data.get_u64();
        let flags = RowFlags::from_bits(data.get_u8()).ok_or(SlateDBError::InvalidRowFlags)?;

        if flags.contains(RowFlags::Tombstone) {
            return Ok(SstRow {
                key: SstRowKey::Full(full_key.freeze()),
                seq,
                flags,
                created_ts: None,
                expire_ts: None,
                value: ValueDeletable::Tombstone,
            });
        }

        // decode extra
        let extra_len = data.get_u16() as usize;
        let extra_buf = data.slice(..extra_len);
        data.advance(extra_len);
        let extra = flatbuffers::root::<SstRowExtra>(extra_buf.as_ref())?;
        let created_ts = extra.created_ts();
        let expire_ts = extra.expire_ts();

        // decode value
        let value_len = data.get_u32() as usize;
        let value = data.slice(..value_len);
        Ok(SstRow {
            key: SstRowKey::Full(full_key.freeze()),
            seq,
            flags,
            created_ts,
            expire_ts,
            value: ValueDeletable::Value(value.into()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_state::RowFeature;
    use crate::types::ValueDeletable;

    #[test]
    fn test_encode_decode_normal_row() {
        let mut encoded_data = Vec::new();
        let key_prefix_len = 3;
        let key_suffix = b"key";
        let value = Some(b"value".as_slice());

        // Encode the row
        let codec = SstRowCodecV1 {};
        codec.encode(
            &mut encoded_data,
            &SstRow::new(key_prefix_len, Bytes::from(key_suffix.to_vec()), 1)
                .with_value(Bytes::from(value.unwrap().to_vec()))
                .with_create_ts(1)
                .with_expire_ts(10),
        );

        let first_key = Bytes::from(b"prefixdata".as_ref());
        let mut data = Bytes::from(encoded_data);

        let decoded = codec
            .decode(&first_key, &mut data)
            .expect("decoding failed");

        // Expected key: first_key[..3] + "key" = "prekey"
        let expected_key = Bytes::from(b"prekey" as &[u8]);
        let expected_value = ValueDeletable::Value(Bytes::from(b"value" as &[u8]));

        assert_eq!(decoded.key(), expected_key);
        assert_eq!(decoded.value, expected_value);
        assert_eq!(decoded.created_ts, Some(1));
        assert_eq!(decoded.expire_ts, Some(10));
    }

    #[test]
    fn test_encode_decode_normal_row_no_expire_ts() {
        let mut encoded_data = Vec::new();
        let key_prefix_len = 3;
        let key_suffix = b"key";
        let value = Some(b"value".as_slice());

        // Encode the row
        let codec = SstRowCodecV1 {};
        codec.encode(
            &mut encoded_data,
            &SstRow::new(key_prefix_len, Bytes::from(key_suffix.to_vec()), 1)
                .with_value(Bytes::from(value.unwrap().to_vec()))
                .with_create_ts(1),
        );

        let first_key = Bytes::from(b"prefixdata".as_ref());
        let mut data = Bytes::from(encoded_data);
        let decoded = codec
            .decode(&first_key, &mut data)
            .expect("decoding failed");

        assert_eq!(decoded.expire_ts, None);
    }

    #[test]
    fn test_encode_decode_tombstone_row() {
        let mut encoded_data = Vec::new();
        let key_prefix_len = 4;
        let key_suffix = b"tomb";

        // Encode the row
        let codec = SstRowCodecV1 {};
        codec.encode(
            &mut encoded_data,
            &SstRow::new(key_prefix_len, Bytes::from(key_suffix.to_vec()), 1)
                .with_create_ts(1)
                .with_tombstone(),
        );

        let first_key = Bytes::from(b"deadbeefdata".as_ref());
        let mut data = Bytes::from(encoded_data);
        let decoded = codec
            .decode(&first_key, &mut data)
            .expect("decoding failed");

        // Expected key: first_key[..4] + "tomb" = "deadtomb"
        let expected_key = Bytes::from(b"deadtomb" as &[u8]);
        let expected_value = ValueDeletable::Tombstone;

        assert_eq!(decoded.key(), expected_key);
        assert_eq!(decoded.value(), &expected_value);
        assert_eq!(decoded.created_ts, None);
    }

    #[test]
    fn test_decode_invalid_flags() {
        let mut encoded_data = Vec::new();
        let key_prefix_len = 3;
        let key_suffix = b"bad".as_slice();

        // Manually encode invalid flags
        encoded_data.put_u16(key_prefix_len as u16);
        encoded_data.put_u16(key_suffix.len() as u16);
        encoded_data.put(key_suffix);
        encoded_data.put_u64(100);
        encoded_data.put_u8(0xFF); // Invalid flag
        encoded_data.put_u32(4);
        encoded_data.put(b"data".as_slice());

        let first_key = Bytes::from(b"prefixdata".as_ref());
        let mut data = Bytes::from(encoded_data);

        // Attempt to decode the row
        let codec = SstRowCodecV1 {};
        let result = codec.decode(&first_key, &mut data);

        assert!(result.is_err());
        match result {
            Err(SlateDBError::InvalidRowFlags) => (),
            _ => panic!("Expected InvalidRowFlags"),
        }
    }

    #[test]
    fn test_encode_decode_empty_key_suffix() {
        let mut encoded_data = Vec::new();
        let key_prefix_len = 4;
        let key_suffix = b""; // Empty key suffix

        // Encode the row
        let codec = SstRowCodecV1 {};
        codec.encode(
            &mut encoded_data,
            &SstRow::new(key_prefix_len, Bytes::from(key_suffix.to_vec()), 1)
                .with_create_ts(1)
                .with_value("value".into()),
        );

        let first_key = Bytes::from(b"keyprefixdata".as_slice());
        let mut data = Bytes::from(encoded_data);
        let decoded = codec
            .decode(&first_key, &mut data)
            .expect("decoding failed");

        // Expected key: first_key[..4] + "" = "keyp"
        let expected_key = Bytes::from(b"keyp" as &[u8]);
        let expected_value = ValueDeletable::Value(Bytes::from(b"value" as &[u8]));

        assert_eq!(decoded.key(), expected_key);
        assert_eq!(decoded.value, expected_value);
    }
}
