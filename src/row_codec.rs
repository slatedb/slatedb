use std::borrow::Cow;

use crate::db_state::RowFeature;
use crate::error::SlateDBError;
use crate::flatbuffer_types::{SstRowExtra, SstRowExtraArgs};
use crate::types::{KeyValueDeletable, RowAttributes, ValueDeletable};
use bitflags::bitflags;
use bytes::{Buf, BufMut, Bytes, BytesMut};

bitflags! {
    pub(crate) struct RowFlags: u8 {
        const Tombstone = 0b00000001;
    }
}

const NO_EXPIRE_TS: i64 = i64::MIN;

/// Encodes key and value using the binary codec for SlateDB row representation
/// using the `v1` encoding scheme.
///
/// The `v1` codec for the key is (for non-tombstones):
///
/// ```txt
///  |-------------------------------------------------------------------------------------------------------------|
///  |       u16      |      u16       |  var        | u32     | 1 byte    | u16       | var   |    u32    |  var  |
///  |----------------|----------------|-------------|---------|-----------|-----------|-------|-----------|-------|
///  | key_prefix_len | key_suffix_len |  key_suffix | seq     | flags     | extra_len | extra | value_len | value |
///  |-------------------------------------------------------------------------------------------------------------|
/// ```
///
/// And for tombstones (flags & Tombstone == 1):
///
///  ```txt
///  |----------------------------------------------------------|-----------|
///  |       u16      |      u16       |  var        | u32      | 1 byte    |
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

pub(crate) enum SstRowKey {
    Full(Bytes),
    SuffixOnly { prefix_len: usize, suffix: Bytes },
}

pub(crate) struct SstRow {
    key: SstRowKey,
    seq: u32,
    flags: RowFlags,
    created_ts: Option<i64>,
    expire_ts: Option<i64>,
    value: Option<Bytes>,
}

impl SstRow {
    fn new(key_prefix_len: usize, key_suffix: Bytes, seq: u32) -> Self {
        Self {
            key: SstRowKey::SuffixOnly {
                prefix_len: key_prefix_len,
                suffix: key_suffix,
            },
            seq,
            created_ts: None,
            expire_ts: None,
            value: None,
            flags: RowFlags::Tombstone,
        }
    }

    fn key(&self) -> &[u8] {
        match &self.key {
            SstRowKey::Full(key) => key.as_ref(),
            _ => unreachable!("the full key should be reconstructed with prefix while decoding"),
        }
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
        self.value = Some(val);
        self.flags.remove(RowFlags::Tombstone);
        self
    }

    fn with_tombstone(mut self) -> Self {
        self.value = None;
        self.flags.insert(RowFlags::Tombstone);
        self
    }
}

struct SstRowCodecV1 {}

impl SstRowCodecV1 {
    fn encode<'a>(&self, output: &mut Vec<u8>, row: &SstRow) {
        match &row.key {
            SstRowKey::SuffixOnly { prefix_len, suffix } => {
                output.put_u16(*prefix_len as u16);
                output.put_u16(suffix.len() as u16);
                output.put(suffix.as_ref());
            }
            _ => unreachable!("only suffix only key is supported on encode"),
        }

        // encode seq & flags
        output.put_u32(row.seq);
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
            .as_ref()
            .expect("value is not set with no tombstone");
        output.put_u16(val.len() as u16);
        output.put(val.as_ref());
    }

    fn decode<'a>(&self, first_key: &Bytes, data: &mut Bytes) -> Result<SstRow, SlateDBError> {
        let key_prefix_len = data.get_u16() as usize;
        let key_suffix_len = data.get_u16() as usize;
        let key_suffix = data.slice(..key_suffix_len);
        data.advance(key_suffix_len);

        // reconstruct full key
        let mut key = BytesMut::with_capacity(key_prefix_len + key_suffix_len);
        key.extend_from_slice(&first_key[..key_prefix_len]);
        key.extend_from_slice(&key_suffix);

        // decode seq & flags
        let seq = data.get_u32();
        let flags = RowFlags::from_bits(data.get_u8()).ok_or(SlateDBError::InvalidRowFlags)?;

        if flags.contains(RowFlags::Tombstone) {
            return Ok(SstRow::new(key_prefix_len, key_suffix, seq).with_tombstone());
        }

        // decode extra
        let extra_len = data.get_u16() as usize;
        let extra_buf = data.slice(..extra_len);
        data.advance(extra_len);
        let extra = flatbuffers::root::<SstRowExtra>(extra_buf.as_ref())?;
        let created_ts = extra.created_ts();
        let expire_ts = extra.expire_ts();

        // decode value
        let value_len = data.get_u16() as usize;
        let value = data.slice(..value_len);
        Ok(SstRow {
            key: SstRowKey::Full(key.freeze()),
            seq,
            flags,
            created_ts,
            expire_ts,
            value: Some(value.into()),
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
        let row_features = vec![
            RowFeature::Flags,
            RowFeature::Timestamp,
            RowFeature::ExpireAtTs,
        ];

        // Encode the row
        encode_row_v0(
            &mut encoded_data,
            key_prefix_len,
            key_suffix,
            value,
            &row_features,
            Some(1),
            Some(10),
        );

        let first_key = Bytes::from(b"prefixdata".as_ref());
        let mut data = Bytes::from(encoded_data);
        let decoded = decode_row_v0(&first_key, &row_features, &mut data).expect("Decoding failed");

        // Expected key: first_key[..3] + "key" = "prekey"
        let expected_key = Bytes::from(b"prekey" as &[u8]);
        let expected_value = ValueDeletable::Value(Bytes::from(b"value" as &[u8]));

        assert_eq!(decoded.key, expected_key);
        assert_eq!(decoded.value, expected_value);
        assert_eq!(decoded.attributes.ts, Some(1));
        assert_eq!(decoded.attributes.expire_ts, Some(10));
    }

    #[test]
    fn test_encode_decode_normal_row_no_expire_ts() {
        let mut encoded_data = Vec::new();
        let key_prefix_len = 3;
        let key_suffix = b"key";
        let value = Some(b"value".as_slice());
        let row_features = vec![
            RowFeature::Flags,
            RowFeature::Timestamp,
            RowFeature::ExpireAtTs,
        ];

        // Encode the row
        encode_row_v0(
            &mut encoded_data,
            key_prefix_len,
            key_suffix,
            value,
            &row_features,
            Some(1),
            None,
        );

        let first_key = Bytes::from(b"prefixdata".as_ref());
        let mut data = Bytes::from(encoded_data);
        let decoded = decode_row_v0(&first_key, &row_features, &mut data).expect("Decoding failed");

        assert_eq!(decoded.attributes.expire_ts, None);
    }

    #[test]
    fn test_encode_decode_row_with_disabled_row_attr_flag() {
        let mut encoded_data = Vec::new();
        let key_prefix_len = 0;
        let key_suffix = b"";
        let value = Some(b"value".as_slice());
        let row_features = vec![RowFeature::Flags];

        // Encode the row
        encode_row_v0(
            &mut encoded_data,
            key_prefix_len,
            key_suffix,
            value,
            &row_features,
            Some(1), // pass in a timestamp, but it should not be encoded because flag is disabled
            Some(10),
        );

        let first_key = Bytes::from(b"".as_ref());
        let mut data = Bytes::from(encoded_data);
        let decoded = decode_row_v0(&first_key, &row_features, &mut data).expect("Decoding failed");

        assert_eq!(decoded.attributes.ts, None);
    }

    #[test]
    fn test_encode_decode_tombstone_row() {
        let mut encoded_data = Vec::new();
        let key_prefix_len = 4;
        let key_suffix = b"tomb";
        let value = None; // Indicates tombstone
        let row_features = vec![RowFeature::Flags];

        // Encode the tombstone row
        encode_row_v0(
            &mut encoded_data,
            key_prefix_len,
            key_suffix,
            value,
            &row_features,
            Some(1),
            Some(10),
        );

        let first_key = Bytes::from(b"deadbeefdata".as_ref());
        let mut data = Bytes::from(encoded_data);
        let decoded = decode_row_v0(&first_key, &row_features, &mut data).expect("Decoding failed");

        // Expected key: first_key[..4] + "tomb" = "deadtomb"
        let expected_key = Bytes::from(b"deadtomb" as &[u8]);
        let expected_value = ValueDeletable::Tombstone;

        assert_eq!(decoded.key, expected_key);
        assert_eq!(decoded.value, expected_value);
    }

    #[test]
    fn test_decode_invalid_flags() {
        let mut encoded_data = Vec::new();
        let key_prefix_len = 3;
        let key_suffix = b"bad".as_slice();
        let row_features = vec![RowFeature::Flags];

        // Manually encode invalid flags
        encoded_data.put_u16(key_prefix_len as u16);
        encoded_data.put_u16(key_suffix.len() as u16);
        encoded_data.put(key_suffix);
        encoded_data.put_u8(0xFF); // Invalid flag
        encoded_data.put_u32(4);
        encoded_data.put(b"data".as_slice());

        let first_key = Bytes::from(b"prefixdata".as_ref());
        let mut data = Bytes::from(encoded_data);

        // Attempt to decode the row
        let result = decode_row_v0(&first_key, &row_features, &mut data);

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
        let value = Some(b"value".as_slice());
        let row_features = vec![RowFeature::Flags];

        // Encode the row
        encode_row_v0(
            &mut encoded_data,
            key_prefix_len,
            key_suffix,
            value,
            &row_features,
            Some(1),
            Some(10),
        );

        let first_key = Bytes::from(b"keyprefixdata".as_slice());
        let mut data = Bytes::from(encoded_data);
        let decoded = decode_row_v0(&first_key, &row_features, &mut data).expect("Decoding failed");

        // Expected key: first_key[..4] + "" = "keyp"
        let expected_key = Bytes::from(b"keyp" as &[u8]);
        let expected_value = ValueDeletable::Value(Bytes::from(b"value" as &[u8]));

        assert_eq!(decoded.key, expected_key);
        assert_eq!(decoded.value, expected_value);
    }
}
