use crate::db_state::RowFeature;
use crate::error::SlateDBError;
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
///  | key_prefix_len | key_suffix_len |  key_suffix | seq     | tombstone | extra_len | extra | value_len | value |
///  |-------------------------------------------------------------------------------------------------------------|
/// ```
///
/// And for tombstones:
///
///  ```txt
///  |----------------------------------------------------------|-----------|
///  |       u16      |      u16       |  var        | u32      | 1 byte    |
///  |----------------|----------------|-------------|----------|-----------|
///  | key_prefix_len | key_suffix_len |  key_suffix | seq      | tombstone |
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

pub(crate) struct SstRow<'a> {
    key_prefix_len: usize,
    key_suffix: &'a [u8],
    seq: u32,
    tombstone: bool,
    create_ts: Option<i64>,
    expire_ts: Option<i64>,
    value: Option<&'a [u8]>,
}

impl<'a> SstRow<'a> {
    fn new(key_prefix_len: usize, key_suffix: &'a [u8], seq: u32) -> Self {
        Self {
            key_prefix_len,
            key_suffix,
            seq,
            create_ts: None,
            expire_ts: None,
            value: None,
            tombstone: true,
        }
    }

    fn with_create_at(mut self, create_at: i64) -> Self {
        self.create_ts = Some(create_at);
        self
    }

    fn with_expire_at(mut self, expire_at: i64) -> Self {
        self.expire_ts = Some(expire_at);
        self
    }

    fn with_value(mut self, val: &'a [u8]) -> Self {
        self.value = Some(val);
        self.tombstone = false;
        self
    }

    fn with_tombstone(mut self) -> Self {
        self.value = None;
        self.tombstone = true;
        self
    }

    fn encode(self, output: &mut Vec<u8>) {}
}

struct SstRowCodec {}

impl SstRowCodec {
    fn encode<'a>(&self, output: &mut Vec<u8>, row: &SstRow<'a>) {
        output.put_u16(row.key_prefix_len as u16);
        output.put_u16(row.key_suffix.len() as u16);
        output.put(row.key_suffix);
        output.put_u32(row.seq);
        output.put_u8(row.tombstone as u8);
    }
}

pub(crate) fn encode_row_v0(
    data: &mut Vec<u8>,
    key_prefix_len: usize,
    key_suffix: &[u8],
    value: Option<&[u8]>,
    row_features: &[RowFeature],
    timestamp: Option<i64>,
    expire_at: Option<i64>,
) {
    data.put_u16(key_prefix_len as u16);
    data.put_u16(key_suffix.len() as u16);
    data.put(key_suffix);

    data.put(encode_meta(
        row_features,
        value.is_none(),
        timestamp,
        expire_at,
    ));

    if let Some(value) = value {
        data.put_u32(value.len() as u32);
        data.put(value);
    }
}

fn encode_meta(
    row_features: &[RowFeature],
    is_tombstone: bool,
    timestamp: Option<i64>,
    expire_at: Option<i64>,
) -> Bytes {
    let mut meta = BytesMut::new();

    for attr in row_features {
        match attr {
            RowFeature::Flags => meta.put_u8(encode_row_flags(is_tombstone)),
            RowFeature::Timestamp => meta.put_i64(timestamp.expect("Timestamp RowAttribute was enabled for SST but attempted to insert a row with no timestamp.")),
            RowFeature::ExpireAtTs => meta.put_i64(expire_at.unwrap_or(NO_EXPIRE_TS))
        }
    }

    meta.freeze()
}

fn encode_row_flags(is_tombstone: bool) -> u8 {
    let mut flags = RowFlags::empty();

    if is_tombstone {
        flags |= RowFlags::Tombstone;
    }

    flags.bits()
}

/// Decodes a row based on the encoding specified in [`encode_row_v0`]. If the
/// `data` passed in was not encoded using `encode_row_v0`, or if there are any
/// unknown row attributes, this method will return an error.
pub(crate) fn decode_row_v0(
    first_key: &Bytes,
    row_features: &[RowFeature],
    data: &mut Bytes,
) -> Result<KeyValueDeletable, SlateDBError> {
    let key_prefix_len = data.get_u16() as usize;
    let key_suffix_len = data.get_u16() as usize;
    let key_suffix = data.slice(..key_suffix_len);
    data.advance(key_suffix_len);

    let meta = decode_meta(row_features, data)?;
    let value = if !(meta.flags & RowFlags::Tombstone).is_empty() {
        ValueDeletable::Tombstone
    } else {
        let value_len = data.get_u32() as usize;
        ValueDeletable::Value(data.slice(..value_len))
    };

    let mut key = BytesMut::with_capacity(key_prefix_len + key_suffix_len);
    key.extend_from_slice(&first_key[..key_prefix_len]);
    key.extend_from_slice(&key_suffix);

    Ok(KeyValueDeletable {
        key: key.into(),
        value,
        attributes: RowAttributes {
            ts: meta.timestamp,
            expire_ts: meta.expire_ts,
        },
    })
}

struct RowMetadata {
    flags: RowFlags,
    timestamp: Option<i64>,
    expire_ts: Option<i64>,
}

fn decode_meta(row_features: &[RowFeature], data: &mut Bytes) -> Result<RowMetadata, SlateDBError> {
    let mut meta = RowMetadata {
        flags: RowFlags::empty(),
        timestamp: None,
        expire_ts: None,
    };
    for attr in row_features {
        match attr {
            RowFeature::Flags => match decode_row_flags(data.get_u8()) {
                Ok(flags) => {
                    meta.flags = flags;
                }
                Err(e) => return Err(e),
            },
            RowFeature::Timestamp => meta.timestamp = Some(data.get_i64()),
            RowFeature::ExpireAtTs => {
                let expire_ts = data.get_i64();
                meta.expire_ts = if expire_ts == NO_EXPIRE_TS {
                    None
                } else {
                    Some(expire_ts)
                };
            }
        }
    }
    Ok(meta)
}

fn decode_row_flags(flags: u8) -> Result<RowFlags, SlateDBError> {
    match RowFlags::from_bits(flags) {
        None => Err(SlateDBError::InvalidRowFlags),
        Some(flags) => Ok(flags),
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
