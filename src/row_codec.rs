use crate::db_state::RowAttribute;
use crate::error::SlateDBError;
use crate::types::{KeyValueDeletable, ValueDeletable};
use bitflags::bitflags;
use bytes::{Buf, BufMut, Bytes, BytesMut};

bitflags! {
    pub(crate) struct RowFlags: u8 {
        const Tombstone = 0x01;
    }
}

/// Encodes key and value using the binary codec for SlateDB row representation
/// using the `v0` encoding scheme.
///
/// The `v0` codec for the key is (for non-tombstones):
///
/// ```txt
///  |--------------------------------------------------------------------------|
///  |       u16      |      u16       |  var        | var  |    u32    |  var  |
///  |----------------|----------------|-------------|------|-----------|-------|
///  | key_prefix_len | key_suffix_len |  key_suffix | meta | value_len | value |
///  |--------------------------------------------------------------------------|
/// ```
///
/// And for tombstones (with `meta[row_flags] & 0x01 == 1`):
///  ```txt
///  |------------------------------------------------------|
///  |       u16      |      u16       |  var        | var  |
///  |----------------|----------------|-------------|------|
///  | key_prefix_len | key_suffix_len |  key_suffix | meta |
///  |------------------------------------------------------|
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
pub(crate) fn encode_row_v0(
    data: &mut Vec<u8>,
    key_prefix_len: usize,
    key_suffix: &[u8],
    value: Option<&[u8]>,
    row_attributes: &Vec<RowAttribute>,
) {
    data.put_u16(key_prefix_len as u16);
    data.put_u16(key_suffix.len() as u16);
    data.put(key_suffix);

    data.put(encode_meta(row_attributes, value.is_none()));

    if let Some(value) = value {
        data.put_u32(value.len() as u32);
        data.put(value);
    }
}

fn encode_meta(row_attributes: &Vec<RowAttribute>, is_tombstone: bool) -> Bytes {
    let mut meta = BytesMut::new();

    for attr in row_attributes {
        match attr {
            RowAttribute::Flags => meta.put_u8(encode_row_flags(is_tombstone)),
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
    row_attributes: &Vec<RowAttribute>,
    data: &mut Bytes,
) -> Result<KeyValueDeletable, SlateDBError> {
    let key_prefix_len = data.get_u16() as usize;
    let key_suffix_len = data.get_u16() as usize;
    let key_suffix = data.slice(..key_suffix_len);
    data.advance(key_suffix_len);

    let meta = match decode_meta(row_attributes, data) {
        Ok(meta) => meta,
        Err(e) => return Err(e),
    };
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
    })
}

struct RowMetadata {
    flags: RowFlags,
}

fn decode_meta(
    row_attributes: &Vec<RowAttribute>,
    data: &mut Bytes,
) -> Result<RowMetadata, SlateDBError> {
    let mut meta = RowMetadata {
        flags: RowFlags::empty(),
    };
    for attr in row_attributes {
        match attr {
            RowAttribute::Flags => match decode_row_flags(data.get_u8()) {
                Ok(flags) => {
                    meta.flags = flags;
                }
                Err(e) => return Err(e),
            },
        }
    }
    Ok(meta)
}

fn decode_row_flags(flags: u8) -> Result<RowFlags, SlateDBError> {
    match RowFlags::from_bits(flags) {
        None => Err(SlateDBError::BlockCompressionError),
        Some(flags) => Ok(flags),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_state::RowAttribute;
    use crate::types::ValueDeletable;

    #[test]
    fn test_encode_decode_normal_row() {
        let mut encoded_data = Vec::new();
        let key_prefix_len = 3;
        let key_suffix = b"key";
        let value = Some(b"value".as_slice());
        let row_attributes = vec![RowAttribute::Flags];

        // Encode the row
        encode_row_v0(
            &mut encoded_data,
            key_prefix_len,
            key_suffix,
            value,
            &row_attributes,
        );

        let first_key = Bytes::from(b"prefixdata".as_ref());
        let mut data = Bytes::from(encoded_data);
        let decoded =
            decode_row_v0(&first_key, &row_attributes, &mut data).expect("Decoding failed");

        // Expected key: first_key[..3] + "key" = "prekey"
        let expected_key = Bytes::from(b"prekey" as &[u8]);
        let expected_value = ValueDeletable::Value(Bytes::from(b"value" as &[u8]));

        assert_eq!(decoded.key, expected_key);
        assert_eq!(decoded.value, expected_value);
    }

    #[test]
    fn test_encode_decode_tombstone_row() {
        let mut encoded_data = Vec::new();
        let key_prefix_len = 4;
        let key_suffix = b"tomb";
        let value = None; // Indicates tombstone
        let row_attributes = vec![RowAttribute::Flags];

        // Encode the tombstone row
        encode_row_v0(
            &mut encoded_data,
            key_prefix_len,
            key_suffix,
            value,
            &row_attributes,
        );

        let first_key = Bytes::from(b"deadbeefdata".as_ref());
        let mut data = Bytes::from(encoded_data);
        let decoded =
            decode_row_v0(&first_key, &row_attributes, &mut data).expect("Decoding failed");

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
        let row_attributes = vec![RowAttribute::Flags];

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
        let result = decode_row_v0(&first_key, &row_attributes, &mut data);

        assert!(result.is_err());
        match result {
            Err(SlateDBError::BlockCompressionError) => (),
            _ => panic!("Expected BlockCompressionError"),
        }
    }

    #[test]
    fn test_encode_decode_empty_key_suffix() {
        let mut encoded_data = Vec::new();
        let key_prefix_len = 4;
        let key_suffix = b""; // Empty key suffix
        let value = Some(b"value".as_slice());
        let row_attributes = vec![RowAttribute::Flags];

        // Encode the row
        encode_row_v0(
            &mut encoded_data,
            key_prefix_len,
            key_suffix,
            value,
            &row_attributes,
        );

        let first_key = Bytes::from(b"keyprefixdata".as_slice());
        let mut data = Bytes::from(encoded_data);
        let decoded =
            decode_row_v0(&first_key, &row_attributes, &mut data).expect("Decoding failed");

        // Expected key: first_key[..4] + "" = "keyp"
        let expected_key = Bytes::from(b"keyp" as &[u8]);
        let expected_value = ValueDeletable::Value(Bytes::from(b"value" as &[u8]));

        assert_eq!(decoded.key, expected_key);
        assert_eq!(decoded.value, expected_value);
    }
}
