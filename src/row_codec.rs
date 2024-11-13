use crate::error::SlateDBError;
use crate::types::ValueDeletable;
use bitflags::bitflags;
use bytes::{Buf, BufMut, Bytes, BytesMut};

bitflags! {
    #[derive(Debug, Clone, PartialEq, Default)]
    pub(crate) struct RowFlags: u8 {
        const TOMBSTONE = 0b00000001;
        const HAS_EXPIRE_TS = 0b00000010;
        const HAS_CREATE_TS = 0b00000100;
    }
}

/// Encodes key and value using the binary codec for SlateDB row representation
/// using the `v0` encoding scheme.
///
/// The `v0` codec for the key is (for non-tombstones):
///
/// ```txt
///  |---------------------------------------------------------------------------------------------------------------------|
///  |       u16      |      u16       |  var        | u64     | u8        | i64       | i64       | u32       |  var      |
///  |----------------|----------------|-------------|---------|-----------|-----------|-----------|-----------|-----------|
///  | key_prefix_len | key_suffix_len |  key_suffix | seq     | flags     | expire_ts | create_ts | value_len | value     |
///  |---------------------------------------------------------------------------------------------------------------------|
/// ```
///
/// And for tombstones (flags & Tombstone == 1):
///
///  ```txt
///  |----------------------------------------------------------|-----------|-----------|
///  |       u16      |      u16       |  var        | u64      | u8        | i64       |
///  |----------------|----------------|-------------|----------|-----------|-----------|
///  | key_prefix_len | key_suffix_len |  key_suffix | seq      | flags     | create_ts |
///  |----------------------------------------------------------------------------------|
///  ```
///
/// | Field            | Type  | Description                                            |
/// |------------------|-------|--------------------------------------------------------|
/// | `key_prefix_len` | `u16` | Length of the key prefix                               |
/// | `key_suffix_len` | `u16` | Length of the key suffix                               |
/// | `key_suffix`     | `var` | Suffix of the key                                      |
/// | `seq`            | `u64` | Sequence Number                                        |
/// | `flags`          | `u8`  | Flags of the row                                       |
/// | `expire_ts`      | `u64` | Optional, only has value when flags & HAS_EXPIRE_TS    |
/// | `create_ts`      | `u64` | Optional, only has value when flags & HAS_CREATE_TS    |
/// | `value_len`      | `u32` | Length of the value                                    |
/// | `value`          | `var` | Value bytes                                            |

pub(crate) struct SstRowEntry {
    pub key_prefix_len: usize,
    pub key_suffix: Bytes,
    pub seq: u64,
    pub expire_ts: Option<i64>,
    pub create_ts: Option<i64>,
    pub value: ValueDeletable,
}

impl SstRowEntry {
    pub fn new(
        key_prefix_len: usize,
        key_suffix: Bytes,
        seq: u64,
        value: ValueDeletable,
        create_ts: Option<i64>,
        expire_ts: Option<i64>,
    ) -> Self {
        Self {
            key_prefix_len,
            key_suffix,
            seq,
            create_ts,
            expire_ts,
            value,
        }
    }

    fn flags(&self) -> RowFlags {
        let mut flags = match &self.value {
            ValueDeletable::Value(_) => RowFlags::default(),
            ValueDeletable::Tombstone => RowFlags::TOMBSTONE,
        };
        if self.expire_ts.is_some() {
            flags |= RowFlags::HAS_EXPIRE_TS;
        }
        if self.create_ts.is_some() {
            flags |= RowFlags::HAS_CREATE_TS;
        }
        flags
    }

    /// Keys in a Block are stored with prefix stripped off to compress the storage size. This function
    /// restores the full key by prepending the prefix to the key suffix.
    pub fn restore_full_key(&self, prefix: &Bytes) -> Bytes {
        let mut full_key = BytesMut::with_capacity(self.key_prefix_len + self.key_suffix.len());
        full_key.extend_from_slice(&prefix[..self.key_prefix_len]);
        full_key.extend_from_slice(&self.key_suffix);
        full_key.freeze()
    }
}

pub(crate) struct SstRowCodecV0 {}

impl SstRowCodecV0 {
    pub fn new() -> Self {
        Self {}
    }

    pub fn encode(&self, output: &mut Vec<u8>, row: &SstRowEntry) {
        output.put_u16(row.key_prefix_len as u16);
        output.put_u16(row.key_suffix.len() as u16);
        output.put(row.key_suffix.as_ref());

        // encode seq & flags
        let flags = row.flags();
        output.put_u64(row.seq);
        output.put_u8(flags.bits());

        // encode expire & create ts
        if flags.contains(RowFlags::HAS_EXPIRE_TS) {
            output.put_i64(
                row.expire_ts
                    .expect("expire_ts should be set with HAS_EXPIRE_TS"),
            );
        }
        if flags.contains(RowFlags::HAS_CREATE_TS) {
            output.put_i64(
                row.create_ts
                    .expect("create_ts should be set with HAS_CREATE_TS"),
            );
        }

        // skip encoding value for tombstone
        if flags.contains(RowFlags::TOMBSTONE) {
            return;
        }

        // encode value
        let val = row
            .value
            .as_option()
            .expect("value is not set with no tombstone");
        output.put_u32(val.len() as u32);
        output.put(val.as_ref());
    }

    pub fn decode(&self, data: &mut Bytes) -> Result<SstRowEntry, SlateDBError> {
        let key_prefix_len = data.get_u16() as usize;
        let key_suffix_len = data.get_u16() as usize;
        let key_suffix = data.slice(..key_suffix_len);
        data.advance(key_suffix_len);

        // decode seq & flags
        let seq = data.get_u64();
        let flags = RowFlags::from_bits(data.get_u8()).ok_or(SlateDBError::InvalidRowFlags)?;

        // decode expire_ts & create_ts
        let (expire_ts, create_ts) =
            if flags.contains(RowFlags::HAS_EXPIRE_TS | RowFlags::HAS_CREATE_TS) {
                (Some(data.get_i64()), Some(data.get_i64()))
            } else if flags.contains(RowFlags::HAS_EXPIRE_TS) {
                (Some(data.get_i64()), None)
            } else if flags.contains(RowFlags::HAS_CREATE_TS) {
                (None, Some(data.get_i64()))
            } else {
                (None, None)
            };

        // skip decoding value for tombstone.
        if flags.contains(RowFlags::TOMBSTONE) {
            return Ok(SstRowEntry {
                key_prefix_len,
                key_suffix,
                seq,
                expire_ts: None, // it does not make sense to have expire_ts for tombstone
                create_ts,
                value: ValueDeletable::Tombstone,
            });
        }

        // decode value
        let value_len = data.get_u32() as usize;
        let value = data.slice(..value_len);
        Ok(SstRowEntry {
            key_prefix_len,
            key_suffix,
            seq,
            expire_ts,
            create_ts,
            value: ValueDeletable::Value(value),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ValueDeletable;

    #[test]
    fn test_encode_decode_normal_row() {
        let mut encoded_data = Vec::new();
        let key_prefix_len = 3;
        let key_suffix = b"key";
        let value = Some(b"value".as_slice());

        // Encode the row
        let codec = SstRowCodecV0 {};
        codec.encode(
            &mut encoded_data,
            &SstRowEntry::new(
                key_prefix_len,
                Bytes::from(key_suffix.to_vec()),
                1,
                ValueDeletable::Value(Bytes::from(value.unwrap().to_vec())),
                None,
                Some(10),
            ),
        );

        let first_key = Bytes::from(b"prefixdata".as_ref());
        let mut data = Bytes::from(encoded_data);

        let decoded = codec.decode(&mut data).expect("decoding failed");

        // Expected key: first_key[..3] + "key" = "prekey"
        let expected_key = Bytes::from(b"prekey" as &[u8]);
        let expected_value = ValueDeletable::Value(Bytes::from(b"value" as &[u8]));

        assert_eq!(decoded.restore_full_key(&first_key), &expected_key);
        assert_eq!(decoded.value, expected_value);
        assert_eq!(decoded.create_ts, None);
        assert_eq!(decoded.expire_ts, Some(10));
    }

    #[test]
    fn test_encode_decode_normal_row_no_expire_ts() {
        let mut encoded_data = Vec::new();
        let key_prefix_len = 3;
        let key_suffix = b"key";
        let value = Some(b"value".as_slice());

        // Encode the row
        let codec = SstRowCodecV0 {};
        codec.encode(
            &mut encoded_data,
            &SstRowEntry::new(
                key_prefix_len,
                Bytes::from(key_suffix.to_vec()),
                1,
                ValueDeletable::Value(Bytes::from(value.unwrap().to_vec())),
                None,
                None,
            ),
        );

        let mut data = Bytes::from(encoded_data);
        let decoded = codec.decode(&mut data).expect("decoding failed");

        assert_eq!(decoded.expire_ts, None);
    }

    #[test]
    fn test_encode_decode_tombstone_row() {
        let mut encoded_data = Vec::new();
        let key_prefix_len = 4;
        let key_suffix = b"tomb";

        // Encode the row
        let codec = SstRowCodecV0 {};
        codec.encode(
            &mut encoded_data,
            &SstRowEntry::new(
                key_prefix_len,
                Bytes::from(key_suffix.to_vec()),
                1,
                ValueDeletable::Tombstone,
                Some(2),
                Some(1),
            ),
        );

        let first_key = Bytes::from(b"deadbeefdata".as_ref());
        let mut data = Bytes::from(encoded_data);
        let decoded = codec.decode(&mut data).expect("decoding failed");

        // Expected key: first_key[..4] + "tomb" = "deadtomb"
        let expected_key = Bytes::from(b"deadtomb" as &[u8]);
        let expected_value = ValueDeletable::Tombstone;

        assert_eq!(decoded.restore_full_key(&first_key), &expected_key);
        assert_eq!(decoded.value, expected_value);
        assert_eq!(decoded.expire_ts, None);
        assert_eq!(decoded.create_ts, Some(2));
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

        let mut data = Bytes::from(encoded_data);

        // Attempt to decode the row
        let codec = SstRowCodecV0 {};
        let result = codec.decode(&mut data);

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
        let codec = SstRowCodecV0 {};
        codec.encode(
            &mut encoded_data,
            &SstRowEntry::new(
                key_prefix_len,
                Bytes::from(key_suffix.to_vec()),
                1,
                ValueDeletable::Value(Bytes::from(b"value".to_vec())),
                None,
                None,
            ),
        );

        let first_key = Bytes::from(b"keyprefixdata".as_slice());
        let mut data = Bytes::from(encoded_data);
        let decoded = codec.decode(&mut data).expect("decoding failed");

        // Expected key: first_key[..4] + "" = "keyp"
        let expected_key = Bytes::from(b"keyp" as &[u8]);
        let expected_value = ValueDeletable::Value(Bytes::from(b"value" as &[u8]));

        assert_eq!(decoded.restore_full_key(&first_key), &expected_key);
        assert_eq!(decoded.value, expected_value);
    }
}
