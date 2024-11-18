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

    pub fn flags(&self) -> RowFlags {
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

    pub fn size(&self) -> usize {
        let mut size = 2  // u16 key_prefix_len
        + 2 // u16 key_suffix_len
        + self.key_suffix.len() // key_suffix
        + 8  // u64 seq
        + 1; // u8 flags
        if self.expire_ts.is_some() {
            size += 8; // i64 expire_ts
        }
        if self.create_ts.is_some() {
            size += 8; // i64 create_ts
        }
        if let Some(value) = self.value.as_option() {
            size += 4; // u32 value_len
            size += value.len(); // value
        }
        size
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
    fn test_encode_decode() {
        struct TestCase {
            name: &'static str,
            key_prefix_len: usize,
            key_suffix: Vec<u8>,
            seq: u64,
            value: Option<Vec<u8>>,
            create_ts: Option<i64>,
            expire_ts: Option<i64>,
            first_key: Vec<u8>,
            expected_key: Vec<u8>,
            expected_encoded: &'static [u8],
        }

        let test_cases = vec![
            TestCase {
                name: "normal row with expire_ts",
                key_prefix_len: 3,
                key_suffix: b"key".to_vec(),
                seq: 1,
                value: Some(b"value".to_vec()),
                create_ts: None,
                expire_ts: Some(10),
                first_key: b"prefixdata".to_vec(),
                expected_key: b"prekey".to_vec(),
                expected_encoded: b"\x00\x03\x00\x03\x6B\x65\x79\x00\x00\x00\x00\x00\x00\x00\x01\x02\x00\x00\x00\x00\x00\x00\x00\x0A\x00\x00\x00\x05\x76\x61\x6C\x75\x65",
            },
            TestCase {
                name: "normal row without expire_ts", 
                key_prefix_len: 3,
                key_suffix: b"key".to_vec(),
                seq: 1,
                value: Some(b"value".to_vec()),
                create_ts: None,
                expire_ts: None,
                first_key: b"".to_vec(),    // Not used in assertions
                expected_key: b"".to_vec(), // Not used in assertions
                expected_encoded: b"\x00\x03\x00\x03\x6b\x65\x79\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x05\x76\x61\x6c\x75\x65",
            },
            TestCase {
                name: "row with both timestamps",
                key_prefix_len: 5,
                key_suffix: b"both".to_vec(),
                seq: 100,
                value: Some(b"value".to_vec()),
                create_ts: Some(1234567890),
                expire_ts: Some(9876543210),
                first_key: b"test_both".to_vec(),
                expected_key: b"test_both".to_vec(),
                expected_encoded: b"\x00\x05\x00\x04\x62\x6F\x74\x68\x00\x00\x00\x00\x00\x00\x00\x64\x06\x00\x00\x00\x02\x4C\xB0\x16\xEA\x00\x00\x00\x00\x49\x96\x02\xD2\x00\x00\x00\x05\x76\x61\x6C\x75\x65",
            },
            TestCase {
                name: "row with only create_ts",
                key_prefix_len: 4,
                key_suffix: b"create".to_vec(),
                seq: 50,
                value: Some(b"test_value".to_vec()),
                create_ts: Some(1234567890),
                expire_ts: None,
                first_key: b"timecreate".to_vec(),
                expected_key: b"timecreate".to_vec(),
                expected_encoded: b"\x00\x04\x00\x06\x63\x72\x65\x61\x74\x65\x00\x00\x00\x00\x00\x00\x00\x32\x04\x00\x00\x00\x00\x49\x96\x02\xD2\x00\x00\x00\x0A\x74\x65\x73\x74\x5F\x76\x61\x6C\x75\x65",
            },
            TestCase {
                name: "tombstone row",
                key_prefix_len: 4,
                key_suffix: b"tomb".to_vec(),
                seq: 1,
                value: None,
                create_ts: Some(2),
                expire_ts: Some(1), // Will be ignored for tombstone
                first_key: b"deadbeefdata".to_vec(),
                expected_key: b"deadtomb".to_vec(),
                expected_encoded: b"\x00\x04\x00\x04\x74\x6F\x6D\x62\x00\x00\x00\x00\x00\x00\x00\x01\x07\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02",
            },
            TestCase {
                name: "empty key suffix",
                key_prefix_len: 4,
                key_suffix: b"".to_vec(),
                seq: 1,
                value: Some(b"value".to_vec()),
                create_ts: None,
                expire_ts: None,
                first_key: b"keyprefixdata".to_vec(),
                expected_key: b"keyp".to_vec(),
                expected_encoded: b"\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x05\x76\x61\x6c\x75\x65",
            },
            TestCase {
                name: "large sequence number",
                key_prefix_len: 3,
                key_suffix: b"seq".to_vec(),
                seq: u64::MAX,
                value: Some(b"value".to_vec()),
                create_ts: None,
                expire_ts: None,
                first_key: b"bigseq".to_vec(),
                expected_key: b"bigseq".to_vec(),
                expected_encoded: b"\x00\x03\x00\x03seq\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x05value",
            },
            TestCase {
                name: "large value",
                key_prefix_len: 2,
                key_suffix: b"big".to_vec(),
                seq: 1,
                value: Some(vec![b'x'; 100]),
                create_ts: None,
                expire_ts: None,
                first_key: b"bigvalue".to_vec(),
                expected_key: b"bibig".to_vec(),
                expected_encoded: b"\x00\x02\x00\x03\x62\x69\x67\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x64\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78\x78",
            },
            TestCase {
                name: "long key suffix",
                key_prefix_len: 2,
                key_suffix: vec![b'k'; 100],
                seq: 1,
                value: Some(b"value".to_vec()),
                create_ts: None,
                expire_ts: None,
                first_key: b"longkey".to_vec(),
                expected_key: b"lokkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk".to_vec(),
                expected_encoded: b"\x00\x02\x00\x64\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x6b\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x05\x76\x61\x6c\x75\x65",
            },
            TestCase {
                name: "unicode key suffix",
                key_prefix_len: 3,
                key_suffix: "你好世界".as_bytes().to_vec(),
                seq: 1,
                value: Some(b"value".to_vec()),
                create_ts: None,
                expire_ts: None,
                first_key: b"unicode".to_vec(),
                expected_key: b"uni\xe4\xbd\xa0\xe5\xa5\xbd\xe4\xb8\x96\xe7\x95\x8c".to_vec(),
                expected_encoded: b"\x00\x03\x00\x0c\xe4\xbd\xa0\xe5\xa5\xbd\xe4\xb8\x96\xe7\x95\x8c\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x05\x76\x61\x6c\x75\x65",
            },
        ];

        for tc in test_cases {
            let mut encoded_data = Vec::new();
            let codec = SstRowCodecV0 {};

            // Encode the row
            let value = match tc.value {
                Some(v) => ValueDeletable::Value(Bytes::from(v)),
                None => ValueDeletable::Tombstone,
            };

            codec.encode(
                &mut encoded_data,
                &SstRowEntry::new(
                    tc.key_prefix_len,
                    Bytes::from(tc.key_suffix),
                    tc.seq,
                    value.clone(),
                    tc.create_ts,
                    tc.expire_ts,
                ),
            );

            let mut data = Bytes::from(encoded_data.clone());
            let decoded = codec.decode(&mut data).expect("decoding failed");

            // Run assertions
            if !tc.first_key.is_empty() {
                assert_eq!(
                    decoded.restore_full_key(&Bytes::from(tc.first_key)),
                    &Bytes::from(tc.expected_key),
                    "test case: {}",
                    tc.name
                );
            }
            assert_eq!(
                encoded_data,
                tc.expected_encoded,
                "test case: {} encoded: {:?}",
                tc.name,
                String::from_utf8_lossy(&encoded_data)
            );
            assert_eq!(decoded.value, value, "test case: {}", tc.name);

            match value {
                ValueDeletable::Tombstone => {
                    assert_eq!(decoded.expire_ts, None, "test case: {}", tc.name);
                    assert_eq!(decoded.create_ts, tc.create_ts, "test case: {}", tc.name);
                }
                ValueDeletable::Value(_) => {
                    assert_eq!(decoded.expire_ts, tc.expire_ts, "test case: {}", tc.name);
                    assert_eq!(decoded.create_ts, tc.create_ts, "test case: {}", tc.name);
                }
            }
        }
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
}
