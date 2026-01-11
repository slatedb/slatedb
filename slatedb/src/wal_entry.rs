use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::SlateDBError;
use crate::types::{RowEntry, ValueDeletable};

const RECORD_TYPE_VALUE_FLAGS: u8 = 0b00;
const RECORD_TYPE_TOMBSTONE_FLAGS: u8 = 0b01;
const RECORD_TYPE_MERGE_FLAGS: u8 = 0b10;
const RECORD_TYPE_MASK: u8 = 0b11;

const FLAG_HAS_CREATE_TS_FLAG: u8 = 0b0100; // bit 2
const FLAG_HAS_EXPIRE_TS_FLAG: u8 = 0b1000; // bit 3

const KNOWN_FLAGS: u8 = RECORD_TYPE_MASK | FLAG_HAS_EXPIRE_TS_FLAG | FLAG_HAS_CREATE_TS_FLAG;

/// A WAL entry represents a record that is written to the WAL.
///
/// A WAL entry has the following byte format:
///
/// +----------------------------------------------------------------+
/// | sequence number (8-bytes unsigned integer, little endian)      |
/// +----------------------------------------------------------------+
/// | flags (1-byte unsigned integer, little endian)                 |
/// +----------------------------------------------------------------+
/// | create_ts (8-bytes signed integer, little endian)              |
/// +----------------------------------------------------------------+
/// | expire_ts (8-bytes signed integer, little endian)              |
/// +----------------------------------------------------------------+
/// | key length (2-bytes unsigned integer, little endian)           |
/// +----------------------------------------------------------------+
/// | key (variable length)                                          |
/// +----------------------------------------------------------------+
/// | value length (4-bytes unsigned integer, little endian)         |
/// +----------------------------------------------------------------+
/// | value (variable length)                                        |
/// +----------------------------------------------------------------+
///
/// [[WalEntry]] provides methods to transform a [[RowEntry]] to a [[WalEntry]] and back.
///
#[derive(Clone, Debug)]
pub(crate) struct WalEntry {
    bytes: Bytes,
}


impl From<RowEntry> for WalEntry {
    fn from(row_entry: RowEntry) -> Self {
        let seqnum_size = size_of::<u64>();
        let flags_size = size_of::<u8>();
        let ts_size = size_of::<i64>();
        let key_len_size = size_of::<u16>();
        let value_len_size = size_of::<u32>();
        let mut size = seqnum_size + flags_size + key_len_size + row_entry.key.len();
        if row_entry.create_ts.is_some() {
            size += ts_size;
        }
        if row_entry.expire_ts.is_some() {
            size += ts_size;
        }
        if !row_entry.value.is_tombstone() {
            size += value_len_size + row_entry.value.len();
        }

        let mut buf = BytesMut::with_capacity(size);

        let record_type = match &row_entry.value {
            ValueDeletable::Value(_) => RECORD_TYPE_VALUE_FLAGS,
            ValueDeletable::Tombstone => RECORD_TYPE_TOMBSTONE_FLAGS,
            ValueDeletable::Merge(_) => RECORD_TYPE_MERGE_FLAGS,
        };
        let mut flags = record_type;
        if row_entry.expire_ts.is_some() {
            flags |= FLAG_HAS_EXPIRE_TS_FLAG;
        }
        if row_entry.create_ts.is_some() {
            flags |= FLAG_HAS_CREATE_TS_FLAG;
        }

        buf.put_u64_le(row_entry.seq);
        buf.put_u8(flags);
        if let Some(create_ts) = row_entry.create_ts {
            buf.put_i64_le(create_ts);
        }
        if let Some(expire_ts) = row_entry.expire_ts {
            buf.put_i64_le(expire_ts);
        }
        buf.put_u16_le(u16::try_from(row_entry.key.len()).expect("key length should be less than u16::MAX_LEN"));
        buf.put_slice(&row_entry.key);
        if let Some(value_bytes) = row_entry.value.as_bytes() {
            buf.put_u32_le(u32::try_from(row_entry.value.len()).expect("value length should be less than u32::MAX_LEN"));
            buf.put_slice(&value_bytes);
        }

        WalEntry {
            bytes: buf.freeze(),
        }
    }
}

impl TryFrom<WalEntry> for RowEntry {
    type Error = SlateDBError;

    fn try_from(wal_entry: WalEntry) -> Result<RowEntry, SlateDBError> {
        let mut buf = wal_entry.bytes;
        let seq = buf.get_u64_le();
        let flags = buf.get_u8();
        let record_type = flags & RECORD_TYPE_MASK;
        let has_create_ts = flags & FLAG_HAS_CREATE_TS_FLAG != 0;
        let has_expire_ts = flags & FLAG_HAS_EXPIRE_TS_FLAG != 0;
        if flags & !KNOWN_FLAGS != 0 {
            return Err(SlateDBError::InvalidRowFlags {
                encoded_bits: flags,
                known_bits: KNOWN_FLAGS,
                message: "Unknown flags in WAL entry. This may be caused by reading data encoded with a newer codec.".to_string(),
            });
        }
        let create_ts = if has_create_ts {
            Some(buf.get_i64_le())
        } else {
            None
        };
        let expire_ts = if has_expire_ts {
            Some(buf.get_i64_le())
        } else {
            None
        };
        let key_len = buf.get_u16_le() as usize;
        let key = buf.slice(0..key_len);
        buf.advance(key_len);
        let value = match record_type {
            RECORD_TYPE_TOMBSTONE_FLAGS => ValueDeletable::Tombstone,
            RECORD_TYPE_VALUE_FLAGS => {
                let value_len = buf.get_u32_le() as usize;
                let value_bytes = buf.slice(0..value_len);
                buf.advance(value_len);
                ValueDeletable::Value(value_bytes)
            }
            RECORD_TYPE_MERGE_FLAGS => {
                let value_len = buf.get_u32_le() as usize;
                let value_bytes = buf.slice(0..value_len);
                buf.advance(value_len);
                ValueDeletable::Merge(value_bytes)
            }
            _ => {
                return Err(SlateDBError::InvalidRowFlags {
                    encoded_bits: flags,
                    known_bits: KNOWN_FLAGS,
                    message: format!(
                        "Invalid record type {} in WAL entry. This may be caused by reading data encoded with a newer codec.",
                        record_type
                    ),
                });
            }
        };

        Ok(RowEntry::new(key, value, seq, create_ts, expire_ts))
    }
}

impl From<Bytes> for WalEntry {
    fn from(bytes: Bytes) -> Self {
        WalEntry { bytes }
    }
}

impl From<WalEntry> for Bytes {
    fn from(wal_entry: WalEntry) -> Bytes {
        wal_entry.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_value_entry() {
        let key = "key";
        let value = "value";
        let seqnum = 42u64;
        let entry = RowEntry::new(
            Bytes::from(key),
            ValueDeletable::Value(Bytes::from(value)),
            seqnum,
            None,
            None,
        );

        let wal_entry = WalEntry::from(entry.clone());
        let bytes: Bytes = wal_entry.clone().into();

        assert_eq!(
            bytes.len(),
            size_of::<u64>()
                + size_of::<u8>()
                + size_of::<u16>()
                + key.len()
                + size_of::<u32>()
                + value.len()
        );
        let mut buf = bytes.clone();
        assert_eq!(buf.get_u64_le(), entry.seq);
        assert_eq!(buf.get_u8(), RECORD_TYPE_VALUE_FLAGS);
        assert_eq!(buf.get_u16_le(), key.len() as u16);
        assert_eq!(&buf.slice(0..key.len())[..], key.as_bytes());
        buf.advance(key.len());
        assert_eq!(buf.get_u32_le(), value.len() as u32);
        assert_eq!(&buf.slice(0..value.len())[..], value.as_bytes());
        buf.advance(value.len());

        let result: RowEntry = wal_entry.try_into().unwrap();
        assert_eq!(result, entry);
    }

    #[test]
    fn should_encode_and_decode_tombstone_entry() {
        let key = "key";
        let seqnum = 123u64;
        let entry = RowEntry::new(Bytes::from(key), ValueDeletable::Tombstone, seqnum, None, None);

        let wal_entry = WalEntry::from(entry.clone());
        let bytes: Bytes = wal_entry.clone().into();

        assert_eq!(
            bytes.len(),
            size_of::<u64>()
                + size_of::<u8>()
                + size_of::<u16>()
                + key.len()
        );
        let mut buf = bytes.clone();
        assert_eq!(buf.get_u64_le(), seqnum);
        assert_eq!(buf.get_u8(), RECORD_TYPE_TOMBSTONE_FLAGS);
        assert_eq!(buf.get_u16_le(), key.len() as u16);
        assert_eq!(&buf.slice(0..key.len())[..], key.as_bytes());
        buf.advance(key.len());
        assert!(buf.is_empty());

        let result: RowEntry = wal_entry.try_into().unwrap();
        assert_eq!(result, entry);
    }

    #[test]
    fn should_encode_and_decode_merge_entry() {
        let key = "key";
        let operand = "operand";
        let seqnum = 456u64;
        let entry = RowEntry::new(
            Bytes::from(key),
            ValueDeletable::Merge(Bytes::from(operand)),
            seqnum,
            None,
            None,
        );

        let wal_entry = WalEntry::from(entry.clone());
        let bytes: Bytes = wal_entry.clone().into();

        assert_eq!(
            bytes.len(),
            size_of::<u64>()
                + size_of::<u8>()
                + size_of::<u16>()
                + key.len()
                + size_of::<u32>()
                + operand.len()
        );
        let mut buf = bytes.clone();
        assert_eq!(buf.get_u64_le(), seqnum);
        assert_eq!(buf.get_u8(), RECORD_TYPE_MERGE_FLAGS);
        assert_eq!(buf.get_u16_le(), key.len() as u16);
        assert_eq!(&buf.slice(0..key.len())[..], key.as_bytes());
        buf.advance(key.len());
        assert_eq!(buf.get_u32_le(), operand.len() as u32);
        assert_eq!(&buf.slice(0..operand.len())[..], operand.as_bytes());
        buf.advance(operand.len());

        let result: RowEntry = wal_entry.try_into().unwrap();
        assert_eq!(result, entry);
    }

    #[test]
    fn should_encode_and_decode_entry_with_expire_ts() {
        let key = "key";
        let value = "value";
        let seqnum = 1u64;
        let expire_ts = 1234567890;
        let entry = RowEntry::new(
            Bytes::from(key),
            ValueDeletable::Value(Bytes::from(value)),
            seqnum,
            None,
            Some(expire_ts),
        );

        let wal_entry = WalEntry::from(entry.clone());
        let bytes: Bytes = wal_entry.clone().into();

        assert_eq!(
            bytes.len(),
            size_of::<u64>()
                + size_of::<u8>()
                + size_of::<i64>()
                + size_of::<u16>()
                + key.len()
                + size_of::<u32>()
                + value.len()
        );
        let mut buf = bytes.clone();
        assert_eq!(buf.get_u64_le(), seqnum);
        assert_eq!(buf.get_u8(), RECORD_TYPE_VALUE_FLAGS | FLAG_HAS_EXPIRE_TS_FLAG);
        assert_eq!(buf.get_i64_le(), expire_ts);
        assert_eq!(buf.get_u16_le(), key.len() as u16);
        assert_eq!(&buf.slice(0..key.len())[..], key.as_bytes());
        buf.advance(key.len());
        assert_eq!(buf.get_u32_le(), value.len() as u32);
        assert_eq!(&buf.slice(0..value.len())[..], value.as_bytes());
        buf.advance(value.len());

        let result: RowEntry = wal_entry.try_into().unwrap();
        assert_eq!(result, entry);
    }

    #[test]
    fn should_encode_and_decode_entry_with_create_ts() {
        let key = "key";
        let value = "value";
        let seqnum = 1u64;
        let create_ts = 9876543210;
        let entry = RowEntry::new(
            Bytes::from(key),
            ValueDeletable::Value(Bytes::from(value)),
            seqnum,
            Some(create_ts),
            None,
        );

        let wal_entry = WalEntry::from(entry.clone());
        let bytes: Bytes = wal_entry.clone().into();

        assert_eq!(
            bytes.len(),
            size_of::<u64>()
                + size_of::<u8>()
                + size_of::<i64>()
                + size_of::<u16>()
                + key.len()
                + size_of::<u32>()
                + value.len()
        );
        let mut buf = bytes.clone();
        assert_eq!(buf.get_u64_le(), seqnum);
        assert_eq!(buf.get_u8(), RECORD_TYPE_VALUE_FLAGS | FLAG_HAS_CREATE_TS_FLAG);
        assert_eq!(buf.get_i64_le(), create_ts);
        assert_eq!(buf.get_u16_le(), key.len() as u16);
        assert_eq!(&buf.slice(0..key.len())[..], key.as_bytes());
        buf.advance(key.len());
        assert_eq!(buf.get_u32_le(), value.len() as u32);
        assert_eq!(&buf.slice(0..value.len())[..], value.as_bytes());
        buf.advance(value.len());

        let result: RowEntry = wal_entry.try_into().unwrap();
        assert_eq!(result, entry);
    }

    #[test]
    fn should_encode_and_decode_entry_with_both_timestamps() {
        let key = "key";
        let value = "value";
        let seqnum = 999u64;
        let create_ts = 1111111111;
        let expire_ts = 2222222222;
        let entry = RowEntry::new(
            Bytes::from(key),
            ValueDeletable::Value(Bytes::from(value)),
            seqnum,
            Some(create_ts),
            Some(expire_ts),
        );

        let wal_entry = WalEntry::from(entry.clone());
        let bytes: Bytes = wal_entry.clone().into();

        assert_eq!(
            bytes.len(),
            size_of::<u64>()
                + size_of::<u8>()
                + size_of::<i64>()
                + size_of::<i64>()
                + size_of::<u16>()
                + key.len()
                + size_of::<u32>()
                + value.len()
        );
        let mut buf = bytes.clone();
        assert_eq!(buf.get_u64_le(), seqnum);
        assert_eq!(
            buf.get_u8(),
            RECORD_TYPE_VALUE_FLAGS | FLAG_HAS_CREATE_TS_FLAG | FLAG_HAS_EXPIRE_TS_FLAG
        );
        assert_eq!(buf.get_i64_le(), create_ts);
        assert_eq!(buf.get_i64_le(), expire_ts);
        assert_eq!(buf.get_u16_le(), key.len() as u16);
        assert_eq!(&buf.slice(0..key.len())[..], key.as_bytes());
        buf.advance(key.len());
        assert_eq!(buf.get_u32_le(), value.len() as u32);
        assert_eq!(&buf.slice(0..value.len())[..], value.as_bytes());
        buf.advance(value.len());

        let result: RowEntry = wal_entry.try_into().unwrap();
        assert_eq!(result, entry);
    }

    #[test]
    fn should_encode_and_decode_tombstone_with_timestamps() {
        let key = "deleted_key";
        let seqnum = 50u64;
        let create_ts = 3333333333;
        let expire_ts = 4444444444;
        let entry = RowEntry::new(
            Bytes::from(key),
            ValueDeletable::Tombstone,
            seqnum,
            Some(create_ts),
            Some(expire_ts),
        );

        let wal_entry = WalEntry::from(entry.clone());
        let bytes: Bytes = wal_entry.clone().into();

        assert_eq!(
            bytes.len(),
            size_of::<u64>()
                + size_of::<u8>()
                + size_of::<i64>()
                + size_of::<i64>()
                + size_of::<u16>()
                + key.len()
        );
        let mut buf = bytes.clone();
        assert_eq!(buf.get_u64_le(), seqnum);
        assert_eq!(
            buf.get_u8(),
            RECORD_TYPE_TOMBSTONE_FLAGS | FLAG_HAS_CREATE_TS_FLAG | FLAG_HAS_EXPIRE_TS_FLAG
        );
        assert_eq!(buf.get_i64_le(), create_ts);
        assert_eq!(buf.get_i64_le(), expire_ts);
        assert_eq!(buf.get_u16_le(), key.len() as u16);
        assert_eq!(&buf.slice(0..key.len())[..], key.as_bytes());
        buf.advance(key.len());
        assert!(buf.is_empty());

        let result: RowEntry = wal_entry.try_into().unwrap();
        assert_eq!(result, entry);
    }

    #[test]
    fn should_convert_to_bytes_and_back() {
        let key = "key";
        let value = "value";
        let seqnum = 100u64;
        let create_ts = 5555555555;
        let expire_ts = 6666666666;
        let entry = RowEntry::new(
            Bytes::from(key),
            ValueDeletable::Value(Bytes::from(value)),
            seqnum,
            Some(create_ts),
            Some(expire_ts),
        );

        let wal_entry = WalEntry::from(entry.clone());
        let bytes: Bytes = wal_entry.clone().into();

        assert_eq!(
            bytes.len(),
            size_of::<u64>()
                + size_of::<u8>()
                + size_of::<i64>()
                + size_of::<i64>()
                + size_of::<u16>()
                + key.len()
                + size_of::<u32>()
                + value.len()
        );
        let mut buf = bytes.clone();
        assert_eq!(buf.get_u64_le(), seqnum);
        assert_eq!(
            buf.get_u8(),
            RECORD_TYPE_VALUE_FLAGS | FLAG_HAS_CREATE_TS_FLAG | FLAG_HAS_EXPIRE_TS_FLAG
        );
        assert_eq!(buf.get_i64_le(), create_ts);
        assert_eq!(buf.get_i64_le(), expire_ts);
        assert_eq!(buf.get_u16_le(), key.len() as u16);
        assert_eq!(&buf.slice(0..key.len()), key.as_bytes());
        buf.advance(key.len());
        assert_eq!(buf.get_u32_le(), value.len() as u32);
        assert_eq!(&buf.slice(0..value.len())[..], value.as_bytes());
        buf.advance(value.len());

        let wal_entry2 = WalEntry::from(bytes);
        let result: RowEntry = wal_entry2.try_into().unwrap();
        assert_eq!(result, entry);
    }

    #[test]
    fn should_fail_on_invalid_record_type() {
        let key = "key";
        let seqnum = 1u64;
        let invalid_record_type_flags = 0b11;
        let mut buf = BytesMut::new();
        buf.put_u64_le(seqnum);
        buf.put_u8(invalid_record_type_flags);
        buf.put_u16_le(key.len() as u16);
        buf.put_slice(key.as_bytes());

        let wal_entry = WalEntry::from(Bytes::from(buf));
        let result: Result<RowEntry, _> = wal_entry.try_into();

        assert!(matches!(result, Err(SlateDBError::InvalidRowFlags { .. })));
    }

    #[test]
    fn should_fail_on_unknown_flags() {
        let key = "key";
        let seqnum = 1u64;
        let unknown_flag = 0b10000;
        let mut buf = BytesMut::new();
        buf.put_u64_le(seqnum);
        buf.put_u8(unknown_flag);
        buf.put_u16_le(key.len() as u16);
        buf.put_slice(key.as_bytes());

        let wal_entry = WalEntry::from(Bytes::from(buf));
        let result: Result<RowEntry, _> = wal_entry.try_into();

        assert!(matches!(result, Err(SlateDBError::InvalidRowFlags { .. })));
    }
}
