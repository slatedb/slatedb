use crate::{RowEntry, ValueDeletable};
use bytes::Bytes;

const MAGIC: &[u8; 4] = b"SKWL";
const VERSION: u8 = 1;
const FENCE_RECORD: u8 = 0;
const WRITE_BATCH_RECORD: u8 = 1;

const VALUE: u8 = 0;
const MERGE: u8 = 1;
const TOMBSTONE: u8 = 2;
const VALUE_KIND_MASK: u8 = 0b0000_0011;
const HAS_CREATE_TS: u8 = 0b0000_0100;
const HAS_EXPIRE_TS: u8 = 0b0000_1000;
const VALID_FLAGS: u8 = VALUE_KIND_MASK | HAS_CREATE_TS | HAS_EXPIRE_TS;

/// The decoded contents of one Kafka WAL record.
#[derive(Debug, PartialEq)]
pub(super) enum DecodedRecord {
    Fence { epoch: u64 },
    WriteBatch { epoch: u64, rows: Vec<RowEntry> },
}

#[derive(Debug, thiserror::Error)]
pub(super) enum CodecError {
    #[error("Kafka WAL record is truncated")]
    Truncated,
    #[error("Kafka WAL record has an invalid magic value")]
    InvalidMagic,
    #[error("unsupported Kafka WAL record version {0}")]
    UnsupportedVersion(u8),
    #[error("unknown Kafka WAL record type {0}")]
    UnknownRecordType(u8),
    #[error("invalid Kafka WAL row flags {0:#010b}")]
    InvalidRowFlags(u8),
    #[error("Kafka WAL record has trailing bytes")]
    TrailingBytes,
    #[error("Kafka WAL {field} length exceeds the binary format limit")]
    LengthOverflow { field: &'static str },
}

/// Binary record layout (all integers are big-endian):
///
/// ```text
/// magic[4] | version:u8 | kind:u8 | epoch:u64
///
/// fence:       (no body)
/// write batch: row_count:u32 | row...
/// row:          seq:u64 | flags:u8 | [create_ts:i64] | [expire_ts:i64]
///               | key_len:u32 | key | [value_len:u32 | value]
/// ```
///
/// The low two flag bits encode value, merge operand, or tombstone. Timestamp
/// presence is encoded in the next two bits. Tombstones omit the value length.
pub(super) fn encode_fence(epoch: u64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(14);
    encode_header(&mut encoded, FENCE_RECORD, epoch);
    encoded
}

pub(super) fn encode_write_batch(epoch: u64, rows: &[RowEntry]) -> Result<Vec<u8>, CodecError> {
    let row_count =
        u32::try_from(rows.len()).map_err(|_| CodecError::LengthOverflow { field: "row count" })?;
    let estimated_size = rows.iter().fold(18usize, |size, row| {
        size.saturating_add(
            row.key
                .len()
                .saturating_add(row.value.len())
                .saturating_add(33),
        )
    });
    let mut encoded = Vec::with_capacity(estimated_size);
    encode_header(&mut encoded, WRITE_BATCH_RECORD, epoch);
    put_u32(&mut encoded, row_count);

    for row in rows {
        let key_len = u32::try_from(row.key.len())
            .map_err(|_| CodecError::LengthOverflow { field: "key" })?;
        let (value_kind, value) = match &row.value {
            ValueDeletable::Value(value) => (VALUE, Some(value)),
            ValueDeletable::Merge(value) => (MERGE, Some(value)),
            ValueDeletable::Tombstone => (TOMBSTONE, None),
        };

        put_u64(&mut encoded, row.seq);
        let mut flags = value_kind;
        if row.create_ts.is_some() {
            flags |= HAS_CREATE_TS;
        }
        if row.expire_ts.is_some() {
            flags |= HAS_EXPIRE_TS;
        }
        encoded.push(flags);
        if let Some(create_ts) = row.create_ts {
            put_i64(&mut encoded, create_ts);
        }
        if let Some(expire_ts) = row.expire_ts {
            put_i64(&mut encoded, expire_ts);
        }
        put_u32(&mut encoded, key_len);
        encoded.extend_from_slice(&row.key);
        if let Some(value) = value {
            let value_len = u32::try_from(value.len())
                .map_err(|_| CodecError::LengthOverflow { field: "value" })?;
            put_u32(&mut encoded, value_len);
            encoded.extend_from_slice(value);
        }
    }
    Ok(encoded)
}

pub(super) fn decode_record(encoded: &[u8]) -> Result<DecodedRecord, CodecError> {
    let mut decoder = Decoder::new(encoded);
    if decoder.take(MAGIC.len())? != MAGIC {
        return Err(CodecError::InvalidMagic);
    }
    let version = decoder.u8()?;
    if version != VERSION {
        return Err(CodecError::UnsupportedVersion(version));
    }
    let record_type = decoder.u8()?;
    let epoch = decoder.u64()?;

    let record = match record_type {
        FENCE_RECORD => DecodedRecord::Fence { epoch },
        WRITE_BATCH_RECORD => {
            let row_count = decoder.u32()? as usize;
            // Do not reserve directly from an untrusted row count. Every row
            // consumes at least thirteen bytes, so the payload bounds this hint.
            let capacity = row_count.min(decoder.remaining() / 13);
            let mut rows = Vec::with_capacity(capacity);
            for _ in 0..row_count {
                rows.push(decode_row(&mut decoder)?);
            }
            DecodedRecord::WriteBatch { epoch, rows }
        }
        other => return Err(CodecError::UnknownRecordType(other)),
    };
    if decoder.remaining() != 0 {
        return Err(CodecError::TrailingBytes);
    }
    Ok(record)
}

fn decode_row(decoder: &mut Decoder<'_>) -> Result<RowEntry, CodecError> {
    let seq = decoder.u64()?;
    let flags = decoder.u8()?;
    if flags & !VALID_FLAGS != 0 || flags & VALUE_KIND_MASK == VALUE_KIND_MASK {
        return Err(CodecError::InvalidRowFlags(flags));
    }
    let create_ts = if flags & HAS_CREATE_TS != 0 {
        Some(decoder.i64()?)
    } else {
        None
    };
    let expire_ts = if flags & HAS_EXPIRE_TS != 0 {
        Some(decoder.i64()?)
    } else {
        None
    };
    let key = Bytes::copy_from_slice(decoder.length_prefixed_bytes()?);
    let value = match flags & VALUE_KIND_MASK {
        VALUE => ValueDeletable::Value(Bytes::copy_from_slice(decoder.length_prefixed_bytes()?)),
        MERGE => ValueDeletable::Merge(Bytes::copy_from_slice(decoder.length_prefixed_bytes()?)),
        TOMBSTONE => ValueDeletable::Tombstone,
        _ => return Err(CodecError::InvalidRowFlags(flags)),
    };
    Ok(RowEntry::new(key, value, seq, create_ts, expire_ts))
}

fn encode_header(encoded: &mut Vec<u8>, record_type: u8, epoch: u64) {
    encoded.extend_from_slice(MAGIC);
    encoded.push(VERSION);
    encoded.push(record_type);
    put_u64(encoded, epoch);
}

fn put_u32(encoded: &mut Vec<u8>, value: u32) {
    encoded.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(encoded: &mut Vec<u8>, value: u64) {
    encoded.extend_from_slice(&value.to_be_bytes());
}

fn put_i64(encoded: &mut Vec<u8>, value: i64) {
    encoded.extend_from_slice(&value.to_be_bytes());
}

struct Decoder<'a> {
    encoded: &'a [u8],
    position: usize,
}

impl<'a> Decoder<'a> {
    fn new(encoded: &'a [u8]) -> Self {
        Self {
            encoded,
            position: 0,
        }
    }

    fn remaining(&self) -> usize {
        self.encoded.len() - self.position
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], CodecError> {
        let end = self
            .position
            .checked_add(len)
            .filter(|end| *end <= self.encoded.len())
            .ok_or(CodecError::Truncated)?;
        let bytes = &self.encoded[self.position..end];
        self.position = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        let bytes: [u8; 4] = self.take(4)?.try_into().expect("length checked");
        Ok(u32::from_be_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64, CodecError> {
        let bytes: [u8; 8] = self.take(8)?.try_into().expect("length checked");
        Ok(u64::from_be_bytes(bytes))
    }

    fn i64(&mut self) -> Result<i64, CodecError> {
        let bytes: [u8; 8] = self.take(8)?.try_into().expect("length checked");
        Ok(i64::from_be_bytes(bytes))
    }

    fn length_prefixed_bytes(&mut self) -> Result<&'a [u8], CodecError> {
        let len = self.u32()? as usize;
        self.take(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_fence() {
        assert_eq!(
            decode_record(&encode_fence(42)).unwrap(),
            DecodedRecord::Fence { epoch: 42 }
        );
    }

    #[test]
    fn round_trips_write_batch() {
        let rows = vec![
            RowEntry::new(
                Bytes::from_static(b"value-key"),
                ValueDeletable::Value(Bytes::from_static(b"value")),
                7,
                Some(-12),
                Some(1234),
            ),
            RowEntry::new(
                Bytes::from_static(b"merge-key"),
                ValueDeletable::Merge(Bytes::from_static(b"operand")),
                7,
                None,
                Some(999),
            ),
            RowEntry::new(
                Bytes::from_static(b"deleted-key"),
                ValueDeletable::Tombstone,
                7,
                Some(1),
                None,
            ),
        ];

        let decoded = decode_record(&encode_write_batch(11, &rows).unwrap()).unwrap();
        assert_eq!(decoded, DecodedRecord::WriteBatch { epoch: 11, rows });
    }

    #[test]
    fn rejects_truncated_and_trailing_records() {
        let mut encoded = encode_write_batch(1, &[RowEntry::new_value(b"k", b"v", 2)]).unwrap();
        assert!(matches!(
            decode_record(&encoded[..encoded.len() - 1]),
            Err(CodecError::Truncated)
        ));

        encoded.push(0);
        assert!(matches!(
            decode_record(&encoded),
            Err(CodecError::TrailingBytes)
        ));
    }
}
