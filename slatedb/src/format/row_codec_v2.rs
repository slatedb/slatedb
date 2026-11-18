use crate::error::SlateDBError;
use crate::format::row::RowFlags;
use crate::types::ValueDeletable;
use crate::utils::{decode_varint, encode_varint, varint_len};
use bytes::{Buf, BufMut, Bytes};

/// Intermediate representation for V2 row encoding.
///
/// The V2 codec encodes entries with prefix compression relative to the **previous key**
/// (not the first key as in V0/V1). At restart points, the full key is stored (shared_bytes=0).
///
/// ```text
/// |-------------------------------------------------------------------------------------------------------|
/// | shared    | unshared  | value_len | key_suffix | value   | seq  | flags | [expire_ts] | [create_ts] |
/// | (varint)  | (varint)  | (varint)  | (var)      | (var)   | u64  | u8    | i64         | i64         |
/// |-------------------------------------------------------------------------------------------------------|
/// ```
///
/// | Field       | Type    | Description                                                |
/// |-------------|---------|------------------------------------------------------------|
/// | shared      | varint  | Bytes shared with previous key (0 at restart points)      |
/// | unshared    | varint  | Length of key_suffix                                       |
/// | value_len   | varint  | Length of value (0 for tombstones)                         |
/// | key_suffix  | var     | The non-shared suffix of the key                           |
/// | value       | var     | Value bytes (omitted for tombstones)                       |
/// | seq         | u64     | Sequence number                                            |
/// | flags       | u8      | Row flags (tombstone, merge operand, timestamp presence)   |
/// | expire_ts   | i64     | Optional expiration timestamp (present if HAS_EXPIRE_TS)   |
/// | create_ts   | i64     | Optional creation timestamp (present if HAS_CREATE_TS)     |
///
/// Note: seq, expire_ts, and create_ts use fixed-width encoding (not varint) because:
/// - Sequence numbers are typically large (monotonically increasing counters)
/// - Timestamps are 64-bit values that rarely benefit from varint compression
/// - Fixed-width encoding simplifies parsing and provides predictable performance
#[derive(Debug, Clone)]
pub(crate) struct SstRowEntryV2 {
    /// Bytes shared with previous key (0 at restart points)
    pub shared_bytes: u32,
    /// Unshared portion of the key (the key suffix/delta)
    pub key_suffix: Bytes,
    pub seq: u64,
    pub expire_ts: Option<i64>,
    pub create_ts: Option<i64>,
    pub value: ValueDeletable,
}

impl SstRowEntryV2 {
    pub(crate) fn new(
        shared_bytes: u32,
        key_suffix: Bytes,
        seq: u64,
        value: ValueDeletable,
        create_ts: Option<i64>,
        expire_ts: Option<i64>,
    ) -> Self {
        Self {
            shared_bytes,
            key_suffix,
            seq,
            expire_ts,
            create_ts,
            value,
        }
    }

    pub(crate) fn flags(&self) -> RowFlags {
        let mut flags = match &self.value {
            ValueDeletable::Value(_) => RowFlags::default(),
            ValueDeletable::Merge(_) => RowFlags::MERGE_OPERAND,
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

    /// Calculate the encoded size of this entry including varint overhead.
    pub(crate) fn encoded_size(&self) -> usize {
        let shared_bytes_len = varint_len(self.shared_bytes);
        let unshared_bytes_len = varint_len(self.key_suffix.len() as u32);
        let value_len = match &self.value {
            ValueDeletable::Value(v) | ValueDeletable::Merge(v) => v.len(),
            ValueDeletable::Tombstone => 0,
        };
        let value_len_varint_size = varint_len(value_len as u32);

        shared_bytes_len              // shared_bytes varint
            + unshared_bytes_len      // unshared_bytes varint
            + value_len_varint_size   // value_len varint
            + self.key_suffix.len()   // key_delta
            + value_len               // value
            + tail_len(&self.flags()) // seq, flags, and timestamps
    }
}

/// The number of bytes after the value, including seq (8), flags (1), and
/// optional timestamps (8 each).
fn tail_len(flags: &RowFlags) -> usize {
    let mut len = 8 + 1;
    if flags.contains(RowFlags::HAS_EXPIRE_TS) {
        len += 8;
    }
    if flags.contains(RowFlags::HAS_CREATE_TS) {
        len += 8;
    }
    len
}

/// The header of a V2 row, i.e., the part of the row before the key suffix.
///
/// A seek reads many rows only to rebuild and compare their keys. The header
/// lets it rebuild each key and skip to the next row, without decoding the
/// whole row.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SstRowHeaderV2 {
    /// The number of leading bytes that the key shares with the key before it.
    pub shared: usize,
    /// The start of the key suffix.
    key_start: usize,
    /// The end of the key suffix, which is also the start of the value.
    key_end: usize,
    /// The end of the value.
    value_end: usize,
}

impl SstRowHeaderV2 {
    /// Decode the row header at `offset` in `data`, without copies or
    /// allocations.
    pub(crate) fn decode(data: &[u8], offset: usize) -> Self {
        let mut buf = &data[offset..];
        let shared = decode_varint(&mut buf) as usize;
        let unshared = decode_varint(&mut buf) as usize;
        let value_len = decode_varint(&mut buf) as usize;
        let key_start = data.len() - buf.len();
        let key_end = key_start + unshared;
        Self {
            shared,
            key_start,
            key_end,
            value_end: key_end + value_len,
        }
    }

    /// The key suffix of the row, as a slice of `data`.
    pub(crate) fn key_suffix<'a>(&self, data: &'a [u8]) -> &'a [u8] {
        &data[self.key_start..self.key_end]
    }

    /// Returns the offset where the next row starts. Reads only the flags
    /// byte after the value.
    pub(crate) fn next_row_offset(&self, data: &[u8]) -> Result<usize, SlateDBError> {
        let flags = RowFlags::try_from(data[self.value_end + 8])?;
        Ok(self.value_end + tail_len(&flags))
    }
}

/// The fields of a V2 row after the key.
#[derive(Debug, Clone)]
pub(crate) struct SstRowTailV2 {
    pub seq: u64,
    pub expire_ts: Option<i64>,
    pub create_ts: Option<i64>,
    pub value: ValueDeletable,
}

impl SstRowTailV2 {
    /// Decode the part of a row after the key.
    ///
    /// The value is a slice of `data`, not a copy. Returns the decoded fields
    /// and the offset of the next row.
    pub(crate) fn decode(
        data: &Bytes,
        header: &SstRowHeaderV2,
    ) -> Result<(Self, usize), SlateDBError> {
        let value = data.slice(header.key_end..header.value_end);
        let mut buf = &data[header.value_end..];
        let seq = buf.get_u64();
        let flags = RowFlags::try_from(buf.get_u8())?;
        let expire_ts = flags
            .contains(RowFlags::HAS_EXPIRE_TS)
            .then(|| buf.get_i64());
        let create_ts = flags
            .contains(RowFlags::HAS_CREATE_TS)
            .then(|| buf.get_i64());
        let value = if flags.contains(RowFlags::TOMBSTONE) {
            ValueDeletable::Tombstone
        } else if flags.contains(RowFlags::MERGE_OPERAND) {
            ValueDeletable::Merge(value)
        } else {
            ValueDeletable::Value(value)
        };
        let tail = Self {
            seq,
            expire_ts,
            create_ts,
            value,
        };
        Ok((tail, data.len() - buf.len()))
    }
}

pub(crate) struct SstRowCodecV2;

impl SstRowCodecV2 {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Encode a V2 row entry to the output buffer.
    pub(crate) fn encode(&self, output: &mut Vec<u8>, row: &SstRowEntryV2) {
        // Encode varints
        encode_varint(output, row.shared_bytes);
        encode_varint(output, row.key_suffix.len() as u32);

        let value_len = match &row.value {
            ValueDeletable::Value(v) | ValueDeletable::Merge(v) => v.len(),
            ValueDeletable::Tombstone => 0,
        };
        encode_varint(output, value_len as u32);

        // Encode key delta
        output.put(row.key_suffix.as_ref());

        // Encode value (if not tombstone)
        match &row.value {
            ValueDeletable::Value(v) | ValueDeletable::Merge(v) => {
                output.put(v.as_ref());
            }
            ValueDeletable::Tombstone => {
                // No value bytes for tombstones
            }
        }

        // Encode seq & flags
        let flags = row.flags();
        output.put_u64(row.seq);
        output.put_u8(flags.bits());

        // Encode expire & create timestamps
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use rstest::rstest;

    /// Helper to create ValueDeletable from a tag and optional data
    fn make_value(tag: &str, data: Option<Vec<u8>>) -> ValueDeletable {
        match tag {
            "value" => ValueDeletable::Value(Bytes::from(data.unwrap())),
            "tombstone" => ValueDeletable::Tombstone,
            "merge" => ValueDeletable::Merge(Bytes::from(data.unwrap())),
            _ => panic!("unknown value tag: {}", tag),
        }
    }

    /// Encodes `entry` as the only row in a buffer.
    fn encode(entry: &SstRowEntryV2) -> Bytes {
        let mut buf = Vec::new();
        SstRowCodecV2::new().encode(&mut buf, entry);
        Bytes::from(buf)
    }

    #[rstest]
    #[case("value entry", 3, b"suffix".to_vec(), 100, "value", Some(b"value".to_vec()), None, None)]
    #[case("entry with timestamps", 0, b"key".to_vec(), 1, "value", Some(b"val".to_vec()), Some(1000), Some(2000))]
    #[case("tombstone", 5, b"tomb".to_vec(), 42, "tombstone", None, Some(500), None)]
    #[case("tombstone with create_ts", 0, b"deleted_key".to_vec(), 999, "tombstone", None, Some(12345), None)]
    #[case("merge operand", 0, b"merge_key".to_vec(), 42, "merge", Some(b"merge_value".to_vec()), None, Some(1000))]
    #[case("2-byte varints", 200, vec![b'k'; 200], 1, "value", Some(vec![b'v'; 200]), None, None)]
    #[case("3-byte varints", 20000, vec![b'k'; 20000], 1, "value", Some(vec![b'v'; 20000]), None, None)]
    fn should_encode_decode_round_trip(
        #[case] _name: &str,
        #[case] shared_bytes: u32,
        #[case] key_suffix: Vec<u8>,
        #[case] seq: u64,
        #[case] value_tag: &str,
        #[case] value_data: Option<Vec<u8>>,
        #[case] create_ts: Option<i64>,
        #[case] expire_ts: Option<i64>,
    ) {
        // given: a row entry
        let value = make_value(value_tag, value_data);

        let entry = SstRowEntryV2::new(
            shared_bytes,
            Bytes::from(key_suffix.clone()),
            seq,
            value.clone(),
            create_ts,
            expire_ts,
        );

        // when: encoding and decoding
        let data = encode(&entry);
        let header = SstRowHeaderV2::decode(&data, 0);
        let (tail, end) = SstRowTailV2::decode(&data, &header).expect("decode failed");

        // then: all fields match
        assert_eq!(header.shared, shared_bytes as usize);
        assert_eq!(header.key_suffix(&data), key_suffix.as_slice());
        assert_eq!(tail.seq, seq);
        assert_eq!(tail.value, value);
        assert_eq!(tail.create_ts, create_ts);
        assert_eq!(tail.expire_ts, expire_ts);
        assert_eq!(end, data.len());
    }

    #[rstest]
    #[case("basic", 5, b"suffix".to_vec(), "value", Some(b"value".to_vec()))]
    #[case("at restart point", 0, b"full_key_at_restart".to_vec(), "value", Some(b"value".to_vec()))]
    #[case("empty suffix", 10, b"".to_vec(), "value", Some(b"value".to_vec()))]
    #[case("large shared bytes", 16384, b"x".to_vec(), "value", Some(b"v".to_vec()))]
    #[case("with tombstone", 3, b"deleted".to_vec(), "tombstone", None)]
    #[case("large key suffix", 0, vec![b'k'; 1000], "value", Some(b"v".to_vec()))]
    fn should_decode_header(
        #[case] _name: &str,
        #[case] shared_bytes: u32,
        #[case] key_suffix: Vec<u8>,
        #[case] value_tag: &str,
        #[case] value_data: Option<Vec<u8>>,
    ) {
        // given: an encoded entry
        let value = make_value(value_tag, value_data);
        let entry = SstRowEntryV2::new(
            shared_bytes,
            Bytes::from(key_suffix.clone()),
            1,
            value,
            None,
            None,
        );
        let data = encode(&entry);

        // when: decoding only the header
        let header = SstRowHeaderV2::decode(&data, 0);

        // then: key info is correct
        assert_eq!(header.shared, shared_bytes as usize);
        assert_eq!(header.key_suffix(&data), key_suffix.as_slice());
    }

    #[test]
    fn should_encode_with_varints() {
        // given: an entry with known values
        let entry = SstRowEntryV2::new(
            3,
            Bytes::from("abc"),
            1,
            ValueDeletable::Value(Bytes::from("xyz")),
            None,
            None,
        );

        // when: encoding
        let buf = encode(&entry);

        // then: varints are at the start
        // shared_bytes=3 -> 0x03 (1 byte)
        // unshared_bytes=3 -> 0x03 (1 byte)
        // value_len=3 -> 0x03 (1 byte)
        assert_eq!(buf[0], 3);
        assert_eq!(buf[1], 3);
        assert_eq!(buf[2], 3);
        // key_delta follows
        assert_eq!(&buf[3..6], b"abc");
        // value follows
        assert_eq!(&buf[6..9], b"xyz");
    }

    #[test]
    fn should_decode_header_positions() {
        // given: an encoded entry
        let entry = SstRowEntryV2::new(
            2,
            Bytes::from("key"),
            1,
            ValueDeletable::Value(Bytes::from("value")),
            None,
            None,
        );
        let data = encode(&entry);

        // when: decoding only the header
        let header = SstRowHeaderV2::decode(&data, 0);

        // then: the positions follow the three 1-byte varints
        assert_eq!(header.key_start, 3);
        assert_eq!(header.key_end, 6); // 3 + "key"
        assert_eq!(header.value_end, 11); // 6 + "value"
        // and: the next row starts after seq (8) and flags (1)
        assert_eq!(header.next_row_offset(&data).unwrap(), 20);
        assert_eq!(data.len(), 20);
    }

    #[test]
    fn should_calculate_encoded_size_correctly() {
        // given: entries with various configurations
        let entry = SstRowEntryV2::new(
            3,
            Bytes::from("abc"),
            1,
            ValueDeletable::Value(Bytes::from("xyz")),
            None,
            None,
        );

        // when: calculating encoded size
        let size = entry.encoded_size();

        // then: size matches actual encoding
        assert_eq!(size, encode(&entry).len());
    }

    /// Strategy to generate arbitrary ValueDeletable
    fn arb_value_deletable() -> impl Strategy<Value = ValueDeletable> {
        prop_oneof![
            prop::collection::vec(any::<u8>(), 0..1024)
                .prop_map(|v| ValueDeletable::Value(Bytes::from(v))),
            prop::collection::vec(any::<u8>(), 0..1024)
                .prop_map(|v| ValueDeletable::Merge(Bytes::from(v))),
            Just(ValueDeletable::Tombstone),
        ]
    }

    /// Strategy to generate arbitrary optional timestamps
    fn arb_optional_timestamp() -> impl Strategy<Value = Option<i64>> {
        prop_oneof![Just(None), any::<i64>().prop_map(Some),]
    }

    proptest! {
        #[test]
        fn should_encode_decode_round_trip_proptest(
            shared_bytes in any::<u32>(),
            key_suffix in prop::collection::vec(any::<u8>(), 0..1024),
            seq in any::<u64>(),
            value in arb_value_deletable(),
            create_ts in arb_optional_timestamp(),
            expire_ts in arb_optional_timestamp(),
        ) {
            // given: an arbitrary row entry
            let entry = SstRowEntryV2::new(
                shared_bytes,
                Bytes::from(key_suffix.clone()),
                seq,
                value.clone(),
                create_ts,
                expire_ts,
            );

            // when: encoding and decoding
            let data = encode(&entry);
            let header = SstRowHeaderV2::decode(&data, 0);
            let (tail, decoded_end) = SstRowTailV2::decode(&data, &header).expect("decode failed");
            let skipped_end = header.next_row_offset(&data).expect("decode failed");

            // then: all fields match
            prop_assert_eq!(header.shared, shared_bytes as usize);
            prop_assert_eq!(header.key_suffix(&data), key_suffix.as_slice());
            prop_assert_eq!(tail.seq, seq);
            prop_assert_eq!(tail.value, value);
            prop_assert_eq!(tail.create_ts, create_ts);
            prop_assert_eq!(tail.expire_ts, expire_ts);

            // and: the decoder, the skip, and encoded_size agree on the row end
            prop_assert_eq!(decoded_end, data.len());
            prop_assert_eq!(skipped_end, data.len());
            prop_assert_eq!(entry.encoded_size(), data.len());
        }
    }
}
