use std::error::Error;
use std::io::{self, Read, Write};
use std::str;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use clap::ValueEnum;
use serde_json::{json, Value};
use slatedb::bytes::Bytes;

type DataResult<T> = Result<T, Box<dyn Error>>;

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum DataFormat {
    Csv,
    Tsv,
    Json,
    Jsonl,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum DataEncoding {
    Utf8,
    Base64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DataRecord {
    pub(crate) key: Bytes,
    pub(crate) value: Bytes,
}

pub(crate) fn read_records<R: Read>(
    reader: R,
    format: DataFormat,
    encoding: DataEncoding,
) -> DataResult<Vec<DataRecord>> {
    match format {
        DataFormat::Csv => read_delimited_records(reader, b',', encoding),
        DataFormat::Tsv => read_delimited_records(reader, b'\t', encoding),
        DataFormat::Json => read_json_records(reader, encoding),
        DataFormat::Jsonl => read_jsonl_records(reader, encoding),
    }
}

#[cfg(test)]
pub(crate) fn write_records<W, I>(
    writer: W,
    format: DataFormat,
    encoding: DataEncoding,
    records: I,
) -> DataResult<u64>
where
    W: Write,
    I: IntoIterator<Item = (Bytes, Bytes)>,
{
    let mut writer = DataRecordWriter::new(writer, format, encoding)?;
    for (key, value) in records {
        writer.write_record(&key, &value)?;
    }
    writer.finish()
}

pub(crate) struct DataRecordWriter<W: Write> {
    inner: DataRecordWriterInner<W>,
    encoding: DataEncoding,
    count: u64,
}

enum DataRecordWriterInner<W: Write> {
    Delimited(Box<csv::Writer<W>>),
    Json { writer: W, first: bool },
    Jsonl(W),
}

impl<W: Write> DataRecordWriter<W> {
    pub(crate) fn new(writer: W, format: DataFormat, encoding: DataEncoding) -> DataResult<Self> {
        let inner = match format {
            DataFormat::Csv => Self::new_delimited_writer(writer, b',')?,
            DataFormat::Tsv => Self::new_delimited_writer(writer, b'\t')?,
            DataFormat::Json => {
                let mut writer = writer;
                writer.write_all(b"[\n")?;
                DataRecordWriterInner::Json {
                    writer,
                    first: true,
                }
            }
            DataFormat::Jsonl => DataRecordWriterInner::Jsonl(writer),
        };

        Ok(Self {
            inner,
            encoding,
            count: 0,
        })
    }

    fn new_delimited_writer(writer: W, delimiter: u8) -> DataResult<DataRecordWriterInner<W>> {
        let mut writer = csv::WriterBuilder::new()
            .delimiter(delimiter)
            .from_writer(writer);
        writer.write_record(["key", "value"])?;
        Ok(DataRecordWriterInner::Delimited(Box::new(writer)))
    }

    pub(crate) fn write_record(&mut self, key: &[u8], value: &[u8]) -> DataResult<()> {
        let key = encode_bytes(key, "key", self.encoding)?;
        let value = encode_bytes(value, "value", self.encoding)?;
        match &mut self.inner {
            DataRecordWriterInner::Delimited(writer) => {
                writer.write_record([key.as_str(), value.as_str()])?;
            }
            DataRecordWriterInner::Json { writer, first } => {
                if !*first {
                    writer.write_all(b",\n")?;
                }
                *first = false;
                serde_json::to_writer(writer, &json!({ "key": key, "value": value }))?;
            }
            DataRecordWriterInner::Jsonl(writer) => {
                serde_json::to_writer(&mut *writer, &json!({ "key": key, "value": value }))?;
                writer.write_all(b"\n")?;
            }
        }
        self.count += 1;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> DataResult<u64> {
        match &mut self.inner {
            DataRecordWriterInner::Delimited(writer) => writer.flush()?,
            DataRecordWriterInner::Json { writer, .. } => {
                writer.write_all(b"\n]\n")?;
                writer.flush()?;
            }
            DataRecordWriterInner::Jsonl(writer) => writer.flush()?,
        }
        Ok(self.count)
    }
}

fn read_delimited_records<R: Read>(
    reader: R,
    delimiter: u8,
    encoding: DataEncoding,
) -> DataResult<Vec<DataRecord>> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(true)
        .from_reader(reader);

    let headers = reader.headers()?;
    if headers.len() != 2 || headers.get(0) != Some("key") || headers.get(1) != Some("value") {
        return Err(invalid_data("expected header row: key,value"));
    }

    let mut records = Vec::new();
    for (index, record) in reader.records().enumerate() {
        let record = record?;
        if record.len() != 2 {
            return Err(invalid_data(format!(
                "record {} expected 2 fields, found {}",
                index + 2,
                record.len()
            )));
        }
        records.push(DataRecord {
            key: decode_string(&record[0], "key", encoding)?,
            value: decode_string(&record[1], "value", encoding)?,
        });
    }
    Ok(records)
}

fn read_json_records<R: Read>(
    mut reader: R,
    encoding: DataEncoding,
) -> DataResult<Vec<DataRecord>> {
    let mut input = String::new();
    reader.read_to_string(&mut input)?;
    let input = input.trim();
    if input.is_empty() {
        return Ok(Vec::new());
    }

    if input.starts_with('[') {
        let values: Vec<Value> = serde_json::from_str(input)?;
        values
            .into_iter()
            .enumerate()
            .map(|(index, value)| record_from_json_value(value, index + 1, encoding))
            .collect()
    } else {
        read_jsonl_records(input.as_bytes(), encoding)
    }
}

fn read_jsonl_records<R: Read>(
    mut reader: R,
    encoding: DataEncoding,
) -> DataResult<Vec<DataRecord>> {
    let mut input = String::new();
    reader.read_to_string(&mut input)?;
    input
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(index, line)| {
            let value = serde_json::from_str(line)?;
            record_from_json_value(value, index + 1, encoding)
        })
        .collect()
}

fn record_from_json_value(
    value: Value,
    record_number: usize,
    encoding: DataEncoding,
) -> DataResult<DataRecord> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid_data(format!("JSON record {record_number} must be an object")))?;
    let key = json_string_field(object, "key", record_number)?;
    let value = json_string_field(object, "value", record_number)?;

    Ok(DataRecord {
        key: decode_string(key, "key", encoding)?,
        value: decode_string(value, "value", encoding)?,
    })
}

fn json_string_field<'a>(
    object: &'a serde_json::Map<String, Value>,
    field: &str,
    record_number: usize,
) -> DataResult<&'a str> {
    object.get(field).and_then(Value::as_str).ok_or_else(|| {
        invalid_data(format!(
            "JSON record {record_number} missing string {field}"
        ))
    })
}

fn encode_bytes(bytes: &[u8], field: &str, encoding: DataEncoding) -> DataResult<String> {
    match encoding {
        DataEncoding::Utf8 => str::from_utf8(bytes).map(str::to_owned).map_err(|_| {
            invalid_data(format!(
                "cannot encode {field} as UTF-8; use --encoding base64"
            ))
        }),
        DataEncoding::Base64 => Ok(BASE64_STANDARD.encode(bytes)),
    }
}

fn decode_string(value: &str, field: &str, encoding: DataEncoding) -> DataResult<Bytes> {
    match encoding {
        DataEncoding::Utf8 => Ok(Bytes::copy_from_slice(value.as_bytes())),
        DataEncoding::Base64 => BASE64_STANDARD
            .decode(value)
            .map(Bytes::from)
            .map_err(|error| invalid_data(format!("invalid base64 {field}: {error}"))),
    }
}

fn invalid_data(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::new(io::ErrorKind::InvalidData, message.into()))
}

#[cfg(test)]
mod tests {
    use slatedb::bytes::Bytes;

    use super::*;

    #[test]
    fn writes_csv_with_header_and_escaped_values() {
        let records = vec![
            (Bytes::from_static(b"alpha"), Bytes::from_static(b"one")),
            (
                Bytes::from_static(b"comma,key"),
                Bytes::from_static(b"two\nlines"),
            ),
        ];
        let mut output = Vec::new();

        let count = write_records(&mut output, DataFormat::Csv, DataEncoding::Utf8, records)
            .expect("csv export should succeed");

        assert_eq!(count, 2);
        assert_eq!(
            String::from_utf8(output).expect("csv should be utf8"),
            "key,value\nalpha,one\n\"comma,key\",\"two\nlines\"\n"
        );
    }

    #[test]
    fn reads_tsv_records() {
        let input = b"key\tvalue\nalpha\tone\nbeta\ttwo\n";

        let records = read_records(&input[..], DataFormat::Tsv, DataEncoding::Utf8)
            .expect("tsv import should succeed");

        assert_eq!(
            records,
            vec![
                DataRecord {
                    key: Bytes::from_static(b"alpha"),
                    value: Bytes::from_static(b"one")
                },
                DataRecord {
                    key: Bytes::from_static(b"beta"),
                    value: Bytes::from_static(b"two")
                }
            ]
        );
    }

    #[test]
    fn reads_json_array_records() {
        let input = br#"[{"key":"alpha","value":"one"},{"key":"beta","value":"two"}]"#;

        let records = read_records(&input[..], DataFormat::Json, DataEncoding::Utf8)
            .expect("json import should succeed");

        assert_eq!(
            records,
            vec![
                DataRecord {
                    key: Bytes::from_static(b"alpha"),
                    value: Bytes::from_static(b"one")
                },
                DataRecord {
                    key: Bytes::from_static(b"beta"),
                    value: Bytes::from_static(b"two")
                }
            ]
        );
    }

    #[test]
    fn reads_jsonl_records() {
        let input =
            b"{\"key\":\"alpha\",\"value\":\"one\"}\n{\"key\":\"beta\",\"value\":\"two\"}\n";

        let records = read_records(&input[..], DataFormat::Jsonl, DataEncoding::Utf8)
            .expect("jsonl import should succeed");

        assert_eq!(
            records,
            vec![
                DataRecord {
                    key: Bytes::from_static(b"alpha"),
                    value: Bytes::from_static(b"one")
                },
                DataRecord {
                    key: Bytes::from_static(b"beta"),
                    value: Bytes::from_static(b"two")
                }
            ]
        );
    }

    #[test]
    fn writes_jsonl_records() {
        let records = vec![
            (Bytes::from_static(b"alpha"), Bytes::from_static(b"one")),
            (Bytes::from_static(b"beta"), Bytes::from_static(b"two")),
        ];
        let mut output = Vec::new();

        let count = write_records(&mut output, DataFormat::Jsonl, DataEncoding::Utf8, records)
            .expect("jsonl export should succeed");

        assert_eq!(count, 2);
        assert_eq!(
            String::from_utf8(output).expect("jsonl should be utf8"),
            "{\"key\":\"alpha\",\"value\":\"one\"}\n{\"key\":\"beta\",\"value\":\"two\"}\n"
        );
    }

    #[test]
    fn round_trips_base64_json_records() {
        let records = vec![(Bytes::from(vec![0, 1, 2]), Bytes::from(vec![255, 254]))];
        let mut output = Vec::new();

        write_records(&mut output, DataFormat::Json, DataEncoding::Base64, records)
            .expect("json export should succeed");
        let decoded = read_records(&output[..], DataFormat::Json, DataEncoding::Base64)
            .expect("json import should succeed");

        assert_eq!(
            decoded,
            vec![DataRecord {
                key: Bytes::from(vec![0, 1, 2]),
                value: Bytes::from(vec![255, 254])
            }]
        );
    }

    #[test]
    fn rejects_utf8_export_for_binary_data() {
        let records = vec![(Bytes::from_static(b"key"), Bytes::from(vec![0xff]))];
        let mut output = Vec::new();

        let err = write_records(&mut output, DataFormat::Json, DataEncoding::Utf8, records)
            .expect_err("binary value should not export as utf8");

        assert!(err.to_string().contains("use --encoding base64"));
    }
}
