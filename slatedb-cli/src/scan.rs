use std::error::Error;
use std::io::{self, Write};
use std::ops::Bound;
use std::sync::Arc;

use object_store::path::Path;
use object_store::ObjectStore;
use slatedb::config::ScanOptions;
use slatedb::{DbReader, DbReaderMode};
use uuid::Uuid;

type KeyRange = (Bound<Vec<u8>>, Bound<Vec<u8>>);

/// How keys are rendered, and how `--prefix`/`--from`/`--to` are parsed.
#[derive(Copy, Clone, Debug, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum KeyMode {
    /// Bounds hex-decode only with a `0x` prefix; keys render as text unless binary or
    /// `0x`-prefixed.
    Auto,
    /// Bounds are hex (optional `0x` prefix); keys render as `0x` hex.
    Hex,
    /// Bounds are taken literally; keys render via lossy UTF-8.
    Utf8,
}

/// How each value is rendered.
#[derive(Copy, Clone, Debug, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum ValueMode {
    /// Clean text is shown as-is, otherwise `<binary N bytes>`.
    Auto,
    /// Lowercase `0x` hex.
    Hex,
    /// Raw bytes, lossily decoded as UTF-8.
    Utf8,
    /// Just the byte length.
    Len,
    /// Omit values entirely; print keys only.
    None,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn exec_scan(
    path: Path,
    object_store: Arc<dyn ObjectStore>,
    prefix: Option<String>,
    from: Option<String>,
    to: Option<String>,
    key_mode: KeyMode,
    value_mode: ValueMode,
    max_keys: Option<u64>,
    count: bool,
    checkpoint: Option<Uuid>,
) -> Result<(), Box<dyn Error>> {
    // Decode the range selectors before opening the reader, so a bad spec fails fast.
    let prefix = prefix
        .as_deref()
        .map(|p| decode_bound(p, key_mode))
        .transpose()?;
    let range = build_range(from.as_deref(), to.as_deref(), key_mode)?;

    let mut builder =
        DbReader::builder(path, object_store).with_reader_mode(DbReaderMode::FollowLatest);
    if let Some(checkpoint_id) = checkpoint {
        builder = builder.with_reader_mode(DbReaderMode::Checkpoint(checkpoint_id));
    }
    let reader = builder.build().await?;

    // The scan defaults fetch one SST data block per request with no prefetch, which
    // turns a cacheless remote scan into thousands of serial round-trips.  Read ahead in
    // chunks and fetch several in parallel instead.
    let scan_opts = ScanOptions::default()
        .with_read_ahead_bytes(4 * 1024 * 1024)
        .with_max_fetch_tasks(4);
    let mut scan_iter = match prefix {
        Some(prefix) => {
            reader
                .scan_prefix_with_options(prefix, .., &scan_opts)
                .await?
        }
        None => reader.scan_with_options(range, &scan_opts).await?,
    };

    let mut out = io::BufWriter::new(io::stdout().lock());
    let mut emitted: u64 = 0;
    let mut total_bytes: u64 = 0;

    while let Some(kv) = scan_iter.next().await? {
        if max_keys.is_some_and(|max| emitted >= max) {
            break;
        }
        emitted += 1;
        total_bytes += (kv.key.len() + kv.value.len()) as u64;
        if !count {
            render_key(&mut out, &kv.key, key_mode)?;
            if !matches!(value_mode, ValueMode::None) {
                out.write_all(b"\t")?;
                render_value(&mut out, &kv.value, value_mode)?;
            }
            out.write_all(b"\n")?;
        }
    }
    out.flush()?;
    drop(out);
    reader.close().await?;

    if count {
        println!("{emitted} entries, {total_bytes} bytes");
    }
    Ok(())
}

fn build_range(from: Option<&str>, to: Option<&str>, mode: KeyMode) -> Result<KeyRange, String> {
    let lower = match from {
        Some(s) => Bound::Included(decode_bound(s, mode)?),
        None => Bound::Unbounded,
    };
    let upper = match to {
        Some(s) => Bound::Excluded(decode_bound(s, mode)?),
        None => Bound::Unbounded,
    };
    Ok((lower, upper))
}

fn decode_bound(spec: &str, mode: KeyMode) -> Result<Vec<u8>, String> {
    match mode {
        KeyMode::Hex => decode_hex(spec),
        KeyMode::Utf8 => Ok(spec.as_bytes().to_vec()),
        KeyMode::Auto => {
            if spec.starts_with("0x") || spec.starts_with("0X") {
                decode_hex(spec)
            } else {
                Ok(spec.as_bytes().to_vec())
            }
        }
    }
}

fn render_key(out: &mut impl Write, key: &[u8], mode: KeyMode) -> io::Result<()> {
    match mode {
        KeyMode::Hex => write_hex_0x(out, key),
        KeyMode::Utf8 => out.write_all(String::from_utf8_lossy(key).as_bytes()),
        KeyMode::Auto => {
            if is_text_key(key) {
                out.write_all(key)
            } else {
                write_hex_0x(out, key)
            }
        }
    }
}

fn render_value(out: &mut impl Write, value: &[u8], mode: ValueMode) -> io::Result<()> {
    match mode {
        ValueMode::None => Ok(()),
        ValueMode::Hex => write_hex_0x(out, value),
        ValueMode::Len => write!(out, "{} bytes", value.len()),
        ValueMode::Utf8 => out.write_all(String::from_utf8_lossy(value).as_bytes()),
        ValueMode::Auto => {
            if is_clean_text(value) {
                out.write_all(value)
            } else {
                write!(out, "<binary {} bytes>", value.len())
            }
        }
    }
}

fn write_hex_0x(out: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    out.write_all(b"0x")?;
    for b in bytes {
        write!(out, "{b:02x}")?;
    }
    Ok(())
}

fn is_text_key(key: &[u8]) -> bool {
    // Exclude a leading 0x so a 0x prefix in the output always means hex.
    is_clean_text(key) && !key.starts_with(b"0x") && !key.starts_with(b"0X")
}

fn is_clean_text(value: &[u8]) -> bool {
    // Reject control chars (tab, newline, NUL) so a value stays on one line.
    match std::str::from_utf8(value) {
        Ok(s) => s.chars().all(|c| !c.is_control()),
        Err(_) => false,
    }
}

fn decode_hex(spec: &str) -> Result<Vec<u8>, String> {
    let s = spec
        .strip_prefix("0x")
        .or_else(|| spec.strip_prefix("0X"))
        .unwrap_or(spec);
    // Reject non-ASCII before the 2-byte slicing below, which would panic on a multi-byte
    // UTF-8 char boundary.
    if !s.is_ascii() {
        return Err(format!("not a hex string: {spec:?}"));
    }
    if !s.len().is_multiple_of(2) {
        return Err(format!(
            "hex string must have an even number of digits: {spec:?}"
        ));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| format!("invalid hex {spec:?}: {e}"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key_str(key: &[u8], mode: KeyMode) -> String {
        let mut buf = Vec::new();
        render_key(&mut buf, key, mode).unwrap();
        String::from_utf8(buf).unwrap()
    }

    fn value_str(value: &[u8], mode: ValueMode) -> String {
        let mut buf = Vec::new();
        render_value(&mut buf, value, mode).unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn decode_bound_hex() {
        assert_eq!(
            decode_bound("40ff", KeyMode::Hex).unwrap(),
            vec![0x40, 0xff]
        );
        assert_eq!(
            decode_bound("0x40ff", KeyMode::Hex).unwrap(),
            vec![0x40, 0xff]
        );
        assert!(decode_bound("4", KeyMode::Hex).is_err());
        assert!(decode_bound("4g", KeyMode::Hex).is_err());
        assert!(decode_bound("zz", KeyMode::Hex).is_err());
    }

    #[test]
    fn decode_bound_utf8() {
        assert_eq!(
            decode_bound("0x40", KeyMode::Utf8).unwrap(),
            b"0x40".to_vec()
        );
        assert_eq!(decode_bound("hi", KeyMode::Utf8).unwrap(), b"hi".to_vec());
    }

    #[test]
    fn decode_bound_auto() {
        // Leading 0x -> hex-decoded.
        assert_eq!(decode_bound("0x40", KeyMode::Auto).unwrap(), vec![0x40]);
        assert_eq!(decode_bound("0X40", KeyMode::Auto).unwrap(), vec![0x40]);
        // Otherwise literal bytes.
        assert_eq!(
            decode_bound("user", KeyMode::Auto).unwrap(),
            b"user".to_vec()
        );
    }

    #[test]
    fn build_range_bounds() {
        assert_eq!(
            build_range(None, None, KeyMode::Auto).unwrap(),
            (Bound::Unbounded, Bound::Unbounded)
        );
        assert_eq!(
            build_range(Some("a"), Some("c"), KeyMode::Utf8).unwrap(),
            (
                Bound::Included(b"a".to_vec()),
                Bound::Excluded(b"c".to_vec())
            )
        );
        assert_eq!(
            build_range(Some("00"), None, KeyMode::Hex).unwrap(),
            (Bound::Included(vec![0x00]), Bound::Unbounded)
        );
    }

    #[test]
    fn render_key_auto() {
        assert_eq!(key_str(b"user/42", KeyMode::Auto), "user/42");
        assert_eq!(key_str(&[0x40, 0x00], KeyMode::Auto), "0x4000");
        assert_eq!(key_str(b"0xabc", KeyMode::Auto), "0x3078616263");
    }

    #[test]
    fn render_key_hex_and_utf8() {
        assert_eq!(key_str(&[0x40, 0xff], KeyMode::Hex), "0x40ff");
        assert_eq!(key_str(b"hi", KeyMode::Utf8), "hi");
        // utf8 mode is lossy on invalid sequences.
        assert_eq!(key_str(&[0xff], KeyMode::Utf8), "\u{fffd}");
    }

    #[test]
    fn render_value_modes() {
        assert_eq!(value_str(b"hello", ValueMode::Auto), "hello");
        assert_eq!(
            value_str(&[0, 1, 2, 3], ValueMode::Auto),
            "<binary 4 bytes>"
        );
        assert_eq!(value_str(&[0x00, 0x01], ValueMode::Hex), "0x0001");
        assert_eq!(value_str(&[0xff], ValueMode::Utf8), "\u{fffd}");
        assert_eq!(value_str(&[0, 1, 2], ValueMode::Len), "3 bytes");
        assert_eq!(value_str(b"anything", ValueMode::None), "");
    }
}
