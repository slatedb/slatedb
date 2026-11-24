//! Immutable local files for cached ranges: *bases* (live rows at one sequence number, no
//! tombstones) and *runs* (all versions in `(lo, hi]`, plus an inline Bloom filter).
//!
//! Files are written to a temporary path, renamed into place, never synced, and deleted when the
//! last [`RangeFile`] reference drops. Only page fence keys and run filters live on the heap.
//!
//! Layout (little endian):
//!
//! ```text
//! [blob run][pad] page 0 ... page N-1
//! fence region:   per page: u64 page offset | u32 key len | first key
//! filter region:  run Bloom filter (empty in a base)
//! trailer (80 B): magic u64 | version u32 | page_size u32 | pages u64 | rows u64 |
//!                 fence_off u64 | fence_len u64 | data_bytes u64 | max_seq u64 |
//!                 filter_len u64 | flags u64 (bit 0 = run)
//! page:           u16 rows n | u16 hint skip s | n x u16 offsets | n x u32 hints | rows | pad
//! row:            u8 flags | u64 seq | i64 create_ts | key ref | value ref
//! ```
//!
//! Keys larger than `page_size / 8` and values that push a row over `page_size / 4` are stored
//! out of line in the blob run preceding their page.

use std::borrow::Cow;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use slatedb_common::file_handle_cache::read_exact_at_offset;

use super::buffer::Kind;
use super::filter::{hash_key, Filter};
use crate::bytes_range::BytesRange;
use crate::error::SlateDBError;
use crate::iter::RowEntryIterator;
use crate::types::{RowEntry, ValueDeletable};

/// The extension of finished range cache files.
pub(crate) const FILE_EXTENSION: &str = "rcf";
/// The extension of range cache files that are being written.
pub(crate) const TEMP_FILE_EXTENSION: &str = "rcf.tmp";

const MAGIC: u64 = u64::from_le_bytes(*b"SLATERCF");
const VERSION: u32 = 2;
const TRAILER_LEN: usize = 80;
/// The trailer flag of a run.
const FILE_FLAG_RUN: u64 = 1;
/// Row count (u16) and the length of the key prefix that the hints skip (u16).
const PAGE_HEADER_LEN: usize = 4;
const ROW_OFFSET_LEN: usize = 2;
/// Four bytes of each key after the skipped prefix, big-endian: a search compares these, in
/// the page header, before it reads a row.
const ROW_HINT_LEN: usize = 4;
/// The longest prefix that hints skip. Writers keep this many bytes plus a hint of each key.
const MAX_HINT_SKIP: usize = 60;
/// Flags, seq and create_ts.
const ROW_FIXED_LEN: usize = 1 + 8 + 8;
const INLINE_REF_LEN: usize = 4;
const OUT_OF_LINE_REF_LEN: usize = 12;

const FLAG_KEY_OUT_OF_LINE: u8 = 1;
const FLAG_VALUE_OUT_OF_LINE: u8 = 2;
/// The row is a merge operand. In a base, it has no base value below it.
const FLAG_MERGE: u8 = 4;
/// The row has a create timestamp.
const FLAG_CREATE_TS: u8 = 8;
/// The row is a tombstone. Only runs hold tombstones.
const FLAG_TOMBSTONE: u8 = 16;

/// The kind of a row with these flags.
fn kind_of(flags: u8) -> Kind {
    if flags & FLAG_TOMBSTONE != 0 {
        Kind::Tombstone
    } else if flags & FLAG_MERGE != 0 {
        Kind::Merge
    } else {
        Kind::Value
    }
}

/// A row's value of `kind`, from its value bytes.
fn value_of(kind: Kind, value: Bytes) -> ValueDeletable {
    match kind {
        Kind::Value => ValueDeletable::Value(value),
        Kind::Merge => ValueDeletable::Merge(value),
        Kind::Tombstone => ValueDeletable::Tombstone,
    }
}

pub(crate) const MIN_PAGE_SIZE: usize = 512;
pub(crate) const MAX_PAGE_SIZE: usize = 65536;

/// Page sizes must be powers of two in `512..=65536`, because row offsets are `u16`.
pub(crate) fn valid_page_size(page_size: usize) -> bool {
    page_size.is_power_of_two() && (MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&page_size)
}

fn u16_at(b: &[u8], off: usize) -> usize {
    u16::from_le_bytes([b[off], b[off + 1]]) as usize
}

fn u32_at(b: &[u8], off: usize) -> usize {
    u32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]]) as usize
}

fn u64_at(b: &[u8], off: usize) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&b[off..off + 8]);
    u64::from_le_bytes(buf)
}

/// The first eight bytes of `bytes` as a big-endian number, zero-padded. Two keys whose
/// numbers differ compare like their numbers.
fn prefix64(bytes: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    let n = bytes.len().min(8);
    buf[..n].copy_from_slice(&bytes[..n]);
    u64::from_be_bytes(buf)
}

/// The first four bytes of `bytes` as a big-endian number, zero-padded. Like `prefix64`.
fn prefix32(bytes: &[u8]) -> u32 {
    let mut buf = [0u8; 4];
    let n = bytes.len().min(4);
    buf[..n].copy_from_slice(&bytes[..n]);
    u32::from_be_bytes(buf)
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

fn corrupt(msg: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("range cache file: {msg}"),
    )
}

/// How the range cache reads its files.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RangeCachePageSource {
    /// Map each file into memory for syscall-free reads on page-cache hits.
    #[default]
    Mmap,
    /// Read files with positional reads (`pread`).
    PositionalRead,
}

/// Reads the bytes of one immutable file.
pub(crate) trait PageSource: Send + Sync {
    /// Returns the whole file if the source maps it into memory.
    fn mapped(&self) -> Option<&[u8]>;
    /// Fills `buf` with the bytes at `offset`.
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
}

struct MmapPageSource {
    map: memmap2::Mmap,
}

impl PageSource for MmapPageSource {
    fn mapped(&self) -> Option<&[u8]> {
        Some(&self.map)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        buf.copy_from_slice(slice_at(&self.map, offset, buf.len())?);
        Ok(())
    }
}

struct PositionalReadPageSource {
    file: File,
}

impl PageSource for PositionalReadPageSource {
    fn mapped(&self) -> Option<&[u8]> {
        None
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        read_exact_at_offset(&self.file, buf, offset)
    }
}

fn slice_at(bytes: &[u8], offset: u64, len: usize) -> io::Result<&[u8]> {
    usize::try_from(offset)
        .ok()
        .and_then(|start| bytes.get(start..start.checked_add(len)?))
        .ok_or_else(|| corrupt("read past the end of the file"))
}

/// Reads `len` bytes at `offset`. A memory-mapped source returns them without a copy.
fn read_bytes(source: &dyn PageSource, offset: u64, len: usize) -> io::Result<Cow<'_, [u8]>> {
    match source.mapped() {
        Some(map) => slice_at(map, offset, len).map(Cow::Borrowed),
        None => {
            let mut buf = vec![0u8; len];
            source.read_exact_at(&mut buf, offset)?;
            Ok(Cow::Owned(buf))
        }
    }
}

/// Copies a key and a value into one allocation.
pub(super) fn copy_key_value(key: &[u8], value: &[u8]) -> (Bytes, Bytes) {
    let mut buf = BytesMut::with_capacity(key.len() + value.len());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    let buf = buf.freeze();
    (buf.slice(..key.len()), buf.slice(key.len()..))
}

/// Where the bytes of a key or a value are.
enum Ref {
    /// In the page, at this range.
    Inline(std::ops::Range<usize>),
    /// Out of line, in the file.
    OutOfLine { offset: u64, len: usize },
}

/// The decoded header and references of one row.
struct RawRow {
    flags: u8,
    seq: u64,
    create_ts: i64,
    key: Ref,
    value: Ref,
    /// The offset just after the row's last byte in its page (its value, or its value's
    /// reference when the value is out of line).
    end: usize,
}

fn parse_ref(page: &[u8], at: usize, out_of_line: bool) -> io::Result<(Ref, usize)> {
    if out_of_line {
        if at + OUT_OF_LINE_REF_LEN > page.len() {
            return Err(corrupt("truncated reference"));
        }
        let offset = u64_at(page, at);
        let len = u32_at(page, at + 8);
        Ok((Ref::OutOfLine { offset, len }, at + OUT_OF_LINE_REF_LEN))
    } else {
        if at + INLINE_REF_LEN > page.len() {
            return Err(corrupt("truncated reference"));
        }
        let len = u32_at(page, at);
        let start = at + INLINE_REF_LEN;
        if start + len > page.len() {
            return Err(corrupt("truncated inline bytes"));
        }
        Ok((Ref::Inline(start..start + len), start + len))
    }
}

/// Decodes row `index` of `page`.
fn parse_row(page: &[u8], index: usize) -> io::Result<RawRow> {
    parse_row_at(page, u16_at(page, PAGE_HEADER_LEN + ROW_OFFSET_LEN * index))
}

/// Decodes the row at offset `off` of `bytes`, a page or a part of one. The references of the
/// row are relative to `bytes`.
fn parse_row_at(page: &[u8], off: usize) -> io::Result<RawRow> {
    if off + ROW_FIXED_LEN > page.len() {
        return Err(corrupt("bad row offset"));
    }
    let flags = page[off];
    let seq = u64_at(page, off + 1);
    let create_ts = u64_at(page, off + 9) as i64;
    let (key, next) = parse_ref(page, off + ROW_FIXED_LEN, flags & FLAG_KEY_OUT_OF_LINE != 0)?;
    let (value, end) = parse_ref(page, next, flags & FLAG_VALUE_OUT_OF_LINE != 0)?;
    Ok(RawRow {
        flags,
        seq,
        create_ts,
        key,
        value,
        end,
    })
}

fn open_page_source(
    file: File,
    len: u64,
    kind: RangeCachePageSource,
) -> io::Result<Box<dyn PageSource>> {
    match kind {
        RangeCachePageSource::Mmap => {
            // SAFETY: the range cache owns its directory. It writes each file once, renames
            // it into place before it maps it, and never changes it after that. It only
            // unlinks files, and an unlinked file stays valid while it is mapped.
            #[allow(unsafe_code)]
            let map = unsafe { memmap2::Mmap::map(&file)? };
            if map.len() as u64 != len {
                return Err(corrupt("file length changed while it was opened"));
            }
            Ok(Box::new(MmapPageSource { map }))
        }
        RangeCachePageSource::PositionalRead => Ok(Box::new(PositionalReadPageSource { file })),
    }
}

/// Writes a base (strictly ascending keys) or run (`key asc, seq desc`) to `<name>.rcf.tmp` and
/// renames it to `<name>.rcf` on finish. Removes the temporary file if dropped unfinished.
pub(crate) struct RangeFileWriter {
    tmp_path: PathBuf,
    final_path: PathBuf,
    out: Option<BufWriter<File>>,
    page_size: usize,
    /// For a run: the hashes of its distinct keys, for its filter. `None` for a base.
    run_keys: Option<Vec<u64>>,
    /// The sequence number of the previous row.
    prev_seq: u64,
    /// Bytes written to the file so far.
    pos: u64,
    /// Rows of the open page, without the page header and the row offsets.
    body: Vec<u8>,
    /// Offsets of the rows in `body`.
    offsets: Vec<usize>,
    /// The first `MAX_HINT_SKIP + ROW_HINT_LEN` bytes of each key of the open page.
    hint_keys: Vec<u8>,
    /// `hint_keys` ends of each key.
    hint_key_ends: Vec<usize>,
    first_key: Vec<u8>,
    fences: Vec<u8>,
    page_buf: Vec<u8>,
    pages: u64,
    rows: u64,
    data_bytes: u64,
    max_seq: u64,
    prev_key: Option<Vec<u8>>,
    /// The keys and row offsets of a run, collected as the writer writes them, if asked for (see
    /// [`RangeFileWriter::with_locators`]).
    locators: Option<RowLocators>,
}

/// The keys and row offsets of a run in file order, grouped by key: what a view build needs to
/// add the run to a range's view (see the `view` module). A flush's writer collects them while it
/// writes the run, so the build does not read the file again.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct RowLocators {
    /// The distinct keys, back to back.
    keys: Vec<u8>,
    /// The end of each key in `keys`.
    key_end: Vec<usize>,
    /// The file offset of every row.
    offsets: Vec<u32>,
    /// The end of each key's rows in `offsets`.
    group_end: Vec<usize>,
    /// Rows counted so far, whose offsets may not be known yet.
    rows: usize,
}

impl RowLocators {
    fn start_key(&mut self, key: &[u8]) {
        if self.rows > 0 {
            self.group_end.push(self.rows);
        }
        self.keys.extend_from_slice(key);
        self.key_end.push(self.keys.len());
    }

    fn finish(&mut self) {
        if self.rows > 0 && self.group_end.last() != Some(&self.rows) {
            self.group_end.push(self.rows);
        }
    }

    /// The number of rows.
    pub(crate) fn rows(&self) -> usize {
        self.offsets.len()
    }

    /// The number of distinct keys.
    pub(crate) fn groups(&self) -> usize {
        self.key_end.len()
    }

    /// Distinct key `g`.
    pub(crate) fn key(&self, g: usize) -> &[u8] {
        let start = if g == 0 { 0 } else { self.key_end[g - 1] };
        &self.keys[start..self.key_end[g]]
    }

    /// The offsets of the rows of distinct key `g`, newest first.
    pub(crate) fn offsets(&self, g: usize) -> &[u32] {
        let start = if g == 0 { 0 } else { self.group_end[g - 1] };
        &self.offsets[start..self.group_end[g]]
    }

    pub(crate) fn first(&self) -> Option<&[u8]> {
        (self.groups() > 0).then(|| self.key(0))
    }

    pub(crate) fn last(&self) -> Option<&[u8]> {
        self.groups().checked_sub(1).map(|g| self.key(g))
    }

    /// The locators of a finished run, read from its file: the same as its writer collected.
    #[cfg(test)]
    pub(crate) fn read(file: &RangeFile) -> io::Result<Self> {
        let mut out = Self::default();
        let mut prev: Option<Vec<u8>> = None;
        file.for_each_row(|offset, key, _| {
            if prev.as_deref() != Some(key) {
                out.start_key(key);
                prev = Some(key.to_vec());
            }
            out.rows += 1;
            out.offsets
                .push(u32::try_from(offset).map_err(|_| corrupt("a run file of 4 GiB or more"))?);
            Ok(())
        })?;
        out.finish();
        Ok(out)
    }
}

impl RangeFileWriter {
    /// Starts the base `dir/<name>.rcf.tmp`.
    pub(crate) fn create(dir: &Path, name: &str, page_size: usize) -> io::Result<Self> {
        Self::create_kind(dir, name, page_size, false)
    }

    /// Starts the run `dir/<name>.rcf.tmp`.
    pub(crate) fn create_run(dir: &Path, name: &str, page_size: usize) -> io::Result<Self> {
        Self::create_kind(dir, name, page_size, true)
    }

    fn create_kind(dir: &Path, name: &str, page_size: usize, run: bool) -> io::Result<Self> {
        if !valid_page_size(page_size) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("page size {page_size} is not a power of two in 512..=65536"),
            ));
        }
        let tmp_path = dir.join(format!("{name}.{TEMP_FILE_EXTENSION}"));
        let final_path = dir.join(format!("{name}.{FILE_EXTENSION}"));
        let file = File::create(&tmp_path)?;
        Ok(Self {
            tmp_path,
            final_path,
            out: Some(BufWriter::with_capacity(256 * 1024, file)),
            page_size,
            run_keys: run.then(Vec::new),
            prev_seq: 0,
            pos: 0,
            body: Vec::with_capacity(page_size),
            offsets: Vec::new(),
            hint_keys: Vec::new(),
            hint_key_ends: Vec::new(),
            first_key: Vec::new(),
            fences: Vec::new(),
            page_buf: vec![0; page_size],
            pages: 0,
            rows: 0,
            data_bytes: 0,
            max_seq: 0,
            prev_key: None,
            locators: None,
        })
    }

    /// Makes the writer of a run collect the keys and offsets of its rows, which
    /// [`RangeFileWriter::finish_with_locators`] returns. Call it before the first row.
    pub(crate) fn with_locators(mut self) -> Self {
        debug_assert!(self.run_keys.is_some() && self.rows == 0);
        self.locators = Some(RowLocators::default());
        self
    }

    /// Returns about the size of the file if it were finished now.
    pub(crate) fn estimated_len(&self) -> u64 {
        let open_page = if self.offsets.is_empty() {
            0
        } else {
            self.page_size as u64
        };
        self.pos + open_page + self.fences.len() as u64 + TRAILER_LEN as u64
    }

    pub(crate) fn rows(&self) -> u64 {
        self.rows
    }

    fn write_raw(&mut self, bytes: &[u8]) -> io::Result<()> {
        match self.out.as_mut() {
            Some(out) => out.write_all(bytes)?,
            None => return Err(io::Error::other("range cache file writer is finished")),
        }
        self.pos += bytes.len() as u64;
        Ok(())
    }

    fn pad_to_page(&mut self) -> io::Result<()> {
        const ZEROS: [u8; 4096] = [0; 4096];
        let page_size = self.page_size as u64;
        let mut pad = (page_size - self.pos % page_size) % page_size;
        while pad > 0 {
            let n = pad.min(ZEROS.len() as u64) as usize;
            self.write_raw(&ZEROS[..n])?;
            pad -= n as u64;
        }
        Ok(())
    }

    /// Appends one non-expiring row. Bases require strictly ascending non-tombstone keys; runs
    /// allow tombstones and descending sequence numbers per key.
    pub(crate) fn push(&mut self, row: &RowEntry) -> io::Result<()> {
        if row.expire_ts.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "range cache files do not hold rows with an expire time",
            ));
        }
        let (kind, value): (Kind, &[u8]) = match &row.value {
            ValueDeletable::Value(v) => (Kind::Value, v),
            ValueDeletable::Merge(v) => (Kind::Merge, v),
            ValueDeletable::Tombstone => (Kind::Tombstone, &[]),
        };
        self.push_version(&row.key, row.seq, kind, row.create_ts, value)
    }

    /// Appends one row from its parts, which need not be in a [`RowEntry`]. See
    /// [`RangeFileWriter::push`].
    pub(crate) fn push_version(
        &mut self,
        key: &[u8],
        seq: u64,
        kind: Kind,
        create_ts: Option<i64>,
        value: &[u8],
    ) -> io::Result<()> {
        let run = self.run_keys.is_some();
        if kind == Kind::Tombstone && !run {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "range cache bases do not hold tombstones",
            ));
        }
        let same_key = match &self.prev_key {
            Some(prev) => match key.cmp(prev.as_slice()) {
                std::cmp::Ordering::Greater => false,
                std::cmp::Ordering::Equal if run && seq < self.prev_seq => true,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        if run {
                            "range cache run rows must be in ascending key order, newest first"
                        } else {
                            "range cache file rows must be in strictly ascending key order"
                        },
                    ))
                }
            },
            None => false,
        };
        let merge = kind == Kind::Merge;
        if key.len() > u32::MAX as usize || value.len() > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "keys and values are limited to u32::MAX bytes",
            ));
        }
        let page_size = self.page_size;
        let key_ool = key.len() > page_size / 8;
        let key_ref_len = if key_ool {
            OUT_OF_LINE_REF_LEN
        } else {
            INLINE_REF_LEN + key.len()
        };
        let value_ool = ROW_FIXED_LEN + key_ref_len + INLINE_REF_LEN + value.len() > page_size / 4;
        let value_ref_len = if value_ool {
            OUT_OF_LINE_REF_LEN
        } else {
            INLINE_REF_LEN + value.len()
        };
        let row_len = ROW_FIXED_LEN + key_ref_len + value_ref_len;
        if !self.offsets.is_empty()
            && PAGE_HEADER_LEN
                + (ROW_OFFSET_LEN + ROW_HINT_LEN) * (self.offsets.len() + 1)
                + self.body.len()
                + row_len
                > page_size
        {
            self.flush_page()?;
        }
        // Blobs go into the run before the page that holds this row.
        let key_off = if key_ool {
            let off = self.pos;
            self.write_raw(key)?;
            Some(off)
        } else {
            None
        };
        let value_off = if value_ool {
            let off = self.pos;
            self.write_raw(value)?;
            Some(off)
        } else {
            None
        };
        if self.offsets.is_empty() {
            self.first_key.clear();
            self.first_key.extend_from_slice(key);
        }
        self.offsets.push(self.body.len());
        self.hint_keys
            .extend_from_slice(&key[..key.len().min(MAX_HINT_SKIP + ROW_HINT_LEN)]);
        self.hint_key_ends.push(self.hint_keys.len());
        let mut flags = 0;
        if key_ool {
            flags |= FLAG_KEY_OUT_OF_LINE;
        }
        if value_ool {
            flags |= FLAG_VALUE_OUT_OF_LINE;
        }
        if merge {
            flags |= FLAG_MERGE;
        }
        if kind == Kind::Tombstone {
            flags |= FLAG_TOMBSTONE;
        }
        if create_ts.is_some() {
            flags |= FLAG_CREATE_TS;
        }
        self.body.push(flags);
        self.body.extend_from_slice(&seq.to_le_bytes());
        self.body
            .extend_from_slice(&create_ts.unwrap_or(0).to_le_bytes());
        for (bytes, off) in [(key, key_off), (value, value_off)] {
            match off {
                Some(off) => {
                    self.body.extend_from_slice(&off.to_le_bytes());
                    self.body
                        .extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                }
                None => {
                    self.body
                        .extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    self.body.extend_from_slice(bytes);
                }
            }
        }
        self.rows += 1;
        self.data_bytes += (key.len() + value.len()) as u64;
        self.max_seq = self.max_seq.max(seq);
        self.prev_seq = seq;
        if let Some(locators) = &mut self.locators {
            // The row's offset is known when its page is written.
            if !same_key {
                locators.start_key(key);
            }
            locators.rows += 1;
        }
        if !same_key {
            if let Some(keys) = &mut self.run_keys {
                keys.push(hash_key(key));
            }
            match &mut self.prev_key {
                Some(prev) => {
                    prev.clear();
                    prev.extend_from_slice(key);
                }
                None => self.prev_key = Some(key.to_vec()),
            }
        }
        Ok(())
    }

    fn flush_page(&mut self) -> io::Result<()> {
        self.pad_to_page()?;
        let page_off = self.pos;
        self.fences.extend_from_slice(&page_off.to_le_bytes());
        self.fences
            .extend_from_slice(&(self.first_key.len() as u32).to_le_bytes());
        self.fences.extend_from_slice(&self.first_key);

        let n = self.offsets.len();
        let hints = PAGE_HEADER_LEN + ROW_OFFSET_LEN * n;
        let base = hints + ROW_HINT_LEN * n;
        if let Some(locators) = &mut self.locators {
            for off in &self.offsets {
                let at = u32::try_from(page_off + (base + off) as u64).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "a range cache run with row locators must be smaller than 4 GiB",
                    )
                })?;
                locators.offsets.push(at);
            }
        }
        let mut page = std::mem::take(&mut self.page_buf);
        page.fill(0);
        // Keys of a page are sorted, so the first and the last share the prefix that all share.
        let last_key = self.prev_key.as_deref().unwrap_or_default();
        let skip = common_prefix_len(&self.first_key, last_key).min(MAX_HINT_SKIP);
        page[..2].copy_from_slice(&(n as u16).to_le_bytes());
        page[2..4].copy_from_slice(&(skip as u16).to_le_bytes());
        let mut key_start = 0;
        for (i, off) in self.offsets.iter().enumerate() {
            let at = PAGE_HEADER_LEN + ROW_OFFSET_LEN * i;
            page[at..at + 2].copy_from_slice(&((base + off) as u16).to_le_bytes());
            let key = &self.hint_keys[key_start..self.hint_key_ends[i]];
            key_start = self.hint_key_ends[i];
            let hint = prefix32(key.get(skip..).unwrap_or_default());
            let at = hints + ROW_HINT_LEN * i;
            page[at..at + ROW_HINT_LEN].copy_from_slice(&hint.to_be_bytes());
        }
        page[base..base + self.body.len()].copy_from_slice(&self.body);
        let written = self.write_raw(&page);
        self.page_buf = page;
        written?;
        self.body.clear();
        self.offsets.clear();
        self.hint_keys.clear();
        self.hint_key_ends.clear();
        self.pages += 1;
        Ok(())
    }

    /// Writes the last page, the fence region, a run's filter and the trailer, and renames the
    /// file into place. Returns the final path.
    pub(crate) fn finish(mut self) -> io::Result<PathBuf> {
        self.finish_file()
    }

    /// [`RangeFileWriter::finish`] for a writer that collects row locators: returns them too.
    pub(crate) fn finish_with_locators(mut self) -> io::Result<(PathBuf, RowLocators)> {
        let path = self.finish_file()?;
        let mut locators = self.locators.take().unwrap_or_default();
        locators.finish();
        debug_assert_eq!(locators.rows(), locators.rows);
        Ok((path, locators))
    }

    fn finish_file(&mut self) -> io::Result<PathBuf> {
        if !self.offsets.is_empty() {
            self.flush_page()?;
        }
        let fence_off = self.pos;
        let fences = std::mem::take(&mut self.fences);
        self.write_raw(&fences)?;
        let mut filter = Vec::new();
        let mut flags = 0;
        if let Some(keys) = self.run_keys.take() {
            Filter::build(&keys).encode(&mut filter);
            self.write_raw(&filter)?;
            flags |= FILE_FLAG_RUN;
        }
        let mut trailer = Vec::with_capacity(TRAILER_LEN);
        trailer.extend_from_slice(&MAGIC.to_le_bytes());
        trailer.extend_from_slice(&VERSION.to_le_bytes());
        trailer.extend_from_slice(&(self.page_size as u32).to_le_bytes());
        for x in [
            self.pages,
            self.rows,
            fence_off,
            fences.len() as u64,
            self.data_bytes,
            self.max_seq,
            filter.len() as u64,
            flags,
        ] {
            trailer.extend_from_slice(&x.to_le_bytes());
        }
        debug_assert_eq!(trailer.len(), TRAILER_LEN);
        self.write_raw(&trailer)?;
        let out = self
            .out
            .take()
            .ok_or_else(|| io::Error::other("range cache file writer is finished"))?;
        out.into_inner().map_err(|e| e.into_error())?;
        fs::rename(&self.tmp_path, &self.final_path)?;
        Ok(self.final_path.clone())
    }
}

impl Drop for RangeFileWriter {
    fn drop(&mut self) {
        if self.out.take().is_some() {
            let _ = fs::remove_file(&self.tmp_path);
        }
    }
}

/// What the open files of one cache take: their bytes on disk, and their metadata on the heap
/// (fence indexes and run filters), from their open until their last holder drops them. Each
/// file adds its own bytes once when it opens and subtracts them once when it drops.
#[derive(Debug, Default)]
pub(crate) struct FileBudget {
    disk: AtomicU64,
    meta: AtomicU64,
}

impl FileBudget {
    /// A budget that nothing else counts in, for tests.
    #[cfg(test)]
    pub(crate) fn detached() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The bytes on disk of all open files.
    pub(crate) fn disk(&self) -> u64 {
        self.disk.load(Ordering::Relaxed)
    }

    /// The heap bytes of the fence indexes and filters of all open files.
    pub(crate) fn meta(&self) -> u64 {
        self.meta.load(Ordering::Relaxed)
    }
}

/// What [`RangeFile::read_version`] found.
pub(crate) enum FileRead {
    /// A value, copied out with its key.
    Value(RowEntry),
    Tombstone,
    Merge,
}

/// An open, immutable range cache file. Dropping the last reference deletes the file.
pub(crate) struct RangeFile {
    path: PathBuf,
    source: Box<dyn PageSource>,
    page_size: usize,
    page_offsets: Vec<u64>,
    /// The first key of each page, concatenated.
    fence_keys: Vec<u8>,
    /// `fence_key_offsets[i]..fence_key_offsets[i + 1]` is the first key of page `i`.
    fence_key_offsets: Vec<usize>,
    /// The length of the prefix that all fence keys share.
    fence_lcp: usize,
    /// For each page, eight bytes of its fence key after the shared prefix, big-endian and
    /// zero-padded. A search compares these first, in one contiguous array, and compares
    /// whole keys only on a tie.
    fence_prefixes: Vec<u64>,
    rows: u64,
    data_bytes: u64,
    max_seq: u64,
    file_len: u64,
    /// The end of the data pages and blob runs, which is the start of the fence region.
    data_end: u64,
    /// True for a run, which can hold several versions of a key.
    run: bool,
    /// A run's filter, copied from the file, until [`RangeFile::take_filter`].
    filter: Option<Filter>,
    /// The heap bytes of the fence index and the filter, which count as metadata.
    meta_bytes: u64,
    /// The open files of one cache.
    budget: Arc<FileBudget>,
}

impl std::fmt::Debug for RangeFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RangeFile")
            .field("path", &self.path)
            .field("pages", &self.page_offsets.len())
            .field("rows", &self.rows)
            .finish()
    }
}

impl RangeFile {
    /// Opens a finished file. After this call, the `RangeFile` owns the file and deletes it
    /// when it is dropped, also when the open fails.
    pub(crate) fn open(
        path: PathBuf,
        page_source: RangeCachePageSource,
        budget: Arc<FileBudget>,
    ) -> io::Result<Self> {
        match Self::open_inner(&path, page_source, budget) {
            Ok(file) => Ok(file),
            Err(e) => {
                let _ = fs::remove_file(&path);
                Err(e)
            }
        }
    }

    fn open_inner(
        path: &Path,
        page_source: RangeCachePageSource,
        budget: Arc<FileBudget>,
    ) -> io::Result<Self> {
        let file = File::open(path)?;
        let file_len = file.metadata()?.len();
        if file_len < TRAILER_LEN as u64 {
            return Err(corrupt("file is shorter than its trailer"));
        }
        let source = open_page_source(file, file_len, page_source)?;
        let trailer = read_bytes(source.as_ref(), file_len - TRAILER_LEN as u64, TRAILER_LEN)?;
        let t = trailer.as_ref();
        if u64_at(t, 0) != MAGIC {
            return Err(corrupt("bad magic"));
        }
        if u32_at(t, 8) as u32 != VERSION {
            return Err(corrupt("unsupported version"));
        }
        let page_size = u32_at(t, 12);
        let pages = u64_at(t, 16) as usize;
        let rows = u64_at(t, 24);
        let fence_off = u64_at(t, 32);
        let fence_len = u64_at(t, 40);
        let data_bytes = u64_at(t, 48);
        let max_seq = u64_at(t, 56);
        let filter_len = u64_at(t, 64);
        let flags = u64_at(t, 72);
        let run = flags & FILE_FLAG_RUN != 0;
        if !valid_page_size(page_size)
            || flags & !FILE_FLAG_RUN != 0
            || run != (filter_len > 0)
            || fence_off
                .checked_add(fence_len)
                .and_then(|n| n.checked_add(filter_len))
                .and_then(|n| n.checked_add(TRAILER_LEN as u64))
                != Some(file_len)
        {
            return Err(corrupt("inconsistent trailer"));
        }
        let filter = if run {
            let region = read_bytes(source.as_ref(), fence_off + fence_len, filter_len as usize)?;
            Some(Filter::decode(&region).ok_or_else(|| corrupt("bad filter region"))?)
        } else {
            None
        };
        let fence_region = read_bytes(source.as_ref(), fence_off, fence_len as usize)?;
        let raw = fence_region.as_ref();
        let mut page_offsets = Vec::with_capacity(pages);
        let mut fence_keys = Vec::new();
        let mut fence_key_offsets = Vec::with_capacity(pages + 1);
        fence_key_offsets.push(0);
        let mut at = 0;
        for _ in 0..pages {
            if at + 12 > raw.len() {
                return Err(corrupt("truncated fence region"));
            }
            let off = u64_at(raw, at);
            let key_len = u32_at(raw, at + 8);
            if at + 12 + key_len > raw.len()
                || !off.is_multiple_of(page_size as u64)
                || off + page_size as u64 > fence_off
            {
                return Err(corrupt("bad fence entry"));
            }
            page_offsets.push(off);
            fence_keys.extend_from_slice(&raw[at + 12..at + 12 + key_len]);
            fence_key_offsets.push(fence_keys.len());
            at += 12 + key_len;
        }
        if at != raw.len() {
            return Err(corrupt("fence region length mismatch"));
        }
        let fence = |i: usize| &fence_keys[fence_key_offsets[i]..fence_key_offsets[i + 1]];
        // Fences are sorted, so the first and the last share the prefix that all share.
        let fence_lcp = match pages {
            0 => 0,
            _ => common_prefix_len(fence(0), fence(pages - 1)),
        };
        let fence_prefixes: Vec<u64> = (0..pages)
            .map(|i| prefix64(fence(i).get(fence_lcp..).unwrap_or_default()))
            .collect();
        drop(fence_region);
        drop(trailer);
        let meta_bytes = (page_offsets.capacity() * size_of::<u64>()
            + fence_keys.capacity()
            + fence_key_offsets.capacity() * size_of::<usize>()
            + fence_prefixes.capacity() * size_of::<u64>()
            + filter.as_ref().map_or(0, Filter::bytes)) as u64;
        budget.disk.fetch_add(file_len, Ordering::Relaxed);
        budget.meta.fetch_add(meta_bytes, Ordering::Relaxed);
        Ok(Self {
            path: path.to_path_buf(),
            source,
            page_size,
            page_offsets,
            fence_keys,
            fence_key_offsets,
            fence_lcp,
            fence_prefixes,
            rows,
            data_bytes,
            max_seq,
            file_len,
            data_end: fence_off,
            run,
            filter,
            meta_bytes,
            budget,
        })
    }

    /// The heap bytes of the file's fence index and filter.
    pub(crate) fn meta_bytes(&self) -> u64 {
        self.meta_bytes
    }

    pub(crate) fn rows(&self) -> u64 {
        self.rows
    }

    /// True for a run.
    pub(crate) fn is_run(&self) -> bool {
        self.run
    }

    /// Moves a run's filter out of the file, to the run that holds the file.
    pub(crate) fn take_filter(&mut self) -> Option<Filter> {
        self.filter.take()
    }

    /// The first key of the file, or `None` if it holds no row.
    pub(crate) fn first_key(&self) -> Option<&[u8]> {
        (self.page_count() > 0).then(|| self.fence(0))
    }

    /// The last key of the file, or `None` if it holds no row.
    pub(crate) fn last_key(&self) -> io::Result<Option<Bytes>> {
        let Some(last) = self.page_count().checked_sub(1) else {
            return Ok(None);
        };
        let page = self.page(last)?;
        let rows = Self::row_count(&page);
        if rows == 0 {
            return Err(corrupt("empty page"));
        }
        let key = self.key_at(&page, rows - 1)?;
        Ok(Some(Bytes::copy_from_slice(&key)))
    }

    /// The size of the file on disk.
    pub(crate) fn file_len(&self) -> u64 {
        self.file_len
    }

    /// The sum of the key and value lengths of all rows.
    #[allow(dead_code)]
    pub(crate) fn data_bytes(&self) -> u64 {
        self.data_bytes
    }

    /// The greatest sequence number of a row in the file.
    #[allow(dead_code)]
    pub(crate) fn max_seq(&self) -> u64 {
        self.max_seq
    }

    fn page_count(&self) -> usize {
        self.page_offsets.len()
    }

    fn fence(&self, page: usize) -> &[u8] {
        &self.fence_keys[self.fence_key_offsets[page]..self.fence_key_offsets[page + 1]]
    }

    /// The number of pages whose first key is `<= key`.
    fn pages_le(&self, key: &[u8]) -> usize {
        self.pages_before(key, false)
    }

    /// The number of pages whose first key is `< key`.
    fn pages_lt(&self, key: &[u8]) -> usize {
        self.pages_before(key, true)
    }

    /// The number of pages whose first key is `<= key` (`< key` when `strict`).
    fn pages_before(&self, key: &[u8], strict: bool) -> usize {
        let pages = self.page_count();
        if pages == 0 {
            return 0;
        }
        // Compare with the shared prefix first: a key outside it is before or after all pages.
        let lcp = self.fence_lcp;
        let shared = &self.fence(0)[..lcp];
        let head = &key[..lcp.min(key.len())];
        match head.cmp(&shared[..head.len()]) {
            std::cmp::Ordering::Less => return 0,
            std::cmp::Ordering::Greater => return pages,
            std::cmp::Ordering::Equal if key.len() < lcp => return 0,
            std::cmp::Ordering::Equal => {}
        }
        // Padded prefixes that differ order their keys the same way. Equal ones need the keys.
        let target = prefix64(&key[lcp..]);
        let (mut lo, mut hi) = (0, pages);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let prefix = self.fence_prefixes[mid];
            let before = prefix < target
                || (prefix == target
                    && match self.fence(mid).cmp(key) {
                        std::cmp::Ordering::Less => true,
                        std::cmp::Ordering::Equal => !strict,
                        std::cmp::Ordering::Greater => false,
                    });
            if before {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    fn read(&self, offset: u64, len: usize) -> io::Result<Cow<'_, [u8]>> {
        read_bytes(self.source.as_ref(), offset, len)
    }

    fn page(&self, page: usize) -> io::Result<Cow<'_, [u8]>> {
        self.read(self.page_offsets[page], self.page_size)
    }

    fn row_count(page: &[u8]) -> usize {
        u16_at(page, 0)
    }

    /// Returns the bytes of a key or a value of a row in `page`.
    fn resolve<'a>(&'a self, page: &'a [u8], r: &Ref) -> io::Result<Cow<'a, [u8]>> {
        match r {
            Ref::Inline(range) => Ok(Cow::Borrowed(&page[range.clone()])),
            Ref::OutOfLine { offset, len } => self.read(*offset, *len),
        }
    }

    fn key_at<'a>(&'a self, page: &'a [u8], index: usize) -> io::Result<Cow<'a, [u8]>> {
        let raw = parse_row(page, index)?;
        self.resolve(page, &raw.key)
    }

    /// Copies a row out of `page`, with its key and value in one allocation.
    fn decode(&self, page: &[u8], raw: &RawRow) -> io::Result<RowEntry> {
        let key = self.resolve(page, &raw.key)?;
        let value = self.resolve(page, &raw.value)?;
        let (key, value) = copy_key_value(&key, &value);
        Ok(RowEntry {
            key,
            value: value_of(kind_of(raw.flags), value),
            seq: raw.seq,
            create_ts: (raw.flags & FLAG_CREATE_TS != 0).then_some(raw.create_ts),
            expire_ts: None,
        })
    }

    /// The index of the first row in `page` whose key is `>= key` (`> key` when `strict`).
    /// `first` is the first key of the page, its fence.
    fn search(&self, page: &[u8], key: &[u8], strict: bool, first: &[u8]) -> io::Result<usize> {
        self.search_by(page, key, first, |_| strict)
    }

    /// Returns the first row index in `page` not ordered before `(key, equal_before)` using page
    /// key hints and `first` (the page fence).
    fn search_by(
        &self,
        page: &[u8],
        key: &[u8],
        first: &[u8],
        equal_before: impl Fn(u64) -> bool,
    ) -> io::Result<usize> {
        let rows = Self::row_count(page);
        let skip = u16_at(page, 2);
        let hints = PAGE_HEADER_LEN + ROW_OFFSET_LEN * rows;
        if hints + ROW_HINT_LEN * rows > page.len() || skip > first.len() {
            return Err(corrupt("bad page header"));
        }
        // All keys of the page start with `first[..skip]`. A key that does not shares no
        // order with the hints: compare whole keys.
        let target = key
            .get(..skip)
            .filter(|head| *head == &first[..skip])
            .map(|_| prefix32(&key[skip..]));
        let (mut lo, mut hi) = (0, rows);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let at = hints + ROW_HINT_LEN * mid;
            let hint = u32::from_be_bytes([page[at], page[at + 1], page[at + 2], page[at + 3]]);
            // Hints that differ order their keys the same way. Equal ones need the keys.
            let less = match target {
                Some(t) if hint < t => true,
                Some(t) if hint > t => false,
                _ => {
                    let raw = parse_row(page, mid)?;
                    let k = self.resolve(page, &raw.key)?;
                    match k.as_ref().cmp(key) {
                        std::cmp::Ordering::Less => true,
                        std::cmp::Ordering::Equal => equal_before(raw.seq),
                        std::cmp::Ordering::Greater => false,
                    }
                }
            };
            if less {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    /// The sequence number of the first row of `page`.
    fn first_seq(&self, page: usize) -> io::Result<u64> {
        let bytes = self.page(page)?;
        if Self::row_count(&bytes) == 0 {
            return Err(corrupt("empty page"));
        }
        Ok(parse_row(&bytes, 0)?.seq)
    }

    /// Finds the newest version of `key` at or below `seq` (spanning pages if needed) and passes
    /// it to `read`.
    fn find_version<T>(
        &self,
        key: &[u8],
        seq: u64,
        read: impl FnOnce(&Self, &[u8], &RawRow) -> io::Result<T>,
    ) -> io::Result<Option<T>> {
        // Pages whose fence is `key` are the last ones of `..le`: step back over them, usually
        // none or one, instead of a second search.
        let le = self.pages_le(key);
        let mut lt = le;
        while lt > 0 && self.fence(lt - 1) == key {
            lt -= 1;
        }
        let (mut lo, mut hi) = (lt, le);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.first_seq(mid)? > seq {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if let Some(prev) = lo.checked_sub(1) {
            let page = self.page(prev)?;
            let index = self.search_by(&page, key, self.fence(prev), |s| s > seq)?;
            if index < Self::row_count(&page) {
                let raw = parse_row(&page, index)?;
                if self.resolve(&page, &raw.key)?.as_ref() != key {
                    return Ok(None);
                }
                return read(self, &page, &raw).map(Some);
            }
        }
        if lo < self.page_count() {
            let page = self.page(lo)?;
            if Self::row_count(&page) == 0 {
                return Err(corrupt("empty page"));
            }
            let raw = parse_row(&page, 0)?;
            if raw.seq <= seq && self.resolve(&page, &raw.key)?.as_ref() == key {
                return read(self, &page, &raw).map(Some);
            }
        }
        Ok(None)
    }

    /// Returns the newest version of `key` at or below `seq`, if the file holds one. In a base,
    /// [`RangeFile::get`] is faster.
    #[cfg(test)]
    pub(crate) fn get_version(&self, key: &[u8], seq: u64) -> io::Result<Option<RowEntry>> {
        self.find_version(key, seq, |file, page, raw| file.decode(page, raw))
    }

    /// Returns what the newest version of `key` at or below `seq` is, and copies it out if it is a
    /// value.
    pub(crate) fn read_version(&self, key: &[u8], seq: u64) -> io::Result<Option<FileRead>> {
        self.find_version(key, seq, |file, page, raw| {
            Ok(match kind_of(raw.flags) {
                Kind::Value => FileRead::Value(file.decode(page, raw)?),
                Kind::Tombstone => FileRead::Tombstone,
                Kind::Merge => FileRead::Merge,
            })
        })
    }

    /// Returns the row for `key`, if the file holds one.
    pub(crate) fn get(&self, key: &[u8]) -> io::Result<Option<RowEntry>> {
        let pages = self.pages_le(key);
        if pages == 0 {
            return Ok(None);
        }
        let page = self.page(pages - 1)?;
        let index = self.search(&page, key, false, self.fence(pages - 1))?;
        if index < Self::row_count(&page) {
            let raw = parse_row(&page, index)?;
            if self.resolve(&page, &raw.key)?.as_ref() == key {
                return self.decode(&page, &raw).map(Some);
            }
        }
        Ok(None)
    }

    /// The position of the first row at or after `start`, as (page, index), reading pages
    /// through `reader`. `(pages, 0)` is the end.
    fn locate(&self, reader: &mut PageReader, start: Bound<&[u8]>) -> io::Result<(usize, usize)> {
        let (key, strict) = match start {
            Bound::Unbounded => return Ok((0, 0)),
            Bound::Included(k) => (k, false),
            Bound::Excluded(k) => (k, true),
        };
        // Search the page before the first one whose fence is at or after the start. In a run,
        // the first version of `key` can be in a page before the last one whose fence is `key`:
        // so for an included start, that is the last page whose fence is smaller. Rows after
        // `key` are never in a page before the last one whose fence is at most `key`, so an
        // excluded start, and any start in a base (one row per key), use that page.
        let pages = if self.run && !strict {
            self.pages_lt(key)
        } else {
            self.pages_le(key)
        };
        if pages == 0 {
            return Ok((0, 0));
        }
        let page = reader.page(self, pages - 1)?;
        let index = self.search(page, key, strict, self.fence(pages - 1))?;
        if index < Self::row_count(page) {
            Ok((pages - 1, index))
        } else {
            Ok((pages, 0))
        }
    }

    /// The whole file, if the page source maps it.
    pub(super) fn mapped(&self) -> Option<&[u8]> {
        self.source.mapped()
    }

    /// Reads `len` bytes at `offset`. A mapped file returns them without a copy.
    pub(super) fn bytes_at(&self, offset: u64, len: usize) -> io::Result<Cow<'_, [u8]>> {
        self.read(offset, len)
    }

    /// Copies `len` bytes at `offset` into one new buffer: from the map, or with one positional
    /// read.
    pub(super) fn copy_at(&self, offset: u64, len: usize) -> io::Result<Bytes> {
        match self.source.mapped() {
            Some(map) => Ok(Bytes::copy_from_slice(slice_at(map, offset, len)?)),
            None => {
                let mut buf = vec![0u8; len];
                self.source.read_exact_at(&mut buf, offset)?;
                Ok(Bytes::from(buf))
            }
        }
    }

    /// The end of the page that holds the row at `offset`, an offset that a writer's
    /// [`RowLocators`] gave. Pages start at multiples of the page size.
    pub(super) fn page_end(&self, offset: u64) -> io::Result<u64> {
        let page_size = self.page_size as u64;
        let end = offset - offset % page_size + page_size;
        if end > self.data_end {
            return Err(corrupt("row offset past the pages"));
        }
        Ok(end)
    }

    /// Decodes the header of the row at `offset`, an offset that a writer's [`RowLocators`]
    /// gave. The key and the value come back as spans of the file. With positional reads this
    /// reads the row's page.
    pub(super) fn row_at(&self, offset: u64) -> io::Result<RowSpan> {
        let page_end = self.page_end(offset)?;
        let page_start = page_end - self.page_size as u64;
        match self.source.mapped() {
            Some(map) => RowSpan::parse(
                slice_at(map, page_start, self.page_size)?,
                page_start,
                offset,
            ),
            None => RowSpan::parse(&self.read(page_start, self.page_size)?, page_start, offset),
        }
    }

    /// Calls `f` with the offset, the key and the sequence number of every row, in file order.
    #[cfg(test)]
    pub(super) fn for_each_row(
        &self,
        mut f: impl FnMut(u64, &[u8], u64) -> io::Result<()>,
    ) -> io::Result<()> {
        for p in 0..self.page_count() {
            let page = self.page(p)?;
            let rows = Self::row_count(&page);
            if PAGE_HEADER_LEN + (ROW_OFFSET_LEN + ROW_HINT_LEN) * rows > page.len() {
                return Err(corrupt("bad page header"));
            }
            for i in 0..rows {
                let off = u16_at(&page, PAGE_HEADER_LEN + ROW_OFFSET_LEN * i);
                let raw = parse_row_at(&page, off)?;
                let key = self.resolve(&page, &raw.key)?;
                f(self.page_offsets[p] + off as u64, &key, raw.seq)?;
            }
        }
        Ok(())
    }
}

/// The header of one row of a file and where its key and value are, as offsets of the file and
/// lengths. See [`RangeFile::row_at`].
#[derive(Clone, Copy, Debug)]
pub(super) struct RowSpan {
    flags: u8,
    pub(super) seq: u64,
    create_ts: i64,
    pub(super) key: (u64, usize),
    value: (u64, usize),
    /// The file offset just after the row's bytes in its page. A copy of the file from the row's
    /// offset to here holds the row's header and its inline key and value.
    pub(super) end: u64,
}

impl RowSpan {
    /// Decodes the row at file offset `offset` from `bytes`, which hold the file from offset
    /// `at`, up to the row's end at least.
    pub(super) fn parse(bytes: &[u8], at: u64, offset: u64) -> io::Result<Self> {
        let off = offset
            .checked_sub(at)
            .and_then(|off| usize::try_from(off).ok())
            .ok_or_else(|| corrupt("row before its bytes"))?;
        let raw = parse_row_at(bytes, off)?;
        let span = |r: &Ref| match r {
            Ref::Inline(range) => (at + range.start as u64, range.len()),
            Ref::OutOfLine { offset, len } => (*offset, *len),
        };
        Ok(RowSpan {
            flags: raw.flags,
            seq: raw.seq,
            create_ts: raw.create_ts,
            key: span(&raw.key),
            value: span(&raw.value),
            end: at + raw.end as u64,
        })
    }

    pub(super) fn kind(&self) -> Kind {
        kind_of(self.flags)
    }

    /// `len` bytes at file offset `offset`: a slice of `copy`, which holds the file from offset
    /// `at`, if the row holds them inline, or a copy from `file` if they are out of line.
    fn bytes_in(
        inline: bool,
        (offset, len): (u64, usize),
        copy: &Bytes,
        at: u64,
        file: &RangeFile,
    ) -> io::Result<Bytes> {
        if !inline {
            return Ok(Bytes::copy_from_slice(&file.read(offset, len)?));
        }
        let start = offset
            .checked_sub(at)
            .and_then(|s| usize::try_from(s).ok())
            .filter(|s| s + len <= copy.len())
            .ok_or_else(|| corrupt("row outside its copy"))?;
        Ok(copy.slice(start..start + len))
    }

    /// The row's key, as [`RowSpan::entry_in`] returns it.
    pub(super) fn key_in(&self, copy: &Bytes, at: u64, file: &RangeFile) -> io::Result<Bytes> {
        let inline = self.flags & FLAG_KEY_OUT_OF_LINE == 0;
        Self::bytes_in(inline, self.key, copy, at, file)
    }

    /// The row, with its inline key and value as slices of `copy`, which holds the file from
    /// offset `at` up to the row's end at least. Out-of-line keys and values are copied on their
    /// own.
    pub(super) fn entry_in(&self, copy: &Bytes, at: u64, file: &RangeFile) -> io::Result<RowEntry> {
        let kind = self.kind();
        let value = match kind {
            Kind::Tombstone => Bytes::new(),
            _ => {
                let inline = self.flags & FLAG_VALUE_OUT_OF_LINE == 0;
                Self::bytes_in(inline, self.value, copy, at, file)?
            }
        };
        Ok(RowEntry {
            key: self.key_in(copy, at, file)?,
            value: value_of(kind, value),
            seq: self.seq,
            create_ts: (self.flags & FLAG_CREATE_TS != 0).then_some(self.create_ts),
            expire_ts: None,
        })
    }
}

impl Drop for RangeFile {
    fn drop(&mut self) {
        self.budget.disk.fetch_sub(self.file_len, Ordering::Relaxed);
        self.budget
            .meta
            .fetch_sub(self.meta_bytes, Ordering::Relaxed);
        let _ = fs::remove_file(&self.path);
    }
}

/// The largest read of a scan with positional reads.
const MAX_READ_AHEAD_BYTES: usize = 64 * 1024;

/// Reads full pages from memory maps directly or via doubling positional read-ahead up to
/// [`MAX_READ_AHEAD_BYTES`].
pub(crate) struct PageReader {
    buf: Vec<u8>,
    offset: u64,
    next_len: usize,
}

impl PageReader {
    pub(crate) fn new() -> Self {
        Self {
            buf: Vec::new(),
            offset: 0,
            next_len: 0,
        }
    }

    fn page<'a>(&'a mut self, file: &'a RangeFile, page: usize) -> io::Result<&'a [u8]> {
        let offset = file.page_offsets[page];
        let len = file.page_size;
        if let Some(map) = file.source.mapped() {
            return slice_at(map, offset, len);
        }
        let buffered =
            offset >= self.offset && offset + len as u64 <= self.offset + self.buf.len() as u64;
        if !buffered {
            let read_len = self.next_len.max(len) as u64;
            let read_len = read_len.min(file.data_end - offset).max(len as u64) as usize;
            self.buf.resize(read_len, 0);
            file.source.read_exact_at(&mut self.buf, offset)?;
            self.offset = offset;
            self.next_len = (read_len * 2).min(MAX_READ_AHEAD_BYTES);
        }
        let start = (offset - self.offset) as usize;
        Ok(&self.buf[start..start + len])
    }
}

/// Forward cursor over a [`RangeFile`] within a key range.
///
/// Borrows the file on each call so scans do not clone `Arc<RangeFile>`. Copies each entered page
/// tail into one `Bytes` buffer so returned rows share one allocation per page without retaining
/// the file mapping.
pub(crate) struct RangeFileCursor {
    /// The address of the file, to check that each call passes the same one.
    #[cfg(debug_assertions)]
    file: usize,
    end: Bound<Bytes>,
    page: usize,
    index: usize,
    reader: PageReader,
    /// The page that `page_bytes` holds.
    loaded_page: Option<usize>,
    /// A copy of `loaded_page` from the first row that the cursor reads to the end of the
    /// page. Rows are slices of it.
    page_bytes: Bytes,
    /// The offset in the page of the first byte of `page_bytes`.
    page_base: usize,
    /// The row offsets of `loaded_page`.
    row_offsets: Vec<u16>,
    /// The decoded current row, or `None` past the last row in the range.
    current: Option<RawRow>,
    /// The key of the current row, if the row stores it out of line.
    out_of_line_key: Option<Bytes>,
}

impl RangeFileCursor {
    pub(crate) fn new(file: &RangeFile, range: &BytesRange) -> io::Result<Self> {
        let start = range.start_bound().map(|b| b.as_ref());
        Self::with_bounds(file, start, range.end_bound().cloned())
    }

    /// Returns a cursor over the rows of `file` from `start` to `end`.
    pub(crate) fn with_bounds(
        file: &RangeFile,
        start: Bound<&[u8]>,
        end: Bound<Bytes>,
    ) -> io::Result<Self> {
        let mut reader = PageReader::new();
        let (page, index) = file.locate(&mut reader, start)?;
        let mut cursor = Self {
            #[cfg(debug_assertions)]
            file: std::ptr::from_ref(file).addr(),
            end,
            page,
            index,
            reader,
            loaded_page: None,
            page_bytes: Bytes::new(),
            page_base: 0,
            row_offsets: Vec::new(),
            current: None,
            out_of_line_key: None,
        };
        cursor.normalize(file)?;
        Ok(cursor)
    }

    fn check(&self, file: &RangeFile) {
        #[cfg(debug_assertions)]
        debug_assert_eq!(
            self.file,
            std::ptr::from_ref(file).addr(),
            "a range file cursor read through another file"
        );
        let _ = file;
    }

    /// Moves to the next row if the position is past the end of a page, decodes the row, and
    /// checks the end bound.
    fn normalize(&mut self, file: &RangeFile) -> io::Result<()> {
        self.check(file);
        self.current = None;
        self.out_of_line_key = None;
        loop {
            if self.page >= file.page_count() {
                return Ok(());
            }
            if self.loaded_page != Some(self.page) {
                let page = self.reader.page(file, self.page)?;
                let rows = RangeFile::row_count(page);
                if PAGE_HEADER_LEN + (ROW_OFFSET_LEN + ROW_HINT_LEN) * rows > page.len() {
                    return Err(corrupt("bad page header"));
                }
                if self.index >= rows {
                    self.page += 1;
                    self.index = 0;
                    continue;
                }
                self.row_offsets.clear();
                self.row_offsets.extend(
                    (0..rows).map(|i| u16_at(page, PAGE_HEADER_LEN + ROW_OFFSET_LEN * i) as u16),
                );
                // Copy only from the first row that the cursor reads: a short scan that starts
                // in the middle of a page copies half of it.
                let base = self.row_offsets[self.index] as usize;
                if base > page.len() {
                    return Err(corrupt("bad row offset"));
                }
                self.page_bytes = Bytes::copy_from_slice(&page[base..]);
                self.page_base = base;
                self.loaded_page = Some(self.page);
            }
            if self.index >= self.row_offsets.len() {
                self.page += 1;
                self.index = 0;
                continue;
            }
            let Some(off) = (self.row_offsets[self.index] as usize).checked_sub(self.page_base)
            else {
                return Err(corrupt("cursor moved before its page copy"));
            };
            let raw = parse_row_at(&self.page_bytes, off)?;
            if let Ref::OutOfLine { offset, len } = raw.key {
                self.out_of_line_key = Some(Bytes::copy_from_slice(&file.read(offset, len)?));
            }
            let key: &[u8] = match (&raw.key, &self.out_of_line_key) {
                (Ref::Inline(range), _) => &self.page_bytes[range.clone()],
                (Ref::OutOfLine { .. }, Some(key)) => key,
                (Ref::OutOfLine { .. }, None) => return Err(corrupt("missing out-of-line key")),
            };
            let past_end = match &self.end {
                Bound::Unbounded => false,
                Bound::Included(end) => key > end.as_ref(),
                Bound::Excluded(end) => key >= end.as_ref(),
            };
            if !past_end {
                self.current = Some(raw);
            }
            return Ok(());
        }
    }

    /// The key of the current row, or `None` past the end of the range.
    pub(crate) fn key(&self) -> Option<&[u8]> {
        match self.current.as_ref()?.key {
            Ref::Inline(ref range) => Some(&self.page_bytes[range.clone()]),
            Ref::OutOfLine { .. } => self.out_of_line_key.as_deref(),
        }
    }

    /// The sequence number of the current row, or `None` past the end of the range.
    pub(crate) fn seq(&self) -> Option<u64> {
        self.current.as_ref().map(|raw| raw.seq)
    }

    /// Returns the current row as slices of the page buffer, and moves to the next row. The
    /// caller must have seen a key.
    pub(crate) fn take(&mut self, file: &RangeFile) -> io::Result<RowEntry> {
        let Some(raw) = self.current.take() else {
            return Err(corrupt("cursor has no current row"));
        };
        let key = match &raw.key {
            Ref::Inline(range) => self.page_bytes.slice(range.clone()),
            Ref::OutOfLine { .. } => self
                .out_of_line_key
                .take()
                .ok_or_else(|| corrupt("missing out-of-line key"))?,
        };
        let kind = kind_of(raw.flags);
        let value = match &raw.value {
            _ if kind == Kind::Tombstone => Bytes::new(),
            Ref::Inline(range) => self.page_bytes.slice(range.clone()),
            Ref::OutOfLine { offset, len } => Bytes::copy_from_slice(&file.read(*offset, *len)?),
        };
        let row = RowEntry {
            key,
            value: value_of(kind, value),
            seq: raw.seq,
            create_ts: (raw.flags & FLAG_CREATE_TS != 0).then_some(raw.create_ts),
            expire_ts: None,
        };
        self.index += 1;
        self.normalize(file)?;
        Ok(row)
    }

    /// Moves to the next row.
    pub(crate) fn skip(&mut self, file: &RangeFile) -> io::Result<()> {
        if self.current.is_some() {
            self.index += 1;
            self.normalize(file)?;
        }
        Ok(())
    }

    /// Moves forward to the first row at or after `key`. It never moves backwards.
    pub(crate) fn seek(&mut self, file: &RangeFile, key: &[u8]) -> io::Result<()> {
        if self.key().is_none_or(|current| current >= key) {
            return Ok(());
        }
        let (page, index) = file.locate(&mut self.reader, Bound::Included(key))?;
        if (page, index) > (self.page, self.index) {
            self.page = page;
            self.index = index;
        }
        self.normalize(file)
    }
}

/// A [`RowEntryIterator`] over a [`RangeFileCursor`]. Rebuilds and the normal iterator stack
/// use it.
pub(crate) struct RangeFileIterator {
    file: Arc<RangeFile>,
    range: BytesRange,
    cursor: Option<RangeFileCursor>,
}

impl RangeFileIterator {
    pub(crate) fn new(file: Arc<RangeFile>, range: BytesRange) -> Self {
        Self {
            file,
            range,
            cursor: None,
        }
    }

    /// The cursor, and the file that it reads.
    fn cursor(&mut self) -> Result<(&mut RangeFileCursor, &RangeFile), SlateDBError> {
        match self.cursor.as_mut() {
            Some(cursor) => Ok((cursor, &self.file)),
            None => Err(SlateDBError::IteratorNotInitialized),
        }
    }
}

#[async_trait]
impl RowEntryIterator for RangeFileIterator {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        if self.cursor.is_none() {
            self.cursor = Some(RangeFileCursor::new(&self.file, &self.range)?);
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        let (cursor, file) = self.cursor()?;
        if cursor.key().is_none() {
            return Ok(None);
        }
        Ok(Some(cursor.take(file)?))
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        let (cursor, file) = self.cursor()?;
        Ok(cursor.seek(file, next_key)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use std::collections::BTreeMap;

    fn row(key: &[u8], value: &[u8], seq: u64) -> RowEntry {
        RowEntry::new_value(key, value, seq)
    }

    fn build(
        dir: &Path,
        name: &str,
        page_size: usize,
        source: RangeCachePageSource,
        rows: &[RowEntry],
    ) -> Arc<RangeFile> {
        let mut writer = RangeFileWriter::create(dir, name, page_size).unwrap();
        for r in rows {
            writer.push(r).unwrap();
        }
        let path = writer.finish().unwrap();
        Arc::new(RangeFile::open(path, source, FileBudget::detached()).unwrap())
    }

    fn key(i: usize) -> Vec<u8> {
        format!("key{:06}", i * 2).into_bytes()
    }

    fn rows(n: usize, value_len: impl Fn(usize) -> usize) -> Vec<RowEntry> {
        (0..n)
            .map(|i| {
                row(
                    &key(i),
                    &vec![(i % 251) as u8; value_len(i)],
                    1000 + i as u64,
                )
            })
            .collect()
    }

    async fn scan(file: &Arc<RangeFile>, range: BytesRange) -> Vec<RowEntry> {
        let mut iter = RangeFileIterator::new(file.clone(), range);
        iter.init().await.unwrap();
        let mut out = Vec::new();
        while let Some(r) = iter.next().await.unwrap() {
            out.push(r);
        }
        out
    }

    #[rstest]
    #[case::mmap(RangeCachePageSource::Mmap)]
    #[case::positional_read(RangeCachePageSource::PositionalRead)]
    #[tokio::test]
    async fn should_round_trip_rows(#[case] source: RangeCachePageSource) {
        let dir = tempfile::tempdir().unwrap();
        let rows = rows(2000, |i| i % 97);
        let file = build(dir.path(), "rt", 4096, source, &rows);
        assert_eq!(file.rows(), 2000);
        assert!(file.page_count() > 10);
        assert_eq!(file.max_seq(), 2999);
        assert_eq!(scan(&file, BytesRange::unbounded()).await, rows);
        for r in &rows {
            assert_eq!(file.get(&r.key).unwrap().as_ref(), Some(r));
        }
        assert!(!dir.path().join("rt.rcf.tmp").exists());
    }

    #[rstest]
    #[case::mmap(RangeCachePageSource::Mmap)]
    #[case::positional_read(RangeCachePageSource::PositionalRead)]
    #[tokio::test]
    async fn should_find_absent_keys_at_page_boundaries(#[case] source: RangeCachePageSource) {
        let dir = tempfile::tempdir().unwrap();
        let rows = rows(3000, |i| 10 + i % 40);
        let file = build(dir.path(), "pb", 512, source, &rows);
        assert!(file.page_count() > 100);
        for p in 0..file.page_count() {
            let page = file.page(p).unwrap();
            let count = RangeFile::row_count(&page);
            let first = file.key_at(&page, 0).unwrap().to_vec();
            let last = file.key_at(&page, count - 1).unwrap().to_vec();
            assert_eq!(first, file.fence(p));
            assert!(file.get(&first).unwrap().is_some());
            assert!(file.get(&last).unwrap().is_some());
            // Absent keys in the gap before the first key and after the last key of a page.
            let mut before = first.clone();
            *before.last_mut().unwrap() -= 1;
            assert!(file.get(&before).unwrap().is_none());
            let mut after = last.clone();
            after.push(0);
            assert!(file.get(&after).unwrap().is_none());
            let next = scan(
                &file,
                BytesRange::new(Bound::Included(Bytes::from(after)), Bound::Unbounded),
            )
            .await;
            match next.first() {
                Some(r) => assert_eq!(r.key.as_ref(), file.fence(p + 1)),
                None => assert_eq!(p + 1, file.page_count()),
            }
        }
    }

    #[tokio::test]
    async fn should_honor_scan_bounds_and_seek_forward_only() {
        let dir = tempfile::tempdir().unwrap();
        let rows = rows(500, |_| 30);
        let file = build(dir.path(), "seek", 1024, RangeCachePageSource::Mmap, &rows);
        let k = |i: usize| Bytes::from(key(i));
        let got = scan(
            &file,
            BytesRange::new(Bound::Excluded(k(10)), Bound::Included(k(20))),
        )
        .await;
        assert_eq!(got, rows[11..=20]);
        let got = scan(
            &file,
            BytesRange::new(Bound::Included(k(10)), Bound::Excluded(k(20))),
        )
        .await;
        assert_eq!(got, rows[10..20]);

        let mut iter = RangeFileIterator::new(file.clone(), BytesRange::unbounded());
        iter.init().await.unwrap();
        iter.seek(&k(100)).await.unwrap();
        assert_eq!(iter.next().await.unwrap().unwrap().key, k(100));
        // A seek to an earlier key does not move the iterator back.
        iter.seek(&k(50)).await.unwrap();
        assert_eq!(iter.next().await.unwrap().unwrap().key, k(101));
        // A seek to an absent key lands on the next row.
        let mut between = key(300);
        between.push(b'!');
        iter.seek(&between).await.unwrap();
        assert_eq!(iter.next().await.unwrap().unwrap().key, k(301));
        iter.seek(b"zzz").await.unwrap();
        assert!(iter.next().await.unwrap().is_none());

        // At a page boundary, a seek to an earlier key does not move the iterator back.
        let mut iter = RangeFileIterator::new(file.clone(), BytesRange::unbounded());
        iter.init().await.unwrap();
        let first_page_rows = {
            let page = file.page(0).unwrap();
            RangeFile::row_count(&page)
        };
        for _ in 0..first_page_rows {
            iter.next().await.unwrap().unwrap();
        }
        iter.seek(&k(0)).await.unwrap();
        assert_eq!(iter.next().await.unwrap().unwrap().key, k(first_page_rows));
    }

    #[rstest]
    #[case::mmap(RangeCachePageSource::Mmap)]
    #[case::positional_read(RangeCachePageSource::PositionalRead)]
    #[tokio::test]
    async fn should_store_large_keys_and_values_out_of_line(#[case] source: RangeCachePageSource) {
        let dir = tempfile::tempdir().unwrap();
        let page = 512;
        let mut rows = Vec::new();
        for i in 0..200usize {
            let value_len = match i % 6 {
                0 => 0,
                1 => 100,
                2 => page / 4,
                3 => page,
                4 => 3 * page + 7,
                _ => 20_000,
            };
            let mut k = key(i);
            if i % 5 == 0 {
                k.extend(std::iter::repeat_n(b'x', 70 + i));
            }
            let v: Vec<u8> = (0..value_len).map(|j| (j * 31 + i) as u8).collect();
            rows.push(row(&k, &v, i as u64));
        }
        rows.sort_by(|a, b| a.key.cmp(&b.key));
        let file = build(dir.path(), "large", page, source, &rows);
        assert_eq!(scan(&file, BytesRange::unbounded()).await, rows);
        for r in &rows {
            assert_eq!(file.get(&r.key).unwrap().as_ref(), Some(r));
        }
        assert!(file.page_offsets.iter().all(|o| o % page as u64 == 0));
    }

    #[tokio::test]
    async fn should_keep_merge_kind_and_create_ts() {
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![
            row(b"a", b"1", 1).with_create_ts(-5),
            RowEntry::new_merge(b"b", b"2", 2),
            RowEntry::new_merge(b"c", b"3", 3).with_create_ts(7),
            row(b"d", b"", 4),
        ];
        let file = build(dir.path(), "kinds", 4096, RangeCachePageSource::Mmap, &rows);
        assert_eq!(scan(&file, BytesRange::unbounded()).await, rows);
        assert_eq!(file.get(b"c").unwrap(), Some(rows[2].clone()));
    }

    #[tokio::test]
    async fn should_open_an_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = build(dir.path(), "empty", 4096, RangeCachePageSource::Mmap, &[]);
        assert_eq!(file.rows(), 0);
        assert_eq!(file.file_len(), TRAILER_LEN as u64);
        assert!(file.get(b"anything").unwrap().is_none());
        assert!(scan(&file, BytesRange::unbounded()).await.is_empty());
    }

    #[tokio::test]
    async fn should_delete_the_file_when_the_last_reference_drops() {
        let dir = tempfile::tempdir().unwrap();
        let budget = FileBudget::detached();
        let mut writer = RangeFileWriter::create(dir.path(), "life", 4096).unwrap();
        writer.push(&row(b"a", b"1", 1)).unwrap();
        assert!(dir.path().join("life.rcf.tmp").exists());
        let path = writer.finish().unwrap();
        let file = Arc::new(
            RangeFile::open(path.clone(), RangeCachePageSource::Mmap, budget.clone()).unwrap(),
        );
        assert_eq!(budget.disk(), file.file_len());
        assert_eq!(budget.meta(), file.meta_bytes());
        assert!(file.meta_bytes() > 0);
        let mut iter = RangeFileIterator::new(file.clone(), BytesRange::unbounded());
        iter.init().await.unwrap();
        drop(file);
        assert!(path.exists(), "an iterator keeps the file");
        assert_eq!(iter.next().await.unwrap().unwrap().key.as_ref(), b"a");
        drop(iter);
        assert!(!path.exists(), "the last reference deletes the file");
        assert_eq!((budget.disk(), budget.meta()), (0, 0));

        // A writer that does not finish removes its temporary file.
        let mut writer = RangeFileWriter::create(dir.path(), "abandoned", 4096).unwrap();
        writer.push(&row(b"a", b"1", 1)).unwrap();
        drop(writer);
        assert!(!dir.path().join("abandoned.rcf.tmp").exists());
    }

    #[test]
    fn should_count_pages_by_fence_with_prefixes() {
        // The keys share "tenant/00", and blocks of 50 share eight more bytes, so the prefix
        // search often ties and compares whole keys.
        let dir = tempfile::tempdir().unwrap();
        let keys: Vec<Vec<u8>> = (0..600)
            .map(|i| format!("tenant/{:04}/{:08}", i / 50, i).into_bytes())
            .collect();
        let rows: Vec<RowEntry> = keys.iter().map(|k| row(k, b"v", 1)).collect();
        let file = build(dir.path(), "fp", 512, RangeCachePageSource::Mmap, &rows);
        assert!(file.page_count() > 20);
        let fences: Vec<Vec<u8>> = (0..file.page_count())
            .map(|p| file.fence(p).to_vec())
            .collect();
        let mut probes: Vec<Vec<u8>> = Vec::new();
        for k in &keys {
            probes.push(k.clone());
            let mut longer = k.clone();
            longer.push(0);
            probes.push(longer);
            probes.push(k[..k.len() - 1].to_vec());
        }
        for p in [
            &b""[..],
            b"a",
            b"t",
            b"tenant",
            b"tenant/0",
            b"tenant/00",
            b"tenant/01",
            b"tenant/0011/99999999",
            b"tenant/1",
            b"zzz",
        ] {
            probes.push(p.to_vec());
        }
        for probe in &probes {
            let expected = fences
                .iter()
                .filter(|f| f.as_slice() <= probe.as_slice())
                .count();
            assert_eq!(
                file.pages_le(probe),
                expected,
                "{:?}",
                String::from_utf8_lossy(probe)
            );
            // The in-page search, through point reads.
            let present = keys.contains(probe);
            assert_eq!(file.get(probe).unwrap().is_some(), present);
        }
    }

    #[rstest]
    #[case::short_prefix(0)]
    #[case::prefix_longer_than_hints_skip(70)]
    #[tokio::test]
    async fn should_search_pages_by_key_hints(#[case] prefix_len: usize) {
        // Keys of different lengths, some equal in their first bytes after a shared prefix, so
        // hints tie and the search compares whole keys.
        let dir = tempfile::tempdir().unwrap();
        let prefix = "p".repeat(prefix_len);
        let mut keys: Vec<Vec<u8>> = (0..400)
            .flat_map(|i| {
                let base = format!("{prefix}{:03}", i / 4);
                [
                    base.clone(),
                    format!("{base}{:02}", i % 4),
                    format!("{base}{:02}zz", i % 4),
                ]
            })
            .map(String::into_bytes)
            .collect();
        keys.sort();
        keys.dedup();
        let rows: Vec<RowEntry> = keys.iter().map(|k| row(k, b"value", 1)).collect();
        let file = build(dir.path(), "kh", 512, RangeCachePageSource::Mmap, &rows);
        for (i, k) in keys.iter().enumerate() {
            assert_eq!(
                file.get(k).unwrap().map(|r| r.key),
                Some(Bytes::copy_from_slice(k))
            );
            let mut after = k.clone();
            after.push(0);
            assert!(file.get(&after).unwrap().is_none());
            // A scan from just after this key starts at the next key.
            let next = scan(
                &file,
                BytesRange::new(Bound::Excluded(Bytes::copy_from_slice(k)), Bound::Unbounded),
            )
            .await;
            assert_eq!(
                next.first().map(|r| r.key.to_vec()),
                keys.get(i + 1).cloned()
            );
        }
    }

    #[test]
    fn should_reject_bad_rows_and_page_sizes() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = RangeFileWriter::create(dir.path(), "order", 4096).unwrap();
        writer.push(&row(b"b", b"", 1)).unwrap();
        assert!(writer.push(&row(b"b", b"", 2)).is_err());
        assert!(writer.push(&row(b"a", b"", 2)).is_err());
        assert!(writer.push(&RowEntry::new_tombstone(b"c", 2)).is_err());
        assert!(writer.push(&row(b"d", b"", 2).with_expire_ts(10)).is_err());
        assert!(RangeFileWriter::create(dir.path(), "p", 1000).is_err());
        assert!(RangeFileWriter::create(dir.path(), "p", 256).is_err());
        assert!(RangeFileWriter::create(dir.path(), "p", 131072).is_err());
    }

    /// The versions of a run: keys with up to 40 versions each, newest first, values, tombstones
    /// and merge operands, some values large enough to go out of line. With 512-byte pages, the
    /// versions of a key span pages.
    fn run_versions(seed: u64) -> Vec<RowEntry> {
        let mut state = seed | 1;
        let mut next = move |n: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % n
        };
        let mut rows = Vec::new();
        for i in 0..120usize {
            let versions = match i % 7 {
                0 => 40,
                1 => 0,
                _ => 1 + next(6),
            };
            let mut seq = 10_000;
            for v in 0..versions {
                seq -= 1 + next(50);
                let k = key(i);
                let row = match next(5) {
                    0 => RowEntry::new_tombstone(&k, seq),
                    1 => RowEntry::new_merge(&k, format!("+{i}.{v}").as_bytes(), seq),
                    2 => row(&k, &vec![v as u8; 300], seq).with_create_ts(seq as i64),
                    _ => row(&k, format!("{i}.{v}").as_bytes(), seq),
                };
                rows.push(row);
            }
        }
        rows
    }

    fn build_run(
        dir: &Path,
        name: &str,
        source: RangeCachePageSource,
        rows: &[RowEntry],
    ) -> RangeFile {
        let mut writer = RangeFileWriter::create_run(dir, name, 512).unwrap();
        for r in rows {
            writer.push(r).unwrap();
        }
        let path = writer.finish().unwrap();
        RangeFile::open(path, source, FileBudget::detached()).unwrap()
    }

    /// A run holds every version: a scan returns them in order, and a read at any sequence
    /// number finds the newest version at or below it, also when the versions of the key span
    /// pages. Its filter passes every key it holds.
    #[rstest]
    #[case::mmap(RangeCachePageSource::Mmap)]
    #[case::positional_read(RangeCachePageSource::PositionalRead)]
    #[tokio::test]
    async fn should_read_every_version_of_a_run(#[case] source: RangeCachePageSource) {
        let dir = tempfile::tempdir().unwrap();
        let rows = run_versions(7);
        let mut file = build_run(dir.path(), "run", source, &rows);
        assert!(file.is_run());
        assert!(file.page_count() > 50);
        assert_eq!(file.rows(), rows.len() as u64);
        let filter = file.take_filter().unwrap();
        assert!(file.take_filter().is_none());
        let file = Arc::new(file);
        assert_eq!(scan(&file, BytesRange::unbounded()).await, rows);
        assert_eq!(file.first_key(), Some(rows[0].key.as_ref()));
        assert_eq!(
            file.last_key().unwrap(),
            Some(rows.last().unwrap().key.clone())
        );
        // Keys whose versions span more than one page.
        let spanning = (0..file.page_count() - 1)
            .filter(|&p| file.fence(p) == file.fence(p + 1))
            .count();
        assert!(
            spanning > 3,
            "{spanning} pages start with the key of the page before"
        );

        let mut by_key: BTreeMap<Bytes, Vec<&RowEntry>> = BTreeMap::new();
        for r in &rows {
            by_key.entry(r.key.clone()).or_default().push(r);
        }
        for i in 0..120usize {
            let k = Bytes::from(key(i));
            let versions = by_key.get(&k).cloned().unwrap_or_default();
            if !versions.is_empty() {
                assert!(filter.may_contain(hash_key(&k)), "key {i}");
            }
            // Every sequence number where the answer changes, and around it.
            let mut seqs = vec![0, 1, 9_999, 10_000, u64::MAX];
            for v in &versions {
                seqs.extend([v.seq - 1, v.seq, v.seq + 1]);
            }
            for seq in seqs {
                let want = versions.iter().find(|v| v.seq <= seq).map(|v| (*v).clone());
                assert_eq!(file.get_version(&k, seq).unwrap(), want, "key {i} at {seq}");
                let kind = file
                    .read_version(&k, seq)
                    .unwrap()
                    .map(|found| match found {
                        FileRead::Value(row) => {
                            assert_eq!(Some(&row), want.as_ref());
                            Kind::Value
                        }
                        FileRead::Tombstone => Kind::Tombstone,
                        FileRead::Merge => Kind::Merge,
                    });
                let want_kind = want.as_ref().map(|r| match r.value {
                    ValueDeletable::Value(_) => Kind::Value,
                    ValueDeletable::Tombstone => Kind::Tombstone,
                    ValueDeletable::Merge(_) => Kind::Merge,
                });
                assert_eq!(kind, want_kind, "key {i} at {seq}");
            }
            // Absent keys around this one.
            let mut after = key(i);
            after.push(0);
            assert_eq!(file.get_version(&after, u64::MAX).unwrap(), None);
            // A scan from the key starts at its newest version, and one after it at the next key.
            let from = scan(
                &file,
                BytesRange::new(Bound::Included(k.clone()), Bound::Unbounded),
            )
            .await;
            let first = rows.iter().position(|r| r.key >= k).unwrap_or(rows.len());
            assert_eq!(from, rows[first..], "scan from key {i}");
            let past = scan(
                &file,
                BytesRange::new(Bound::Excluded(k.clone()), Bound::Unbounded),
            )
            .await;
            let next = rows.iter().position(|r| r.key > k).unwrap_or(rows.len());
            assert_eq!(past, rows[next..], "scan after key {i}");
            // A cursor that seeks to the key lands on its newest version.
            let mut iter = RangeFileIterator::new(file.clone(), BytesRange::unbounded());
            iter.init().await.unwrap();
            iter.seek(&k).await.unwrap();
            assert_eq!(iter.next().await.unwrap().as_ref(), rows.get(first));
        }
    }

    #[test]
    fn should_keep_bases_and_runs_apart() {
        let dir = tempfile::tempdir().unwrap();
        // A run takes tombstones and older versions of a key, newest first, only.
        let mut writer = RangeFileWriter::create_run(dir.path(), "run", 4096).unwrap();
        writer.push(&row(b"a", b"2", 5)).unwrap();
        writer.push(&RowEntry::new_tombstone(b"a", 4)).unwrap();
        assert!(writer.push(&row(b"a", b"1", 4)).is_err());
        assert!(writer.push(&row(b"a", b"1", 9)).is_err());
        writer.push(&row(b"b", b"1", 9)).unwrap();
        assert!(writer.push(&row(b"a", b"1", 1)).is_err());
        assert!(writer.push(&row(b"c", b"", 2).with_expire_ts(10)).is_err());
        let path = writer.finish().unwrap();
        let mut run =
            RangeFile::open(path, RangeCachePageSource::Mmap, FileBudget::detached()).unwrap();
        assert!(run.is_run());
        assert!(run.take_filter().is_some());
        // A base has no filter, and holds one row per key.
        let base = build(
            dir.path(),
            "base",
            4096,
            RangeCachePageSource::Mmap,
            &[row(b"a", b"1", 1)],
        );
        assert!(!base.is_run());
        assert_eq!(base.file_len(), 4096 + 4 + 8 + 1 + TRAILER_LEN as u64);
    }

    #[test]
    fn should_reject_a_corrupt_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.rcf");
        fs::write(&path, vec![7u8; 200]).unwrap();
        let err = RangeFile::open(
            path.clone(),
            RangeCachePageSource::PositionalRead,
            FileBudget::detached(),
        )
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(!path.exists(), "a file that fails to open is removed");
    }
}
