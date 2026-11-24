//! Scans that cross cached and uncached key ranges.
//!
//! A mixed scan splits its range into ordered parts. Cached parts read pinned layers with
//! [`CachedScan`]. Gaps read the normal path from a state view capped at the scan sequence
//! number. Parts open lazily so early stops or seeks skip unopened gaps.

use std::collections::VecDeque;
use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use super::scan::CachedScan;
use super::stats::ReadCount;
use super::{Layers, RangeCache};
use crate::bytes_range::BytesRange;
use crate::config::ScanOptions;
use crate::db_iter::DbIterator;
use crate::db_state::DbStateView;
use crate::error::SlateDBError;
use crate::iter::RowEntryIterator;
use crate::merge_operator::MergeOperatorType;
use crate::reader::{Reader, ScanContext};
use crate::types::RowEntry;

/// Source of one part of a mixed scan.
pub(crate) enum PartKind {
    Cached(Arc<Layers>),
    Gap,
}

/// A non-empty key range inside a mixed scan.
pub(crate) struct Part {
    pub(crate) kind: PartKind,
    pub(crate) start: Bound<Bytes>,
    pub(crate) end: Bound<Bytes>,
}

/// Inputs used to open gaps on the normal read path.
pub(crate) struct GapSource {
    pub(crate) reader: Reader,
    pub(crate) view: DbStateView,
    pub(crate) options: ScanOptions,
    pub(crate) prefix: Option<Bytes>,
    pub(crate) cache: Arc<RangeCache>,
}

enum Open {
    None,
    Cached(Box<CachedScan>),
    Gap(Box<DbIterator>),
}

/// Iterator that stitches cached parts and normal-path gaps at one sequence number.
pub(crate) struct MixedScan {
    current: Open,
    current_start: Bound<Bytes>,
    current_end: Bound<Bytes>,
    parts: VecDeque<Part>,
    seq: u64,
    point: bool,
    merge_operator: Option<MergeOperatorType>,
    gaps: GapSource,
}

fn ends_before(end: &Bound<Bytes>, key: &[u8]) -> bool {
    match end {
        Unbounded => false,
        Included(end) => end.as_ref() < key,
        Excluded(end) => end.as_ref() <= key,
    }
}

fn starts_before(start: &Bound<Bytes>, key: &[u8]) -> bool {
    match start {
        Unbounded => true,
        Included(start) | Excluded(start) => start.as_ref() < key,
    }
}

impl MixedScan {
    pub(crate) fn new(
        parts: Vec<Part>,
        seq: u64,
        point: bool,
        merge_operator: Option<MergeOperatorType>,
        gaps: GapSource,
    ) -> Self {
        Self {
            current: Open::None,
            current_start: Unbounded,
            current_end: Unbounded,
            parts: parts.into(),
            seq,
            point,
            merge_operator,
            gaps,
        }
    }

    /// Opens the next part synchronously if it is cached.
    fn open_cached(&mut self) -> Result<bool, SlateDBError> {
        if !matches!(
            self.parts.front(),
            Some(Part {
                kind: PartKind::Cached(_),
                ..
            })
        ) {
            return Ok(false);
        }
        let Some(part) = self.parts.pop_front() else {
            return Ok(false);
        };
        let PartKind::Cached(layers) = part.kind else {
            return Ok(false);
        };
        let Some(range) = BytesRange::try_new(part.start.clone(), part.end.clone()) else {
            return Ok(true);
        };
        let mut scan =
            CachedScan::new(vec![layers], self.seq, &range, self.merge_operator.clone())?;
        scan.set_point(self.point);
        self.current = Open::Cached(Box::new(scan));
        self.current_start = part.start;
        self.current_end = part.end;
        Ok(true)
    }

    /// Opens the next part. Returns false when no parts remain.
    async fn open_next(&mut self) -> Result<bool, SlateDBError> {
        if self.open_cached()? {
            return Ok(true);
        }
        let Some(part) = self.parts.pop_front() else {
            return Ok(false);
        };
        let PartKind::Gap = part.kind else {
            return Ok(true);
        };
        let Some(range) = BytesRange::try_new(part.start.clone(), part.end.clone()) else {
            return Ok(true);
        };
        let gaps = &self.gaps;
        gaps.cache.hits.count(ReadCount::ScanGap);
        let iter = gaps
            .reader
            .scan_with_options(
                range,
                &gaps.options,
                ScanContext {
                    db_state: &gaps.view,
                    write_batch_iter: None,
                    max_seq: Some(self.seq),
                    prefix: gaps.prefix.clone(),
                },
            )
            .await?;
        self.current = Open::Gap(Box::new(iter));
        self.current_start = part.start;
        self.current_end = part.end;
        Ok(true)
    }
}

#[async_trait]
impl RowEntryIterator for MixedScan {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        loop {
            match &mut self.current {
                Open::Cached(scan) => {
                    if let Some(entry) = scan.next().await? {
                        return Ok(Some(entry));
                    }
                }
                Open::Gap(iter) => {
                    if let Some(entry) = iter.next_entry().await? {
                        return Ok(Some(entry));
                    }
                }
                Open::None => {
                    if !self.open_next().await? {
                        return Ok(None);
                    }
                    continue;
                }
            }
            self.current = Open::None;
        }
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        if !matches!(self.current, Open::None) {
            if ends_before(&self.current_end, next_key) {
                self.current = Open::None;
            } else if !starts_before(&self.current_start, next_key) {
                return Ok(());
            }
        }
        match &mut self.current {
            Open::Cached(scan) => scan.seek(next_key).await,
            Open::Gap(iter) => iter.seek_entry(next_key).await,
            Open::None => {
                while self
                    .parts
                    .front()
                    .is_some_and(|part| ends_before(&part.end, next_key))
                {
                    self.parts.pop_front();
                }
                if let Some(part) = self.parts.front_mut() {
                    if starts_before(&part.start, next_key) {
                        part.start = Included(Bytes::copy_from_slice(next_key));
                    }
                }
                Ok(())
            }
        }
    }

    fn next_sync(&mut self) -> Option<Result<Option<RowEntry>, SlateDBError>> {
        loop {
            match &mut self.current {
                Open::Cached(scan) => match scan.next_sync()? {
                    Ok(Some(entry)) => return Some(Ok(Some(entry))),
                    Ok(None) => self.current = Open::None,
                    Err(e) => return Some(Err(e)),
                },
                Open::Gap(_) => return None,
                Open::None => {
                    if self.parts.is_empty() {
                        return Some(Ok(None));
                    }
                    match self.open_cached() {
                        Ok(true) => continue,
                        Ok(false) => return None,
                        Err(e) => return Some(Err(e)),
                    }
                }
            }
        }
    }
}
