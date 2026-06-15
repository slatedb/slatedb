//! Typed intents attached to object store calls.
//!
//! SlateDB tags every compacted-SST read and write it issues with a typed
//! intent describing the caller's purpose: a [`WriteIntent`] on writes and a
//! [`ReadIntent`] on reads. "Compacted" names the SST family (the compacted
//! directory), not the origin: L0 flush outputs and compaction outputs alike
//! are compacted SSTs; only WAL segments are not. The intent is inserted into the call's
//! [`Extensions`] (carried by [`GetOptions`](object_store::GetOptions),
//! [`PutOptions`](object_store::PutOptions), and
//! [`PutMultipartOptions`](object_store::PutMultipartOptions)).
//!
//! All other calls carry no intent: WAL segments and fences, manifests,
//! compactions state, the GC boundary file, and calls with no opts-capable
//! variant (`delete`, `list`, and the convenience `head`).

use object_store::Extensions;

/// Declares the purpose of a compacted-SST write issued by SlateDB.
///
/// Attached to the extensions of every compacted-SST write (`put_opts` and
/// multipart upload initiation). A wrapper can use the [`CompactedSstWriteKind`] to
/// decide admission per kind, for example admitting flushes while skipping
/// bulk compaction outputs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteIntent {
    /// The kind of write being performed.
    pub kind: CompactedSstWriteKind,
}

/// The kind of write being performed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompactedSstWriteKind {
    /// A memtable flush producing an L0 SST.
    Flush,
    /// An SST produced by compaction.
    CompactionOutput,
}

impl WriteIntent {
    /// Creates a write intent with the given kind.
    pub fn new(kind: CompactedSstWriteKind) -> Self {
        Self { kind }
    }

    /// A memtable flush write ([`CompactedSstWriteKind::Flush`]).
    pub fn flush() -> Self {
        Self::new(CompactedSstWriteKind::Flush)
    }

    /// A compaction output write ([`CompactedSstWriteKind::CompactionOutput`]).
    pub fn compaction_output() -> Self {
        Self::new(CompactedSstWriteKind::CompactionOutput)
    }

    /// Returns a fresh [`Extensions`] map carrying this intent.
    pub fn into_extensions(self) -> Extensions {
        let mut extensions = Extensions::new();
        extensions.insert(self);
        extensions
    }

    /// Reads the intent back from an [`Extensions`] map, if present.
    pub fn from_extensions(extensions: &Extensions) -> Option<Self> {
        extensions.get::<Self>().copied()
    }
}

/// Declares the purpose of a compacted-SST read issued by SlateDB.
///
/// Attached to the extensions of every compacted-SST read (`get_opts`,
/// including head-style requests issued via `get_opts` with `head` set). A
/// wrapper can use the [`CompactedSstReadKind`] to apply different policies to foreground
/// queries and compaction-input scans, and use [`ReadIntent::retry`] to
/// detect a reissued read after a validation failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadIntent {
    /// The kind of read being performed.
    pub kind: CompactedSstReadKind,
    /// Set when SlateDB reissues a read after the previous response failed
    /// validation (CRC mismatch, block decode error, decompression error).
    ///
    /// A caching wrapper should treat this as a signal that any locally
    /// cached bytes for the path may be corrupt: drop them and fetch fresh
    /// bytes from upstream. A wrapper that keeps serving the same corrupt
    /// bytes would make the retry pointless and surface the validation
    /// failure to the caller.
    pub retry: Option<RetryReason>,
}

/// The kind of read being performed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompactedSstReadKind {
    /// A read on the caller's critical path (point reads and scans). Cache
    /// warmup reads issued via
    /// [`DbCacheManagerOps::warm_sst`](crate::DbCacheManagerOps::warm_sst)
    /// also carry this kind.
    Foreground,
    /// A bulk read of compaction input SSTs.
    CompactionInput,
}

impl ReadIntent {
    /// Creates a read intent with the given kind and no retry reason.
    pub fn new(kind: CompactedSstReadKind) -> Self {
        Self { kind, retry: None }
    }

    /// A foreground read ([`CompactedSstReadKind::Foreground`]).
    pub fn foreground() -> Self {
        Self::new(CompactedSstReadKind::Foreground)
    }

    /// A compaction input read ([`CompactedSstReadKind::CompactionInput`]).
    pub fn compaction_input() -> Self {
        Self::new(CompactedSstReadKind::CompactionInput)
    }

    /// Returns this intent marked as a retry for the given reason.
    pub fn with_retry(self, reason: RetryReason) -> Self {
        Self {
            retry: Some(reason),
            ..self
        }
    }

    /// Returns a fresh [`Extensions`] map carrying this intent.
    pub fn into_extensions(self) -> Extensions {
        let mut extensions = Extensions::new();
        extensions.insert(self);
        extensions
    }

    /// Reads the intent back from an [`Extensions`] map, if present.
    pub fn from_extensions(extensions: &Extensions) -> Option<Self> {
        extensions.get::<Self>().copied()
    }
}

/// Why a read is being reissued after a validation failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryReason {
    /// The previous response failed an SST checksum validation.
    CrcMismatch,
    /// The previous response could not be decoded as a valid SST section.
    BlockDecodeError,
    /// The previous response could not be decompressed.
    DecompressionError,
}

/// Test support for asserting which intents object store calls carry.
#[cfg(test)]
pub(crate) mod testing {
    use std::fmt;
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutOptions,
        PutPayload, PutResult,
    };

    use super::{ReadIntent, WriteIntent};

    /// One object store call observed by [`IntentRecordingObjectStore`], with the
    /// intent it carried, if any.
    #[derive(Clone, Debug)]
    pub(crate) enum RecordedCall {
        Get {
            head: bool,
            intent: Option<ReadIntent>,
        },
        Put {
            intent: Option<WriteIntent>,
        },
        PutMultipart {
            intent: Option<WriteIntent>,
        },
    }

    /// Wraps an object store and records the intent extensions carried by every
    /// get/put/multipart-init call, delegating all I/O to the inner store.
    #[derive(Debug)]
    pub(crate) struct IntentRecordingObjectStore {
        inner: Arc<dyn ObjectStore>,
        calls: parking_lot::Mutex<Vec<RecordedCall>>,
    }

    impl IntentRecordingObjectStore {
        pub(crate) fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                calls: parking_lot::Mutex::new(Vec::new()),
            }
        }

        pub(crate) fn calls(&self) -> Vec<RecordedCall> {
            self.calls.lock().clone()
        }

        pub(crate) fn clear(&self) {
            self.calls.lock().clear();
        }

        /// Intents observed on get calls matching `head`, in call order.
        pub(crate) fn get_intents(&self, head: bool) -> Vec<Option<ReadIntent>> {
            self.calls()
                .into_iter()
                .filter_map(|call| match call {
                    RecordedCall::Get {
                        head: h, intent, ..
                    } if h == head => Some(intent),
                    _ => None,
                })
                .collect()
        }

        /// Intents observed on put and multipart-init calls, in call order.
        pub(crate) fn write_intents(&self) -> Vec<Option<WriteIntent>> {
            self.calls()
                .into_iter()
                .filter_map(|call| match call {
                    RecordedCall::Put { intent, .. }
                    | RecordedCall::PutMultipart { intent, .. } => Some(intent),
                    _ => None,
                })
                .collect()
        }
    }

    impl fmt::Display for IntentRecordingObjectStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "IntentRecordingObjectStore({})", self.inner)
        }
    }

    #[async_trait]
    impl ObjectStore for IntentRecordingObjectStore {
        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.calls.lock().push(RecordedCall::Get {
                head: options.head,
                intent: ReadIntent::from_extensions(&options.extensions),
            });
            self.inner.get_opts(location, options).await
        }

        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.calls.lock().push(RecordedCall::Put {
                intent: WriteIntent::from_extensions(&opts.extensions),
            });
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.calls.lock().push(RecordedCall::PutMultipart {
                intent: WriteIntent::from_extensions(&opts.extensions),
            });
            self.inner.put_multipart_opts(location, opts).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_round_trip_write_intent_through_extensions() {
        let extensions = WriteIntent::compaction_output().into_extensions();

        let intent = WriteIntent::from_extensions(&extensions);

        assert_eq!(
            intent,
            Some(WriteIntent::new(CompactedSstWriteKind::CompactionOutput))
        );
        assert_eq!(ReadIntent::from_extensions(&extensions), None);
    }

    #[test]
    fn should_round_trip_read_intent_through_extensions() {
        let extensions = ReadIntent::foreground()
            .with_retry(RetryReason::CrcMismatch)
            .into_extensions();

        let intent = ReadIntent::from_extensions(&extensions);

        assert_eq!(
            intent,
            Some(ReadIntent {
                kind: CompactedSstReadKind::Foreground,
                retry: Some(RetryReason::CrcMismatch),
            })
        );
        assert_eq!(WriteIntent::from_extensions(&extensions), None);
    }

    #[test]
    fn should_survive_extensions_clone() {
        // Options structs are cloned per retry attempt by RetryingObjectStore;
        // intents must survive the clone.
        let extensions = ReadIntent::compaction_input().into_extensions();

        let cloned = extensions.clone();

        assert_eq!(
            ReadIntent::from_extensions(&cloned),
            Some(ReadIntent::compaction_input())
        );
    }

    #[test]
    fn should_construct_intents_with_expected_kinds() {
        assert_eq!(WriteIntent::flush().kind, CompactedSstWriteKind::Flush);
        assert_eq!(
            WriteIntent::compaction_output().kind,
            CompactedSstWriteKind::CompactionOutput
        );
        assert_eq!(
            ReadIntent::foreground().kind,
            CompactedSstReadKind::Foreground
        );
        assert_eq!(
            ReadIntent::compaction_input().kind,
            CompactedSstReadKind::CompactionInput
        );
        assert_eq!(ReadIntent::foreground().retry, None);
    }
}
