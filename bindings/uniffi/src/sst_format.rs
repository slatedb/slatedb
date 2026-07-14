use std::sync::Arc;

use parking_lot::Mutex;

use crate::config::SstBlockSize;
use crate::filter_policy::FilterPolicy;
use crate::types::CompressionCodec;

/// SST configuration shared by database writers and readers.
///
/// Configure a format once, then pass it to [`crate::DbBuilder`] and, when
/// applicable, [`crate::DbReaderBuilder`].
#[derive(uniffi::Object)]
pub struct SsTableFormat {
    inner: Mutex<slatedb::SsTableFormat>,
}

impl SsTableFormat {
    pub(crate) fn inner(&self) -> slatedb::SsTableFormat {
        self.inner.lock().clone()
    }
}

#[uniffi::export]
impl SsTableFormat {
    /// Creates an SST format populated with SlateDB defaults.
    #[uniffi::constructor(name = "default")]
    pub fn with_defaults() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(slatedb::SsTableFormat::default()),
        })
    }

    /// Sets the block size used for newly written SST data blocks.
    pub fn with_sst_block_size(&self, block_size: SstBlockSize) {
        let mut format = self.inner.lock();
        *format = format.clone().with_sst_block_size(block_size.into());
    }

    /// Sets the minimum number of keys required before filter blocks are emitted.
    pub fn with_min_filter_keys(&self, min_filter_keys: u32) {
        let mut format = self.inner.lock();
        *format = format.clone().with_min_filter_keys(min_filter_keys);
    }

    /// Sets the filter policies used for SST filter construction and evaluation.
    pub fn with_filter_policies(&self, policies: Vec<Arc<FilterPolicy>>) {
        let policies = policies
            .into_iter()
            .map(|policy| policy.inner.clone())
            .collect();
        let mut format = self.inner.lock();
        *format = format.clone().with_filter_policies(policies);
    }

    /// Sets the compression codec used for SST blocks and metadata blocks.
    pub fn with_compression_codec(&self, compression_codec: Option<CompressionCodec>) {
        let mut format = self.inner.lock();
        *format = format
            .clone()
            .with_compression_codec(compression_codec.map(Into::into));
    }
}
