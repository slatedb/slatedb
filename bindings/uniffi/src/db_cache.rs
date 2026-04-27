use crate::error::Error;
use slatedb::db_cache::{foyer, moka, SplitCache};
use std::sync::Arc;
use std::time::Duration;

/// Options for configuring Foyer DB cache.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FoyerCacheOptions {
    pub max_capacity: u64,
    pub shards: u64,
}

impl From<FoyerCacheOptions> for foyer::FoyerCacheOptions {
    fn from(value: FoyerCacheOptions) -> Self {
        foyer::FoyerCacheOptions {
            max_capacity: value.max_capacity,
            shards: value.shards as usize,
        }
    }
}

/// Options for configuring Moka DB cache.
#[derive(Clone, Debug, uniffi::Record)]
pub struct MokaCacheOptions {
    pub max_capacity: u64,
    pub time_to_live: Option<u64>,
    pub time_to_idle: Option<u64>,
}

impl From<MokaCacheOptions> for moka::MokaCacheOptions {
    fn from(value: MokaCacheOptions) -> Self {
        moka::MokaCacheOptions {
            max_capacity: value.max_capacity,
            time_to_live: value.time_to_live.map(Duration::from_millis),
            time_to_idle: value.time_to_idle.map(Duration::from_millis),
        }
    }
}

/// Database cache used to store blocks in memory.
#[derive(uniffi::Object)]
pub struct DbCache {
    pub(crate) inner: Arc<dyn slatedb::db_cache::DbCache>,
}

#[uniffi::export]
impl DbCache {
    /// Creates a new Foyer based DB cache.
    #[uniffi::constructor]
    pub fn new_foyer_cache(options: FoyerCacheOptions) -> Result<Arc<Self>, Error> {
        Ok(Arc::new(Self {
            inner: Arc::new(foyer::FoyerCache::new_with_opts(options.into())),
        }))
    }

    /// Creates a new Moka based DB cache.
    #[uniffi::constructor]
    pub fn new_moka_cache(options: MokaCacheOptions) -> Result<Arc<Self>, Error> {
        Ok(Arc::new(Self {
            inner: Arc::new(moka::MokaCache::new_with_opts(options.into())),
        }))
    }

    /// Creates a new split cache with separate block and metadata capacities.
    #[uniffi::constructor]
    pub fn new_split_cache(
        block_cache: Arc<Self>,
        meta_cache: Arc<Self>,
    ) -> Result<Arc<Self>, Error> {
        let inner = Arc::new(
            SplitCache::new()
                .with_block_cache(Some(block_cache.inner.clone()))
                .with_meta_cache(Some(meta_cache.inner.clone()))
                .build(),
        ) as Arc<dyn slatedb::db_cache::DbCache>;
        Ok(Arc::new(Self { inner }))
    }
}
