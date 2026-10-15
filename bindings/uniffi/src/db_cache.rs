use crate::error::{Error, SlateDbError};
use slatedb::db_cache::{foyer, moka, CachedEntry, CachedKey, SplitCache};
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
    bounded_foyer: Option<::foyer::Cache<CachedKey, CachedEntry>>,
}

#[uniffi::export]
impl DbCache {
    /// Creates a new Foyer based DB cache.
    #[uniffi::constructor]
    pub fn new_foyer_cache(options: FoyerCacheOptions) -> Result<Arc<Self>, Error> {
        Ok(Arc::new(Self {
            inner: Arc::new(foyer::FoyerCache::new_with_opts(options.into())),
            bounded_foyer: None,
        }))
    }

    /// Creates a FIFO cache with a ceiling on indexed entry weight.
    ///
    /// Capacity and shards must be positive, and shards cannot exceed capacity.
    /// Entries weigh at least one byte. Entries larger than capacity / shards
    /// are loaded successfully without admission. Retained values and pending
    /// loads are outside this ceiling; it is not an RSS limit.
    #[uniffi::constructor]
    pub fn new_bounded_foyer_cache(options: FoyerCacheOptions) -> Result<Arc<Self>, Error> {
        let capacity = usize::try_from(options.max_capacity).map_err(|_| {
            SlateDbError::ValueTooLargeForUsize {
                field: "max_capacity",
            }
        })?;
        let shards = usize::try_from(options.shards)
            .map_err(|_| SlateDbError::ValueTooLargeForUsize { field: "shards" })?;
        if capacity == 0 || shards == 0 || shards > capacity {
            return Err(Error::Invalid {
                message: "bounded cache requires positive capacity and 1 <= shards <= capacity"
                    .to_owned(),
            });
        }
        let max_entry_weight = capacity / shards;
        let cache = ::foyer::CacheBuilder::new(capacity)
            .with_weighter(|_, v: &CachedEntry| v.size().max(1))
            .with_filter(move |_, v: &CachedEntry| v.size().max(1) <= max_entry_weight)
            .with_shards(shards)
            .with_eviction_config(::foyer::FifoConfig::default())
            .build();
        Ok(Arc::new(Self {
            inner: Arc::new(foyer::FoyerCache::new_with_cache(cache.clone())),
            bounded_foyer: Some(cache),
        }))
    }

    /// Returns the current indexed weight for a bounded Foyer cache.
    ///
    /// Returns None for ordinary Foyer, Moka and split caches. For a split cache,
    /// retain its bounded child handles and observe each child separately.
    /// Shards are sampled separately. Evicted values retained by readers and
    /// pending loads are excluded; this observation does not measure RSS.
    pub fn indexed_weight(&self) -> Option<u64> {
        self.bounded_foyer
            .as_ref()
            .map(|cache| cache.usage() as u64)
    }

    /// Creates a new Moka based DB cache.
    #[uniffi::constructor]
    pub fn new_moka_cache(options: MokaCacheOptions) -> Result<Arc<Self>, Error> {
        Ok(Arc::new(Self {
            inner: Arc::new(moka::MokaCache::new_with_opts(options.into())),
            bounded_foyer: None,
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
        Ok(Arc::new(Self {
            inner,
            bounded_foyer: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_binding_validates_options() {
        for (max_capacity, shards) in [(0, 1), (1, 0), (1, 2), (1, u64::MAX)] {
            assert!(matches!(
                DbCache::new_bounded_foyer_cache(FoyerCacheOptions {
                    max_capacity,
                    shards,
                }),
                Err(Error::Invalid { .. })
            ));
        }
    }

    #[cfg(target_pointer_width = "32")]
    #[test]
    fn bounded_binding_rejects_shards_conversion_overflow() {
        let error = DbCache::new_bounded_foyer_cache(FoyerCacheOptions {
            max_capacity: 1,
            shards: u64::from(u32::MAX) + 1,
        })
        .err()
        .unwrap();
        assert!(
            matches!(error, Error::Invalid { ref message } if message == "shards is too large")
        );
    }

    #[test]
    fn bounded_binding_observation_identifies_supported_cache() {
        let cache = DbCache::new_bounded_foyer_cache(FoyerCacheOptions {
            max_capacity: 64,
            shards: 1,
        })
        .unwrap();
        assert_eq!(cache.indexed_weight(), Some(0));

        let ordinary = DbCache::new_foyer_cache(FoyerCacheOptions {
            max_capacity: 64,
            shards: 1,
        })
        .unwrap();
        assert_eq!(ordinary.indexed_weight(), None);
        let moka = DbCache::new_moka_cache(MokaCacheOptions {
            max_capacity: 64,
            time_to_live: None,
            time_to_idle: None,
        })
        .unwrap();
        assert_eq!(moka.indexed_weight(), None);
        let split = DbCache::new_split_cache(cache.clone(), cache).unwrap();
        assert_eq!(split.indexed_weight(), None);
    }
}
