use std::path::Path as StdPath;
use std::str::FromStr;
use std::time::Duration;

use rand::Rng;
use slatedb::config::{
    CompressionCodec, GarbageCollectorDirectoryOptions, GarbageCollectorOptions,
};
use slatedb::{DbRand, Settings};

const MIB_1: usize = 1024 * 1024;
const MIB_500: usize = 500 * MIB_1;
const GIB_2: usize = 2048 * MIB_1;

const COMPRESSION_CODECS: [Option<&str>; 5] = [
    Some("snappy"),
    Some("zlib"),
    Some("lz4"),
    Some("zstd"),
    None,
];

/// Builds a randomized deterministic [`Settings`] value for DST scenarios.
///
/// The returned settings are entirely derived from `rand`. Object-store caching
/// is deterministically enabled or disabled per run; when enabled it uses
/// `cache_root` as the cache directory.
pub async fn build_settings(rand: &DbRand, cache_root: &StdPath) -> Settings {
    let mut rng = rand.rng();
    let flush_interval = rng.random_range(Duration::from_millis(1)..Duration::from_secs(60));
    let manifest_poll_interval = rng.random_range(Duration::from_secs(1)..Duration::from_secs(60));
    let manifest_update_timeout = rng.random_range(Duration::from_secs(1)..Duration::from_secs(60));
    let min_filter_keys = rng.random_range(100..1000);
    let filter_bits_per_key = rng.random_range(1..20);
    let l0_sst_size_bytes = rng.random_range(MIB_1..MIB_500);
    let l0_max_ssts = rng.random_range(4..8);
    let max_unflushed_bytes = rng.random_range(MIB_1..GIB_2);
    let compression_codec_idx = rng.random_range(0..COMPRESSION_CODECS.len());
    let compression_codec =
        if let Some(compression_codec) = COMPRESSION_CODECS[compression_codec_idx] {
            CompressionCodec::from_str(compression_codec).ok()
        } else {
            None
        };
    let object_store_cache_enabled = rng.random_bool(0.5);

    let mut settings = Settings {
        flush_interval: Some(flush_interval),
        manifest_poll_interval,
        manifest_update_timeout,
        min_filter_keys,
        filter_bits_per_key,
        l0_sst_size_bytes,
        l0_max_ssts,
        max_unflushed_bytes,
        compression_codec,
        garbage_collector_options: Some(GarbageCollectorOptions {
            manifest_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(
                    rng.random_range(Duration::from_millis(1)..Duration::from_secs(600)),
                ),
                min_age: rng.random_range(Duration::from_millis(20)..Duration::from_secs(900)),
            }),
            wal_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(
                    rng.random_range(Duration::from_millis(1)..Duration::from_secs(600)),
                ),
                min_age: rng.random_range(Duration::from_millis(20)..Duration::from_secs(900)),
            }),
            compacted_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(
                    rng.random_range(Duration::from_millis(1)..Duration::from_secs(600)),
                ),
                min_age: rng.random_range(Duration::from_millis(20)..Duration::from_secs(900)),
            }),
            compactions_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(
                    rng.random_range(Duration::from_millis(1)..Duration::from_secs(600)),
                ),
                min_age: rng.random_range(Duration::from_millis(20)..Duration::from_secs(900)),
            }),
        }),
        #[cfg(feature = "wal_disable")]
        wal_enabled: rng.random_bool(0.5),
        ..Default::default()
    };

    if object_store_cache_enabled {
        settings.object_store_cache_options.root_folder = Some(cache_root.to_path_buf());
        settings.object_store_cache_options.part_size_bytes = 1024;
        settings.object_store_cache_options.scan_interval = None;
    }

    settings
}
