#![cfg(feature = "foyer")]
#![allow(
    clippy::disallowed_types,
    clippy::disallowed_methods,
    clippy::disallowed_macros
)]

use slatedb::config::{FlushOptions, FlushType, Settings};
use slatedb::db_cache::foyer::FoyerCache;
use slatedb::object_store::memory::InMemory;
use slatedb::Db;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

static EVENTS: AtomicUsize = AtomicUsize::new(0);

struct Logs;

impl log::Log for Logs {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }
    fn log(&self, record: &log::Record<'_>) {
        if record.target() == "cache-loader-check" {
            EVENTS.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn flush(&self) {}
}

#[tokio::test]
async fn cache_loaders_preserve_log_forwarding_without_subscriber() {
    log::set_logger(&Logs).unwrap();
    log::set_max_level(log::LevelFilter::Info);
    let store = Arc::new(InMemory::new());
    let db = Db::builder("cache-logging", store.clone())
        .with_settings(Settings {
            compactor_options: None,
            ..Settings::default()
        })
        .with_db_cache(Arc::new(FoyerCache::new()), 0)
        .build()
        .await
        .unwrap();
    db.put(b"key", b"value").await.unwrap();
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    db.close().await.unwrap();
    let db = Db::builder("cache-logging", store)
        .with_settings(Settings {
            compactor_options: None,
            ..Settings::default()
        })
        .with_db_cache(Arc::new(FoyerCache::new()), 0)
        .build()
        .await
        .unwrap();
    tracing::info!(target: "cache-loader-check", "before");
    assert_eq!(EVENTS.load(Ordering::Relaxed), 1);
    assert_eq!(db.get(b"key").await.unwrap().unwrap().as_ref(), b"value");
    tracing::info!(target: "cache-loader-check", "after");
    assert_eq!(EVENTS.load(Ordering::Relaxed), 2);
    db.close().await.unwrap();
}
