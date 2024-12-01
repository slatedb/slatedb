#[cfg(feature = "foyer")]
pub mod foyer;
#[cfg(feature = "moka")]
pub mod moka;

mod db_cache;

pub use db_cache::{CachedEntry, CachedKey, DbCache, DEFAULT_MAX_CAPACITY};
