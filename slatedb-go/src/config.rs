use std::path::PathBuf;
use std::time::Duration;
use std::sync::Arc;

use serde::Deserialize;
use slatedb::config::{
    Settings, ScanOptions, DurabilityLevel, SstBlockSize, CompactorOptions,
    WriteOptions, PutOptions, ReadOptions, DbReaderOptions, Ttl
};
use slatedb::object_store::ObjectStore;
use slatedb::bytes::Bytes;
use std::ops::Bound;
use crate::types::{CSdbWriteOptions, CSdbPutOptions, CSdbReadOptions, CSdbScanOptions};
use crate::db_reader::CSdbReaderOptions;

#[derive(Debug, Clone, Deserialize)]
pub struct AwsConfigJson {
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub request_timeout: Option<u64>, // timeout in nanoseconds
}



#[derive(Debug, Clone, Deserialize)]
pub struct StoreConfigJson {
    pub provider: String,  // "local", "aws"
    pub aws: Option<AwsConfigJson>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CompactorOptionsJson {
    pub poll_interval: Option<u64>,  // duration in nanoseconds
    pub manifest_update_timeout: Option<u64>,  // timeout in nanoseconds
    pub max_sst_size_bytes: Option<u64>,
    pub max_concurrent_compactions: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SlateDBOptionsJson {
    pub l0_sst_size_bytes: Option<u64>,
    pub flush_interval: Option<u64>, // interval in nanoseconds
    pub cache_folder: Option<String>,
    pub sst_block_size: Option<u8>,
    pub compactor_options: Option<CompactorOptionsJson>,
}

// Parsed configuration types

#[derive(Debug, Clone)]
pub struct ParsedStoreConfig {
    pub provider: String,
    pub aws_config: Option<AwsConfigJson>,
}

#[derive(Debug, Clone)]
pub struct ParsedDbOptions {
    pub settings: Settings,
    pub sst_block_size: Option<SstBlockSize>,
}

// Configuration parsing errors
#[derive(Debug)]
pub enum ConfigError {
    JsonParseError(serde_json::Error),
    InvalidProvider(String),
    InvalidBlockSize(u8),
}

impl From<serde_json::Error> for ConfigError {
    fn from(err: serde_json::Error) -> Self {
        ConfigError::JsonParseError(err)
    }
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::JsonParseError(e) => write!(f, "JSON parse error: {}", e),
            ConfigError::InvalidProvider(p) => write!(f, "Invalid provider: {}", p),
            ConfigError::InvalidBlockSize(s) => write!(f, "Invalid block size: {}", s),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConfigError::JsonParseError(e) => Some(e),
            _ => None,
        }
    }
}


// Configuration parsing functions

pub fn parse_store_config(json_str: &str) -> Result<ParsedStoreConfig, ConfigError> {
    let config: StoreConfigJson = serde_json::from_str(json_str)?;
    
    // Validate provider
    match config.provider.as_str() {
        "local" | "aws" => {},
        _ => return Err(ConfigError::InvalidProvider(config.provider)),
    }
    
    Ok(ParsedStoreConfig {
        provider: config.provider,
        aws_config: config.aws,
    })
}

pub fn parse_db_options(json_str: Option<&str>) -> Result<ParsedDbOptions, ConfigError> {
    let mut settings = Settings::default();
    let mut sst_block_size = None;
    
    if let Some(json) = json_str {
        let options: SlateDBOptionsJson = serde_json::from_str(json)?;
        
        // Convert L0 SST size
        if let Some(l0_size) = options.l0_sst_size_bytes {
            if l0_size > 0 {
                settings.l0_sst_size_bytes = l0_size as usize;
            }
        }
        
        // Convert flush interval (Go duration is in nanoseconds)
        if let Some(flush_interval_ns) = options.flush_interval {
            if flush_interval_ns > 0 {
                settings.flush_interval = Some(Duration::from_nanos(flush_interval_ns));
            }
        }
        
        // Convert cache folder
        if let Some(cache_folder) = options.cache_folder {
            if !cache_folder.is_empty() {
                settings.object_store_cache_options.root_folder = Some(PathBuf::from(cache_folder));
            }
        }
        
        // Convert sst_block_size from u8 to SstBlockSize enum
        if let Some(block_size) = options.sst_block_size {
            sst_block_size = match block_size {
                1 => Some(SstBlockSize::Block1Kib),
                2 => Some(SstBlockSize::Block2Kib),
                3 => Some(SstBlockSize::Block4Kib),
                4 => Some(SstBlockSize::Block8Kib),
                5 => Some(SstBlockSize::Block16Kib),
                6 => Some(SstBlockSize::Block32Kib),
                7 => Some(SstBlockSize::Block64Kib),
                _ => return Err(ConfigError::InvalidBlockSize(block_size)),
            };
        }
        
        // Convert compactor options 
        settings.compactor_options = convert_compactor_options_json(options.compactor_options);
    } else {
        // No options provided, use defaults with default compaction
        settings.compactor_options = Some(CompactorOptions::default());
    }
    
    Ok(ParsedDbOptions {
        settings,
        sst_block_size,
    })
}

// Convert JSON compactor options to Rust CompactorOptions
fn convert_compactor_options_json(opts: Option<CompactorOptionsJson>) -> Option<CompactorOptions> {
    let defaults = CompactorOptions::default();
    
    match opts {
        None => Some(defaults),
        Some(opts) => Some(CompactorOptions {
            poll_interval: if let Some(interval_ns) = opts.poll_interval {
                if interval_ns > 0 {
                    Duration::from_nanos(interval_ns)
                } else {
                    defaults.poll_interval
                }
            } else {
                defaults.poll_interval
            },
            
            manifest_update_timeout: if let Some(timeout_ns) = opts.manifest_update_timeout {
                if timeout_ns > 0 {
                    Duration::from_nanos(timeout_ns)
                } else {
                    defaults.manifest_update_timeout
                }
            } else {
                defaults.manifest_update_timeout
            },
            
            max_sst_size: if let Some(size) = opts.max_sst_size_bytes {
                if size > 0 {
                    size as usize
                } else {
                    defaults.max_sst_size
                }
            } else {
                defaults.max_sst_size
            },
            
            max_concurrent_compactions: if let Some(count) = opts.max_concurrent_compactions {
                if count > 0 {
                    count as usize
                } else {
                    defaults.max_concurrent_compactions
                }
            } else {
                defaults.max_concurrent_compactions
            },
        })
    }
}

// Object store creation helper

pub async fn create_object_store(config: &ParsedStoreConfig) -> Result<Arc<dyn ObjectStore>, crate::error::CSdbResult> {
    use crate::object_store::{create_inmemory_store, create_aws_store};
    use crate::error::{CSdbError, create_error_result};
    
    match config.provider.as_str() {
        "local" => create_inmemory_store(),
        "aws" => {
            if let Some(aws_cfg) = &config.aws_config {
                create_aws_store(aws_cfg).await
            } else {
                Err(create_error_result(CSdbError::InvalidArgument, "AWS config required for 'aws' provider"))
            }
        }
        _ => {
            Err(create_error_result(CSdbError::InvalidArgument, &format!("Unsupported provider: {}", config.provider)))
        }
    }
}

// Convert C options to Rust options

// Convert C scan options to Rust ScanOptions
pub fn convert_scan_options(c_opts: *const CSdbScanOptions) -> ScanOptions {
    if c_opts.is_null() {
        return ScanOptions::default();
    }
    
    let opts = unsafe { &*c_opts };
    let durability = match opts.durability_filter {
        1 => DurabilityLevel::Memory,
        _ => DurabilityLevel::Remote,
    };
    
    ScanOptions::new()
        .with_durability_filter(durability)
        .with_dirty(opts.dirty)
        .with_read_ahead_bytes(opts.read_ahead_bytes as usize)
        .with_cache_blocks(opts.cache_blocks)
}

// Convert C write options to Rust WriteOptions
pub fn convert_write_options(c_opts: *const CSdbWriteOptions) -> WriteOptions {
    if c_opts.is_null() {
        return WriteOptions { await_durable: true };
    }
    
    let opts = unsafe { &*c_opts };
    WriteOptions { 
        await_durable: opts.await_durable 
    }
}

// Convert C put options to Rust PutOptions
pub fn convert_put_options(c_opts: *const CSdbPutOptions) -> PutOptions {
    if c_opts.is_null() {
        return PutOptions::default();
    }
    
    let opts = unsafe { &*c_opts };
    let ttl = match opts.ttl_type {
        0 => Ttl::Default,
        1 => Ttl::NoExpiry,
        2 => Ttl::ExpireAfter(opts.ttl_value),
        _ => Ttl::Default, // fallback
    };
    
    PutOptions { ttl }
}

// Convert C read options to Rust ReadOptions
pub fn convert_read_options(c_opts: *const CSdbReadOptions) -> ReadOptions {
    if c_opts.is_null() {
        return ReadOptions::default();
    }
    
    let opts = unsafe { &*c_opts };
    let durability_filter = match opts.durability_filter {
        0 => DurabilityLevel::Remote,
        1 => DurabilityLevel::Memory,
        _ => DurabilityLevel::Memory, // fallback
    };
    
    ReadOptions {
        durability_filter,
        dirty: opts.dirty,
    }
}

// Convert C range bounds to Rust range bounds
// Returns (start_bound, end_bound) tuple that can be used to create ranges
pub fn convert_range_bounds(
    start: *const u8, start_len: usize,
    end: *const u8, end_len: usize,
) -> (Bound<Bytes>, Bound<Bytes>) {
    let start_bound = if start.is_null() {
        Bound::Unbounded
    } else {
        let start_slice = unsafe { std::slice::from_raw_parts(start, start_len) };
        Bound::Included(Bytes::copy_from_slice(start_slice))
    };
    
    let end_bound = if end.is_null() {
        Bound::Unbounded  
    } else {
        let end_slice = unsafe { std::slice::from_raw_parts(end, end_len) };
        Bound::Excluded(Bytes::copy_from_slice(end_slice))  // Exclusive end bound
    };
    
    (start_bound, end_bound)
}

// Convert C reader options to Rust DbReaderOptions
pub fn convert_reader_options(c_opts: *const CSdbReaderOptions) -> DbReaderOptions {
    if c_opts.is_null() {
        return DbReaderOptions::default();
    }
    
    let opts = unsafe { &*c_opts };
    DbReaderOptions {
        manifest_poll_interval: Duration::from_millis(opts.manifest_poll_interval_ms),
        checkpoint_lifetime: Duration::from_millis(opts.checkpoint_lifetime_ms),
        max_memtable_bytes: opts.max_memtable_bytes,
        block_cache: DbReaderOptions::default().block_cache, // Use default
    }
}
