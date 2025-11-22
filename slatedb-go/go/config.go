package slatedb

/*
#cgo LDFLAGS: -lslatedb_go
#include "slatedb.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"
import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unsafe"
)

// ============================================================================
// TYPE DEFINITIONS
// ============================================================================

// Provider types
type Provider string

const (
	ProviderLocal Provider = "local"
	ProviderAWS   Provider = "aws"
)

// AWSConfig contains AWS S3 specific configuration
type AWSConfig struct {
	Bucket         string        `json:"bucket,omitempty"`          // optional, fallback to AWS_BUCKET env var
	Region         string        `json:"region,omitempty"`          // optional, fallback to AWS_REGION env var
	Endpoint       string        `json:"endpoint,omitempty"`        // for S3-compatible storage
	RequestTimeout time.Duration `json:"request_timeout,omitempty"` // HTTP timeout for S3 requests
}

// StoreConfig contains object storage provider configuration
type StoreConfig struct {
	Provider Provider   `json:"provider"`
	AWS      *AWSConfig `json:"aws,omitempty"`
}

// DurabilityLevel represents the durability filter for scans
type DurabilityLevel int

const (
	DurabilityMemory DurabilityLevel = 0 // Default - includes both persisted and in-memory data
	DurabilityRemote DurabilityLevel = 1 // Only data persisted to object storage
)

// WriteOptions controls write operation behavior
type WriteOptions struct {
	AwaitDurable bool // Default: true
}

// TTLType represents the type of TTL configuration
type TTLType int

const (
	TTLDefault     TTLType = iota // Use database default TTL
	TTLNoExpiry                   // Never expire
	TTLExpireAfter                // Expire after specified duration
)

// PutOptions controls put operation behavior
type PutOptions struct {
	TTLType  TTLType // Type of TTL configuration
	TTLValue uint64  // TTL value in milliseconds (only used with TTLExpireAfter)
}

// ReadOptions controls read operation behavior
type ReadOptions struct {
	DurabilityFilter DurabilityLevel // Minimum durability level for returned data
	Dirty            bool            // Whether to include uncommitted data
}

// DbReaderOptions controls DbReader behavior
type DbReaderOptions struct {
	ManifestPollInterval uint64 // How often to poll for updates (in milliseconds). Default: 10000 (10s) if 0
	CheckpointLifetime   uint64 // How long checkpoints should live (in milliseconds). Default: 600000 (10m) if 0
	MaxMemtableBytes     uint64 // Max size of in-memory table for WAL buffering. Default: 67108864 (64MB) if 0
}

// Settings represents SlateDB configuration that mirrors Rust's Settings struct exactly
// Duration fields use strings to match Rust's JSON serialization format
type Settings struct {
	FlushInterval         string `json:"flush_interval,omitempty"`
	ManifestPollInterval  string `json:"manifest_poll_interval,omitempty"`
	ManifestUpdateTimeout string `json:"manifest_update_timeout,omitempty"`
	CompressionCodec      string `json:"compression_codec,omitempty"`
	DefaultTTL            string `json:"default_ttl,omitempty"`

	MinFilterKeys     uint32 `json:"min_filter_keys,omitempty"`
	FilterBitsPerKey  uint32 `json:"filter_bits_per_key,omitempty"`
	L0SstSizeBytes    uint64 `json:"l0_sst_size_bytes,omitempty"`
	L0MaxSsts         uint32 `json:"l0_max_ssts,omitempty"`
	MaxUnflushedBytes uint64 `json:"max_unflushed_bytes,omitempty"`

	WalEnabled *bool `json:"wal_enabled,omitempty"`

	MetaCachePreload       *string `json:"meta_cache_preload,omitempty"`
	MetaCacheUpdateOnWrite bool    `json:"meta_cache_update_on_write"`

	CompactorOptions        *CompactorOptions        `json:"compactor_options,omitempty"`
	ObjectStoreCacheOptions *ObjectStoreCacheOptions `json:"object_store_cache_options,omitempty"`
	GarbageCollectorOptions *GarbageCollectorOptions `json:"garbage_collector_options,omitempty"`
}

// CompactorOptions represents compaction configuration
type CompactorOptions struct {
	PollInterval             string `json:"poll_interval"`
	ManifestUpdateTimeout    string `json:"manifest_update_timeout"`
	MaxSstSize               uint64 `json:"max_sst_size"`
	MaxConcurrentCompactions uint32 `json:"max_concurrent_compactions"`
}

// ObjectStoreCacheOptions represents object store caching configuration
type ObjectStoreCacheOptions struct {
	RootFolder                string `json:"root_folder,omitempty"`
	ScanInterval              string `json:"scan_interval,omitempty"`
	MaxCacheSizeBytes         uint64 `json:"max_cache_size_bytes,omitempty"`
	PartSizeBytes             uint64 `json:"part_size_bytes,omitempty"`
	CachePuts                 *bool  `json:"cache_puts,omitempty"`
	PreloadDiskCacheOnStartup *bool  `json:"preload_disk_cache_on_startup,omitempty"`
}

// GarbageCollectorOptions represents garbage collection configuration
//
// Behavior:
// - nil GarbageCollectorOptions: Garbage collection disabled
// - Empty GarbageCollectorOptions{}: Garbage collection enabled with Rust defaults
// - Specific directory options: Garbage collection enabled for all directories, options applied to specific directories
type GarbageCollectorOptions struct {
	Manifest  *GarbageCollectorDirectoryOptions `json:"manifest_options,omitempty"`
	Wal       *GarbageCollectorDirectoryOptions `json:"wal_options,omitempty"`
	Compacted *GarbageCollectorDirectoryOptions `json:"compacted_options,omitempty"`
}

// GarbageCollectorDirectoryOptions represents per-directory GC configuration
//
// Default values match Rust defaults:
// - Interval: "300s" (5 minutes)
// - MinAge: "86400s" (24 hours)
//
// Override as needed:
//
//	Manifest: &GarbageCollectorDirectoryOptions{Interval: "60s", MinAge: "1h"}
type GarbageCollectorDirectoryOptions struct {
	Interval string `json:"interval"` // Default: "300s" (5 minutes)
	MinAge   string `json:"min_age"`  // Default: "86400s" (24 hours)
}

// DefaultGarbageCollectorDirectoryOptions returns Rust default values
func DefaultGarbageCollectorDirectoryOptions() *GarbageCollectorDirectoryOptions {
	return &GarbageCollectorDirectoryOptions{
		Interval: "300s",   // 5 minutes
		MinAge:   "86400s", // 24 hours
	}
}

// SstBlockSize represents SST block size options
type SstBlockSize uint8

const (
	SstBlockSize1Kib SstBlockSize = iota + 1
	SstBlockSize2Kib
	SstBlockSize4Kib
	SstBlockSize8Kib
	SstBlockSize16Kib
	SstBlockSize32Kib
	SstBlockSize64Kib
)

// ============================================================================
// SETTINGS CONSTRUCTORS
// ============================================================================

// settingsFromJSON converts JSON from Rust FFI into a Settings struct
// This handles Rust's string durations ("100ms", "1s") transparently
func settingsFromJSON(jsonStr string) (*Settings, error) {
	var settings Settings
	if err := json.Unmarshal([]byte(jsonStr), &settings); err != nil {
		return nil, fmt.Errorf("failed to unmarshal settings JSON: %w", err)
	}
	return &settings, nil
}

// SettingsDefault creates default Settings
func SettingsDefault() (*Settings, error) {
	cJson := C.slatedb_settings_default()
	if cJson == nil {
		return nil, errors.New("failed to create default settings")
	}
	defer C.free(unsafe.Pointer(cJson))

	jsonStr := C.GoString(cJson)
	return settingsFromJSON(jsonStr)
}

// SettingsFromFile loads Settings from a configuration file
// The result is always merged with defaults, so partial configurations work correctly
func SettingsFromFile(path string) (*Settings, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	cJson := C.slatedb_settings_from_file(cPath)
	if cJson == nil {
		return nil, fmt.Errorf("failed to load settings from file: %s", path)
	}
	defer C.free(unsafe.Pointer(cJson))

	jsonStr := C.GoString(cJson)
	return settingsFromJSON(jsonStr)
}

// SettingsFromEnv loads Settings from environment variables
// The result is always merged with defaults, so partial configurations work correctly
func SettingsFromEnv(prefix string) (*Settings, error) {
	cPrefix := C.CString(prefix)
	defer C.free(unsafe.Pointer(cPrefix))

	cJson := C.slatedb_settings_from_env(cPrefix)
	if cJson == nil {
		return nil, fmt.Errorf("failed to load settings from environment with prefix: %s", prefix)
	}
	defer C.free(unsafe.Pointer(cJson))

	jsonStr := C.GoString(cJson)
	return settingsFromJSON(jsonStr)
}

// SettingsLoad loads Settings using auto-detection (files, env vars, etc.)
// The result is always merged with defaults, so partial configurations work correctly
func SettingsLoad() (*Settings, error) {
	cJson := C.slatedb_settings_load()
	if cJson == nil {
		return nil, errors.New("failed to load settings using auto-detection")
	}
	defer C.free(unsafe.Pointer(cJson))

	jsonStr := C.GoString(cJson)
	return settingsFromJSON(jsonStr)
}

// ============================================================================
// MERGE FUNCTIONS
// ============================================================================

// MergeSettings merges two Settings structs, with override taking precedence over base
func MergeSettings(base, override *Settings) *Settings {
	if base == nil && override == nil {
		return &Settings{} // Return empty settings instead of nil
	}
	if base == nil {
		return override
	}
	if override == nil {
		return base
	}

	result := *base // Copy base

	if override.FlushInterval != "" {
		result.FlushInterval = override.FlushInterval
	}
	if override.ManifestPollInterval != "" {
		result.ManifestPollInterval = override.ManifestPollInterval
	}
	if override.ManifestUpdateTimeout != "" {
		result.ManifestUpdateTimeout = override.ManifestUpdateTimeout
	}
	if override.CompressionCodec != "" {
		result.CompressionCodec = override.CompressionCodec
	}
	if override.DefaultTTL != "" {
		result.DefaultTTL = override.DefaultTTL
	}

	if override.MinFilterKeys != 0 {
		result.MinFilterKeys = override.MinFilterKeys
	}
	if override.FilterBitsPerKey != 0 {
		result.FilterBitsPerKey = override.FilterBitsPerKey
	}
	if override.L0SstSizeBytes != 0 {
		result.L0SstSizeBytes = override.L0SstSizeBytes
	}
	if override.L0MaxSsts != 0 {
		result.L0MaxSsts = override.L0MaxSsts
	}
	if override.MaxUnflushedBytes != 0 {
		result.MaxUnflushedBytes = override.MaxUnflushedBytes
	}

	if override.MetaCachePreload != nil {
		result.MetaCachePreload = override.MetaCachePreload
	}
	result.MetaCacheUpdateOnWrite = override.MetaCacheUpdateOnWrite

	if override.WalEnabled != nil {
		result.WalEnabled = override.WalEnabled
	}
	if override.CompactorOptions != nil {
		if result.CompactorOptions == nil {
			result.CompactorOptions = override.CompactorOptions
		} else {
			// Merge CompactorOptions fields individually
			merged := *result.CompactorOptions
			if override.CompactorOptions.PollInterval != "" {
				merged.PollInterval = override.CompactorOptions.PollInterval
			}
			if override.CompactorOptions.ManifestUpdateTimeout != "" {
				merged.ManifestUpdateTimeout = override.CompactorOptions.ManifestUpdateTimeout
			}
			if override.CompactorOptions.MaxSstSize != 0 {
				merged.MaxSstSize = override.CompactorOptions.MaxSstSize
			}
			if override.CompactorOptions.MaxConcurrentCompactions != 0 {
				merged.MaxConcurrentCompactions = override.CompactorOptions.MaxConcurrentCompactions
			}
			result.CompactorOptions = &merged
		}
	}
	if override.ObjectStoreCacheOptions != nil {
		if result.ObjectStoreCacheOptions == nil {
			result.ObjectStoreCacheOptions = override.ObjectStoreCacheOptions
		} else {
			// Merge ObjectStoreCacheOptions fields individually
			merged := *result.ObjectStoreCacheOptions
			if override.ObjectStoreCacheOptions.RootFolder != "" {
				merged.RootFolder = override.ObjectStoreCacheOptions.RootFolder
			}
			if override.ObjectStoreCacheOptions.ScanInterval != "" {
				merged.ScanInterval = override.ObjectStoreCacheOptions.ScanInterval
			}
			if override.ObjectStoreCacheOptions.MaxCacheSizeBytes != 0 {
				merged.MaxCacheSizeBytes = override.ObjectStoreCacheOptions.MaxCacheSizeBytes
			}
			if override.ObjectStoreCacheOptions.PartSizeBytes != 0 {
				merged.PartSizeBytes = override.ObjectStoreCacheOptions.PartSizeBytes
			}
			if override.ObjectStoreCacheOptions.CachePuts != nil {
				merged.CachePuts = override.ObjectStoreCacheOptions.CachePuts
			}
			if override.ObjectStoreCacheOptions.PreloadDiskCacheOnStartup != nil {
				merged.PreloadDiskCacheOnStartup = override.ObjectStoreCacheOptions.PreloadDiskCacheOnStartup
			}
			result.ObjectStoreCacheOptions = &merged
		}
	}
	if override.GarbageCollectorOptions != nil {
		if result.GarbageCollectorOptions == nil {
			result.GarbageCollectorOptions = override.GarbageCollectorOptions
		} else {
			// Merge GarbageCollectorOptions fields individually
			merged := *result.GarbageCollectorOptions
			if override.GarbageCollectorOptions.Manifest != nil {
				merged.Manifest = mergeGarbageCollectorDirectoryOptions(merged.Manifest, override.GarbageCollectorOptions.Manifest)
			}
			if override.GarbageCollectorOptions.Wal != nil {
				merged.Wal = mergeGarbageCollectorDirectoryOptions(merged.Wal, override.GarbageCollectorOptions.Wal)
			}
			if override.GarbageCollectorOptions.Compacted != nil {
				merged.Compacted = mergeGarbageCollectorDirectoryOptions(merged.Compacted, override.GarbageCollectorOptions.Compacted)
			}
			result.GarbageCollectorOptions = &merged
		}
	}

	return &result
}

// Helper functions for nested struct merging

// mergeGarbageCollectorDirectoryOptions merges two GarbageCollectorDirectoryOptions
func mergeGarbageCollectorDirectoryOptions(base, override *GarbageCollectorDirectoryOptions) *GarbageCollectorDirectoryOptions {
	if base == nil {
		return override
	}
	if override == nil {
		return base
	}

	merged := *base
	if override.Interval != "" {
		merged.Interval = override.Interval
	}
	if override.MinAge != "" {
		merged.MinAge = override.MinAge
	}
	return &merged
}

// ScanOptions contains options for scan operations
type ScanOptions struct {
	DurabilityFilter DurabilityLevel
	Dirty            bool   // Include uncommitted writes (default: false)
	ReadAheadBytes   uint64 // Buffer size for read-ahead (default: 1)
	CacheBlocks      bool   // Whether to cache fetched blocks (default: false)
	MaxFetchTasks    uint64 // Maximum concurrent fetch tasks (default: 1)
}

// ============================================================================
// FFI CONVERTER FUNCTIONS
// ============================================================================

// convertToCScanOptions converts Go ScanOptions to C ScanOptions
func convertToCScanOptions(opts *ScanOptions) *C.CSdbScanOptions {
	if opts == nil {
		return nil // Use default options
	}

	return &C.CSdbScanOptions{
		durability_filter: C.int(int32(opts.DurabilityFilter)),
		dirty:             C.bool(opts.Dirty),
		read_ahead_bytes:  C.uint64_t(opts.ReadAheadBytes),
		cache_blocks:      C.bool(opts.CacheBlocks),
		max_fetch_tasks:   C.uint64_t(opts.MaxFetchTasks),
	}
}

// Helper functions to convert Go options to C options

// convertToCWriteOptions converts Go WriteOptions to C WriteOptions
func convertToCWriteOptions(opts *WriteOptions) *C.CSdbWriteOptions {
	if opts == nil {
		opts = &WriteOptions{AwaitDurable: true}
	}
	return &C.CSdbWriteOptions{
		await_durable: C.bool(opts.AwaitDurable),
	}
}

// convertToCPutOptions converts Go PutOptions to C PutOptions
func convertToCPutOptions(opts *PutOptions) *C.CSdbPutOptions {
	if opts == nil {
		opts = &PutOptions{TTLType: TTLDefault}
	}
	return &C.CSdbPutOptions{
		ttl_type:  C.uint32_t(opts.TTLType),
		ttl_value: C.uint64_t(opts.TTLValue),
	}
}

// convertToCReadOptions converts Go ReadOptions to C ReadOptions
func convertToCReadOptions(opts *ReadOptions) *C.CSdbReadOptions {
	if opts == nil {
		opts = &ReadOptions{DurabilityFilter: DurabilityMemory, Dirty: false}
	}
	return &C.CSdbReadOptions{
		durability_filter: C.uint32_t(opts.DurabilityFilter),
		dirty:             C.bool(opts.Dirty),
	}
}

// convertToCReaderOptions converts Go DbReaderOptions to C CSdbReaderOptions
func convertToCReaderOptions(opts *DbReaderOptions) *C.CSdbReaderOptions {
	if opts == nil {
		return nil
	}

	return &C.CSdbReaderOptions{
		manifest_poll_interval_ms: C.uint64_t(opts.ManifestPollInterval),
		checkpoint_lifetime_ms:    C.uint64_t(opts.CheckpointLifetime),
		max_memtable_bytes:        C.uint64_t(opts.MaxMemtableBytes),
	}
}

// JSON serialization functions

// convertStoreConfigToJSON converts Go StoreConfig to JSON string
// Returns C string and pointer to free. Caller must free the returned pointer.
func convertStoreConfigToJSON(config *StoreConfig) (*C.char, unsafe.Pointer) {
	if config == nil {
		return nil, nil
	}

	jsonBytes, err := json.Marshal(config)
	if err != nil {
		return nil, nil // Return null on error
	}

	cStr := C.CString(string(jsonBytes))
	return cStr, unsafe.Pointer(cStr)
}
