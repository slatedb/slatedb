package slatedb

/*
#include "slatedb.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"
import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"unsafe"
)

type config interface {
	DbConfig | DbReaderConfig | WalReaderConfig
}

type DbConfig struct {
	url     *string
	envFile *string
}

type DbReaderConfig struct {
	url          *string
	envFile      *string
	checkpointId *string
	opts         *DbReaderOptions
}

type WalReaderConfig struct {
	url     *string
	envFile *string
}

type Option[T config] func(*T)

func WithUrl[T config](url string) Option[T] {
	return func(cfg *T) {
		switch c := any(cfg).(type) {
		case *DbConfig:
			c.url = &url
		case *DbReaderConfig:
			c.url = &url
		case *WalReaderConfig:
			c.url = &url
		}
	}
}

func WithEnvFile[T config](envFile string) Option[T] {
	return func(cfg *T) {
		switch c := any(cfg).(type) {
		case *DbConfig:
			c.envFile = &envFile
		case *DbReaderConfig:
			c.envFile = &envFile
		case *WalReaderConfig:
			c.envFile = &envFile
		}
	}
}

func WithCheckpointId(checkpointId string) Option[DbReaderConfig] {
	return func(cfg *DbReaderConfig) {
		cfg.checkpointId = &checkpointId
	}
}

func WithDbReaderOptions(opts DbReaderOptions) Option[DbReaderConfig] {
	return func(cfg *DbReaderConfig) {
		cfg.opts = &opts
	}
}

// ============================================================================
// TYPE DEFINITIONS
// ============================================================================

// DurabilityLevel represents the durability filter for reads/scans.
type DurabilityLevel int

const (
	DurabilityMemory DurabilityLevel = 0 // Default - includes both persisted and in-memory data
	DurabilityRemote DurabilityLevel = 1 // Only data persisted to object storage
)

// PreloadLevel represents different levels of cache preloading on startup.
type PreloadLevel string

const (
	NoPreload PreloadLevel = ""       // Default, no preloading during database startup
	L0Sst     PreloadLevel = "L0Sst"  // Preload only L0 SSTs (most recently written files)
	AllSst    PreloadLevel = "AllSst" // Preload all SSTs (both L0 and compacted levels)
)

// WriteOptions controls write operation behavior.
type WriteOptions struct {
	AwaitDurable bool // Default: true
}

// TTLType represents the type of TTL configuration.
type TTLType int

const (
	TTLDefault     TTLType = iota // Use database default TTL
	TTLNoExpiry                   // Never expire
	TTLExpireAfter                // Expire after specified duration
)

// PutOptions controls put operation behavior.
type PutOptions struct {
	TTLType  TTLType // Type of TTL configuration
	TTLValue uint64  // TTL value in milliseconds (only used with TTLExpireAfter)
}

// MergeOptions controls merge operation behavior.
type MergeOptions struct {
	TTLType  TTLType // Type of TTL configuration
	TTLValue uint64  // TTL value in milliseconds (only used with TTLExpireAfter)
}

// ReadOptions controls read operation behavior.
type ReadOptions struct {
	DurabilityFilter DurabilityLevel // Minimum durability level for returned data
	Dirty            bool            // Whether to include uncommitted data
	CacheBlocks      bool            // Whether to cache fetched blocks
}

// DbReaderOptions controls DbReader behavior.
type DbReaderOptions struct {
	ManifestPollInterval uint64 // How often to poll for updates (in milliseconds). Default: 10000 (10s) if 0
	CheckpointLifetime   uint64 // How long checkpoints should live (in milliseconds). Default: 600000 (10m) if 0
	MaxMemtableBytes     uint64 // Max size of in-memory table for WAL buffering. Default: 67108864 (64MB) if 0
	SkipWalReplay        bool   // When true, skip WAL replay entirely (only see compacted data). Default: false
}

// Settings represents SlateDB configuration that mirrors Rust's Settings struct exactly.
// Duration fields use strings to match Rust's JSON serialization format.
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

	CompactorOptions        *CompactorOptions        `json:"compactor_options,omitempty"`
	ObjectStoreCacheOptions *ObjectStoreCacheOptions `json:"object_store_cache_options,omitempty"`
	GarbageCollectorOptions *GarbageCollectorOptions `json:"garbage_collector_options,omitempty"`
}

// CompactorOptions represents compaction configuration.
type CompactorOptions struct {
	PollInterval             string `json:"poll_interval"`
	ManifestUpdateTimeout    string `json:"manifest_update_timeout"`
	MaxSstSize               uint64 `json:"max_sst_size"`
	MaxConcurrentCompactions uint32 `json:"max_concurrent_compactions"`
}

// ObjectStoreCacheOptions represents object store caching configuration.
type ObjectStoreCacheOptions struct {
	RootFolder                string       `json:"root_folder,omitempty"`
	ScanInterval              string       `json:"scan_interval,omitempty"`
	MaxCacheSizeBytes         uint64       `json:"max_cache_size_bytes,omitempty"`
	PartSizeBytes             uint64       `json:"part_size_bytes,omitempty"`
	CachePuts                 *bool        `json:"cache_puts,omitempty"`
	PreloadDiskCacheOnStartup PreloadLevel `json:"preload_disk_cache_on_startup,omitempty"`
}

// GarbageCollectorOptions represents garbage collection configuration.
//
// Behavior:
// - nil GarbageCollectorOptions: garbage collection disabled
// - Empty GarbageCollectorOptions{}: garbage collection enabled with Rust defaults
// - Specific directory options: configuration applied per directory where set
type GarbageCollectorOptions struct {
	Manifest    *GarbageCollectorDirectoryOptions `json:"manifest_options,omitempty"`
	Wal         *GarbageCollectorDirectoryOptions `json:"wal_options,omitempty"`
	Compacted   *GarbageCollectorDirectoryOptions `json:"compacted_options,omitempty"`
	Compactions *GarbageCollectorDirectoryOptions `json:"compactions_options,omitempty"`
}

// GarbageCollectorDirectoryOptions represents per-directory GC configuration.
//
// Default values match Rust defaults:
// - Interval: "300s" (5 minutes)
// - MinAge: "86400s" (24 hours)
//
// Example override:
//
//	Manifest: &GarbageCollectorDirectoryOptions{Interval: "60s", MinAge: "1h"}
type GarbageCollectorDirectoryOptions struct {
	Interval string `json:"interval"` // Default: "300s" (5 minutes)
	MinAge   string `json:"min_age"`  // Default: "86400s" (24 hours)
}

// DefaultGarbageCollectorDirectoryOptions returns Rust default values.
func DefaultGarbageCollectorDirectoryOptions() *GarbageCollectorDirectoryOptions {
	return &GarbageCollectorDirectoryOptions{
		Interval: "300s",   // 5 minutes
		MinAge:   "86400s", // 24 hours
	}
}

// SstBlockSize represents SST block size options.
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

// ScanOptions contains options for scan operations.
//
// Note: pass nil scan options to scan methods to use full Rust defaults.
type ScanOptions struct {
	DurabilityFilter DurabilityLevel
	Dirty            bool   // Include uncommitted writes (default: false)
	ReadAheadBytes   uint64 // Buffer size for read-ahead (default from Rust)
	CacheBlocks      bool   // Whether to cache fetched blocks (default from Rust)
	MaxFetchTasks    uint64 // Maximum concurrent fetch tasks (default from Rust)
}

// ============================================================================
// SETTINGS CONSTRUCTORS
// ============================================================================

func settingsFromJSON(jsonBytes []byte) (*Settings, error) {
	var settings Settings
	if err := json.Unmarshal(jsonBytes, &settings); err != nil {
		return nil, fmt.Errorf("failed to unmarshal settings JSON: %w", err)
	}
	return &settings, nil
}

func settingsFromHandle(handle *C.slatedb_settings_t) (*Settings, error) {
	if handle == nil {
		return nil, errors.New("settings handle is nil")
	}

	var jsonPtr *C.uint8_t
	var jsonLen C.uintptr_t
	result := C.slatedb_settings_to_json(handle, &jsonPtr, &jsonLen)
	if err := resultToErrorAndFree(result); err != nil {
		return nil, err
	}
	defer C.slatedb_bytes_free(jsonPtr, jsonLen)

	settings, err := settingsFromJSON(C.GoBytes(unsafe.Pointer(jsonPtr), C.int(jsonLen)))
	if err != nil {
		return nil, err
	}
	return settings, nil
}

// SettingsDefault creates default Settings.
func SettingsDefault() (*Settings, error) {
	var handle *C.slatedb_settings_t
	result := C.slatedb_settings_default(&handle)
	if err := resultToErrorAndFree(result); err != nil {
		return nil, err
	}
	defer closeSettingsHandle(handle)
	return settingsFromHandle(handle)
}

// SettingsFromFile loads Settings from a configuration file.
//
// Supported file types: `.json`, `.toml`, `.yaml`, `.yml`.
func SettingsFromFile(path string) (*Settings, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	var handle *C.slatedb_settings_t
	result := C.slatedb_settings_from_file(cPath, &handle)
	if err := resultToErrorAndFree(result); err != nil {
		return nil, err
	}
	defer closeSettingsHandle(handle)
	return settingsFromHandle(handle)
}

// SettingsFromEnv loads Settings from environment variables.
//
// `prefix` is the environment variable prefix used by SlateDB settings parsing.
func SettingsFromEnv(prefix string) (*Settings, error) {
	cPrefix := C.CString(prefix)
	defer C.free(unsafe.Pointer(cPrefix))

	var handle *C.slatedb_settings_t
	result := C.slatedb_settings_from_env(cPrefix, &handle)
	if err := resultToErrorAndFree(result); err != nil {
		return nil, err
	}
	defer closeSettingsHandle(handle)
	return settingsFromHandle(handle)
}

// SettingsLoad loads Settings using SlateDB's auto-detection logic.
//
// This follows the Rust `Settings::load()` behavior.
func SettingsLoad() (*Settings, error) {
	var handle *C.slatedb_settings_t
	result := C.slatedb_settings_load(&handle)
	if err := resultToErrorAndFree(result); err != nil {
		return nil, err
	}
	defer closeSettingsHandle(handle)
	return settingsFromHandle(handle)
}

func closeSettingsHandle(handle *C.slatedb_settings_t) {
	if handle == nil {
		return
	}
	_ = resultToErrorAndFree(C.slatedb_settings_close(handle))
}

// ============================================================================
// MERGE FUNCTIONS
// ============================================================================

// MergeSettings merges two Settings structs, with override taking precedence over base.
func MergeSettings(base, override *Settings) *Settings {
	if base == nil && override == nil {
		return &Settings{}
	}
	if base == nil {
		return override
	}
	if override == nil {
		return base
	}

	result := *base

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

	if override.WalEnabled != nil {
		result.WalEnabled = override.WalEnabled
	}
	if override.CompactorOptions != nil {
		if result.CompactorOptions == nil {
			result.CompactorOptions = override.CompactorOptions
		} else {
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
			if override.ObjectStoreCacheOptions.PreloadDiskCacheOnStartup != NoPreload {
				merged.PreloadDiskCacheOnStartup = override.ObjectStoreCacheOptions.PreloadDiskCacheOnStartup
			}
			result.ObjectStoreCacheOptions = &merged
		}
	}
	if override.GarbageCollectorOptions != nil {
		if result.GarbageCollectorOptions == nil {
			result.GarbageCollectorOptions = override.GarbageCollectorOptions
		} else {
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
			if override.GarbageCollectorOptions.Compactions != nil {
				merged.Compactions = mergeGarbageCollectorDirectoryOptions(merged.Compactions, override.GarbageCollectorOptions.Compactions)
			}
			result.GarbageCollectorOptions = &merged
		}
	}

	return &result
}

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

// ============================================================================
// FFI CONVERTER FUNCTIONS
// ============================================================================

func convertToCScanOptions(opts *ScanOptions) *C.slatedb_scan_options_t {
	if opts == nil {
		return nil
	}

	return &C.slatedb_scan_options_t{
		durability_filter: C.uint8_t(opts.DurabilityFilter),
		dirty:             C.bool(opts.Dirty),
		read_ahead_bytes:  C.uint64_t(opts.ReadAheadBytes),
		cache_blocks:      C.bool(opts.CacheBlocks),
		max_fetch_tasks:   C.uint64_t(opts.MaxFetchTasks),
	}
}

func convertToCWriteOptions(opts *WriteOptions) *C.slatedb_write_options_t {
	if opts == nil {
		opts = &WriteOptions{AwaitDurable: true}
	}
	return &C.slatedb_write_options_t{await_durable: C.bool(opts.AwaitDurable)}
}

func convertToCPutOptions(opts *PutOptions) *C.slatedb_put_options_t {
	if opts == nil {
		return nil
	}
	return &C.slatedb_put_options_t{
		ttl_type:  C.uint8_t(opts.TTLType),
		ttl_value: C.uint64_t(opts.TTLValue),
	}
}

func convertToCMergeOptions(opts *MergeOptions) *C.slatedb_merge_options_t {
	if opts == nil {
		return nil
	}
	return &C.slatedb_merge_options_t{
		ttl_type:  C.uint8_t(opts.TTLType),
		ttl_value: C.uint64_t(opts.TTLValue),
	}
}

func convertToCReadOptions(opts *ReadOptions) *C.slatedb_read_options_t {
	if opts == nil {
		return nil
	}
	return &C.slatedb_read_options_t{
		durability_filter: C.uint8_t(opts.DurabilityFilter),
		dirty:             C.bool(opts.Dirty),
		cache_blocks:      C.bool(opts.CacheBlocks),
	}
}

func convertToCReaderOptions(opts *DbReaderOptions) *C.slatedb_db_reader_options_t {
	if opts == nil {
		return nil
	}

	return &C.slatedb_db_reader_options_t{
		manifest_poll_interval_ms: C.uint64_t(opts.ManifestPollInterval),
		checkpoint_lifetime_ms:    C.uint64_t(opts.CheckpointLifetime),
		max_memtable_bytes:        C.uint64_t(opts.MaxMemtableBytes),
		skip_wal_replay:           C.bool(opts.SkipWalReplay),
	}
}

// ============================================================================
// OBJECT STORE URL RESOLUTION
// ============================================================================

func resolveObjectStoreURL(url *string, envFile *string) (string, bool, error) {
	if url != nil && strings.TrimSpace(*url) != "" {
		return strings.TrimSpace(*url), true, nil
	}

	if directURL, ok := lookupDirectURL(nil); ok {
		return directURL, true, nil
	}

	if envFile != nil && strings.TrimSpace(*envFile) != "" {
		envValues, err := loadEnvFile(*envFile)
		if err != nil {
			return "", false, err
		}
		if directURL, ok := lookupDirectURL(envValues); ok {
			return directURL, true, nil
		}
	}

	return "", false, nil
}

func lookupDirectURL(envValues map[string]string) (string, bool) {
	for _, key := range []string{"SLATEDB_OBJECT_STORE_URL", "OBJECT_STORE_URL"} {
		if value, ok := os.LookupEnv(key); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value), true
		}
		if envValues != nil {
			if value, ok := envValues[key]; ok && strings.TrimSpace(value) != "" {
				return strings.TrimSpace(value), true
			}
		}
	}
	return "", false
}

func loadEnvFile(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open env file: %w", err)
	}
	defer file.Close()

	values := map[string]string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}

		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		value = strings.Trim(value, `"'`)
		if key != "" {
			values[key] = value
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read env file: %w", err)
	}

	return values, nil
}
