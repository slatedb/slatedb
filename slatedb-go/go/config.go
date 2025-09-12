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
	"time"
	"unsafe"
)

// Provider types
type Provider string

const (
	ProviderLocal Provider = "local"
	ProviderAWS   Provider = "aws"
)

// SstBlockSize represents SST block size options
type SstBlockSize uint8

const (
	SstBlockSize1Kib  = iota + 1 // 1KiB blocks
	SstBlockSize2Kib             // 2KiB blocks
	SstBlockSize4Kib             // 4KiB blocks (default)
	SstBlockSize8Kib             // 8KiB blocks
	SstBlockSize16Kib            // 16KiB blocks
	SstBlockSize32Kib            // 32KiB blocks
	SstBlockSize64Kib            // 64KiB blocks
)

// AWSConfig contains AWS S3 specific configuration
type AWSConfig struct {
	Bucket         string        `json:"bucket,omitempty"`          // optional, fallback to AWS_BUCKET env var
	Region         string        `json:"region,omitempty"`          // optional, fallback to AWS_REGION env var
	Endpoint       string        `json:"endpoint,omitempty"`        // for S3-compatible storage
	RequestTimeout time.Duration `json:"request_timeout,omitempty"` // HTTP timeout for S3 requests
}

// CompactorOptions contains configuration for the background compactor
type CompactorOptions struct {
	// PollInterval sets how often to check for compaction opportunities
	// If 0, uses SlateDB default (5s)
	PollInterval time.Duration `json:"poll_interval,omitempty"`

	// ManifestUpdateTimeout sets timeout for manifest update operations
	// If 0, uses SlateDB default (300s)
	ManifestUpdateTimeout time.Duration `json:"manifest_update_timeout,omitempty"`

	// MaxSSTSizeBytes sets the target size for output SSTs during compaction
	// If 0, uses SlateDB default (256MB)
	MaxSSTSizeBytes uint64 `json:"max_sst_size_bytes,omitempty"`

	// MaxConcurrentCompactions limits parallel compaction jobs
	// If 0, uses SlateDB default (4)
	MaxConcurrentCompactions uint32 `json:"max_concurrent_compactions,omitempty"`
}

// StoreConfig contains object storage provider configuration
type StoreConfig struct {
	Provider Provider   `json:"provider"`
	AWS      *AWSConfig `json:"aws,omitempty"`
}

// SlateDBOptions contains optional configuration for opening a SlateDB database
type SlateDBOptions struct {
	// L0SstSizeBytes sets the size threshold for L0 SSTable flushes.
	// If 0, uses default (64MB). Larger values reduce API calls but increase recovery time.
	L0SstSizeBytes uint64 `json:"l0_sst_size_bytes,omitempty"`

	// FlushInterval sets how often to flush the WAL to object storage.
	// If 0, uses default (100ms). Lower values reduce latency but increase API costs.
	FlushInterval time.Duration `json:"flush_interval,omitempty"`

	// CacheFolder specifies the local folder for caching SSTables.
	// If empty, no cache is used. If set, enables local caching of SSTables.
	CacheFolder string `json:"cache_folder,omitempty"`

	// SstBlockSize sets the block size for SSTable blocks.
	// If 0, uses default (4KiB). Blocks are the unit of reading and caching.
	SstBlockSize SstBlockSize `json:"sst_block_size,omitempty"`

	// CompactorOptions controls background compaction behavior
	// If nil, uses SlateDB defaults for all compactor settings
	CompactorOptions *CompactorOptions `json:"compactor_options,omitempty"`
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
	ManifestPollInterval uint64 // How often to poll for updates (in milliseconds). Default: 5000 (5s) if 0
	CheckpointLifetime   uint64 // How long checkpoints should live (in milliseconds). Default: 60000 (60s) if 0
	MaxMemtableBytes     uint64 // Max size of in-memory table for WAL buffering. Default: 1048576 (1MB) if 0
}

// ScanOptions contains options for scan operations
type ScanOptions struct {
	DurabilityFilter DurabilityLevel
	Dirty            bool   // Include uncommitted writes (default: false)
	ReadAheadBytes   uint64 // Buffer size for read-ahead (default: 1)
	CacheBlocks      bool   // Whether to cache fetched blocks (default: false)
	MaxFetchTasks    uint64 // Maximum concurrent fetch tasks (default: 1)
}

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

// convertOptionsToJSON converts Go SlateDBOptions to JSON string
// Returns C string and pointer to free. Caller must free the returned pointer.
func convertOptionsToJSON(opts *SlateDBOptions) (*C.char, unsafe.Pointer) {
	if opts == nil {
		return nil, nil // Pass null to C = use all defaults
	}

	jsonBytes, err := json.Marshal(opts)
	if err != nil {
		return nil, nil // Return null on error
	}

	cStr := C.CString(string(jsonBytes))
	return cStr, unsafe.Pointer(cStr)
}
