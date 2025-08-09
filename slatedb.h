#pragma once
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * The default max capacity for the cache. (64MB)
 */
#define DEFAULT_MAX_CAPACITY ((64 * 1024) * 1024)

#define ENUM_MIN_COMPRESSION_FORMAT 0

#define ENUM_MAX_COMPRESSION_FORMAT 4

#define ENUM_MIN_BOUND_TYPE 0

#define ENUM_MAX_BOUND_TYPE 3

#define ENUM_MIN_CHECKPOINT_METADATA 0

#define ENUM_MAX_CHECKPOINT_METADATA 1

/**
 * Internal struct that owns a `tokio` runtime and a SlateDB instance.
 */
typedef struct SlateDbFFI SlateDbFFI;

typedef struct SlateDbHandle {
  struct SlateDbFFI *_0;
} SlateDbHandle;

/**
 * Callback type used for iteration. The data pointed to by `key_ptr` and
 * `val_ptr` is only valid for the duration of the callback. Copy the bytes if
 * you need to hold on to them.
 */
typedef void (*SlateDbIterCallback)(const uint8_t *key_ptr,
                                    unsigned int key_len,
                                    const uint8_t *val_ptr,
                                    unsigned int val_len,
                                    void *userdata);

#define SsTableInfo_VT_FIRST_KEY 4

#define SsTableInfo_VT_INDEX_OFFSET 6

#define SsTableInfo_VT_INDEX_LEN 8

#define SsTableInfo_VT_FILTER_OFFSET 10

#define SsTableInfo_VT_FILTER_LEN 12

#define SsTableInfo_VT_COMPRESSION_FORMAT 14

#define BlockMeta_VT_OFFSET 4

#define SsTableIndex_VT_BLOCK_META 4

#define Uuid_VT_HIGH 4

#define Uuid_VT_LOW 6

#define ExternalDb_VT_PATH 4

#define ExternalDb_VT_SOURCE_CHECKPOINT_ID 6

#define ExternalDb_VT_FINAL_CHECKPOINT_ID 8

#define ExternalDb_VT_SST_IDS 10

#define BytesBound_VT_KEY 4

#define BytesBound_VT_BOUND_TYPE 6

#define BytesRange_VT_START_BOUND 4

#define BytesRange_VT_END_BOUND 6

#define ManifestV1_VT_MANIFEST_ID 4

#define ManifestV1_VT_EXTERNAL_DBS 6

#define ManifestV1_VT_INITIALIZED 8

#define ManifestV1_VT_WRITER_EPOCH 10

#define ManifestV1_VT_COMPACTOR_EPOCH 12

#define ManifestV1_VT_REPLAY_AFTER_WAL_ID 14

#define ManifestV1_VT_WAL_ID_LAST_SEEN 16

#define ManifestV1_VT_L0_LAST_COMPACTED 18

#define ManifestV1_VT_L0 20

#define ManifestV1_VT_COMPACTED 22

#define ManifestV1_VT_LAST_L0_CLOCK_TICK 24

#define ManifestV1_VT_CHECKPOINTS 26

#define ManifestV1_VT_LAST_L0_SEQ 28

#define ManifestV1_VT_WAL_OBJECT_STORE_URI 30

#define CompactedSsTable_VT_ID 4

#define CompactedSsTable_VT_INFO 6

#define CompactedSsTable_VT_VISIBLE_RANGE 8

#define SortedRun_VT_SSTS 6

#define WriterCheckpoint_VT_EPOCH 4

#define Checkpoint_VT_CHECKPOINT_EXPIRE_TIME_S 8

#define Checkpoint_VT_CHECKPOINT_CREATE_TIME_S 10

#define Checkpoint_VT_METADATA_TYPE 12

#define Checkpoint_VT_METADATA 14

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Open a SlateDB backed by an object-store configured via environment
 * variables (see `admin::load_object_store_from_env`).
 *
 * On success returns a non-null handle. On failure returns a null handle.
 */
SlateDbHandle slatedb_open(const char *path);

/**
 * Close the database and free all associated resources.
 */
void slatedb_close(SlateDbHandle handle);

/**
 * Insert or update a key/value pair. Returns 1 on success, 0 on error.
 */
unsigned int slatedb_put(SlateDbHandle handle,
                         const uint8_t *key_ptr,
                         unsigned int key_len,
                         const uint8_t *val_ptr,
                         unsigned int val_len);

/**
 * Delete a key. Returns 1 on success, 0 on error.
 */
unsigned int slatedb_delete(SlateDbHandle handle,
                            const uint8_t *key_ptr,
                            unsigned int key_len);

/**
 * Iterate over the entire key-space and invoke a user-supplied callback for
 * each record.
 * Returns 1 on success, 0 on error.
 */
unsigned int slatedb_iterate(SlateDbHandle handle, SlateDbIterCallback cb, void *userdata);

/* Returns last error message as a NUL-terminated C string, or NULL if none. */
const char *slatedb_last_error(void);

#ifdef __cplusplus
}
#endif
