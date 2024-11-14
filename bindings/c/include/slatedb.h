/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


#ifndef _SLATEDB_H
#define _SLATEDB_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

typedef enum slatedb_code {
  IO_ERROR,
  CHECKSUM_MISMATCH,
  EMPTY_SSTABLE,
  EMPTY_BLOCK_META,
  EMPTY_BLOCK,
  OBJECT_STORE_ERROR,
  MANIFEST_VERSION_EXISTS,
  MANIFEST_MISSING,
  INVALID_DELETION,
  INVALID_FLATBUFFER,
  INVALID_DB_STATE,
  INVALID_COMPACTION,
  FENCED,
  INVALID_CACHE_PART_SIZE,
  INVALID_COMPRESSION_CODEC,
  BLOCK_DECOMPRESSION_ERROR,
  BLOCK_COMPRESSION_ERROR,
  INVALID_ROW_FLAGS,
} slatedb_code;

typedef struct slatedb_instance {
  void *inner;
  void *rt;
} slatedb_instance;

typedef struct slatedb_error {
  enum slatedb_code code;
  const char *message;
} slatedb_error;

typedef struct slatedb_new_result {
  struct slatedb_instance *instance;
  struct slatedb_error *error;
} slatedb_new_result;

typedef enum slatedb_config_source_Tag {
  DEFAULT,
  FROM_FILE,
  FROM_ENV,
  LOAD,
} slatedb_config_source_Tag;

typedef struct slatedb_config_source {
  slatedb_config_source_Tag tag;
  union {
    struct {
      const char *from_file;
    };
    struct {
      const char *from_env;
    };
  };
} slatedb_config_source;

typedef enum slatedb_s3_conditional_put_Tag {
  NONE,
  ETAG_MATCH,
  ETAG_PUT_IF_NOT_EXISTS,
  DYNAMO,
} slatedb_s3_conditional_put_Tag;

typedef struct slatedb_s3_conditional_put {
  slatedb_s3_conditional_put_Tag tag;
  union {
    struct {
      const char *dynamo;
    };
  };
} slatedb_s3_conditional_put;

typedef struct slatedb_aws_config {
  const char *bucket_name;
  struct slatedb_s3_conditional_put conditional_put;
} slatedb_aws_config;

typedef enum slatedb_object_store_Tag {
  AWS,
  IN_MEMORY,
  LOCAL_FILESYSTEM,
} slatedb_object_store_Tag;

typedef struct slatedb_object_store {
  slatedb_object_store_Tag tag;
  union {
    struct {
      struct slatedb_aws_config aws;
    };
  };
} slatedb_object_store;

typedef struct slatedb_instance_get_result {
  const char *value;
  struct slatedb_error *error;
} slatedb_instance_get_result;

#define CompactedSstId_VT_HIGH 4

#define CompactedSstId_VT_LOW 6

#define CompactedSsTable_VT_ID 4

#define CompactedSsTable_VT_INFO 6

#define SsTableInfo_VT_FIRST_KEY 4

#define SsTableInfo_VT_INDEX_OFFSET 6

#define SsTableInfo_VT_INDEX_LEN 8

#define SsTableInfo_VT_FILTER_OFFSET 10

#define SsTableInfo_VT_FILTER_LEN 12

#define SsTableInfo_VT_COMPRESSION_FORMAT 14

#define BlockMeta_VT_OFFSET 4

#define SsTableIndex_VT_BLOCK_META 4

#define ManifestV1_VT_MANIFEST_ID 4

#define ManifestV1_VT_INITIALIZED 6

#define ManifestV1_VT_WRITER_EPOCH 8

#define ManifestV1_VT_COMPACTOR_EPOCH 10

#define ManifestV1_VT_WAL_ID_LAST_COMPACTED 12

#define ManifestV1_VT_WAL_ID_LAST_SEEN 14

#define ManifestV1_VT_L0_LAST_COMPACTED 16

#define ManifestV1_VT_L0 18

#define ManifestV1_VT_COMPACTED 20

#define ManifestV1_VT_LAST_CLOCK_TICK 22

#define ManifestV1_VT_CHECKPOINTS 24

#define SortedRun_VT_SSTS 6

#define WriterCheckpoint_VT_EPOCH 4

#define Checkpoint_VT_CHECKPOINT_EXPIRE_TIME_S 8

#define Checkpoint_VT_CHECKPOINT_CREATE_TIME_S 10

#define Checkpoint_VT_METADATA_TYPE 12

#define Checkpoint_VT_METADATA 14

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

void slatedb_instance_free(const struct slatedb_instance *ptr);

/**
 * Creates a new instance of slatedb based on the config provided
 */
struct slatedb_new_result slatedb_instance_new(const char *path,
                                               struct slatedb_config_source config,
                                               struct slatedb_object_store object_store);

/**
 * Corresponds to slatedb::Db::get
 */
struct slatedb_instance_get_result slatedb_instance_get(const struct slatedb_instance *instance,
                                                        const char *key);

/**
 * Corresponds to slatedb::Db::put
 */
struct slatedb_error *slatedb_instance_put(const struct slatedb_instance *instance,
                                           const char *key,
                                           const char *value);

/**
 * Corresponds to slatedb::Db::delete
 */
struct slatedb_error *slatedb_instance_delete(const struct slatedb_instance *instance,
                                              const char *key);

/**
 * Corresponds to slatedb::Db::flush
 */
struct slatedb_error *slatedb_instance_flush(const struct slatedb_instance *instance);

/**
 * Corresponds to slatedb::Db::close
 */
struct slatedb_error *slatedb_instance_close(const struct slatedb_instance *instance);

/**
 * Frees the slatedb_error, ok to call on NULL
 */
void slatedb_error_free(struct slatedb_error *ptr);

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus

#endif /* _SLATEDB_H */
