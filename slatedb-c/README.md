# SlateDB C Bindings (`slatedb-c`)

`slatedb-c` exposes a C ABI for [SlateDB](https://slatedb.io).

The crate builds:
- A C header: `slatedb-c/include/slatedb.h`
- Libraries:
  - `target/{debug,release}/libslatedb_c.a`
  - `target/{debug,release}/libslatedb_c.{dylib,so}`

## Build

From the repository root:

```bash
cargo build -p slatedb-c
# or
cargo build --release -p slatedb-c
```

## Integrate In C/C++

1. Add include path: `slatedb-c/include`
2. Link `libslatedb_c`
3. Ensure runtime loader can find the shared library (for `.so`/`.dylib`)

Example compile/link:

```bash
cc -I slatedb-c/include \
  example.c \
  -L target/release \
  -lslatedb_c \
  -o example
```

## API Model

SlateDB C uses opaque handles so callers never depend on Rust internals.

- `slatedb_object_store_t*` represents a resolved object-store backend (for example `memory:///` or `file:///...`) and is reused when opening databases.
- `slatedb_db_t*` represents an open read/write database connection.
- `slatedb_db_reader_t*` represents a read-only database reader with its own refresh/replay behavior.
- `slatedb_iterator_t*` represents an active scan cursor returned by scan APIs.
- `slatedb_write_batch_t*` represents a mutable batch that stages multiple writes before commit.
- `slatedb_settings_t*` represents parsed configuration values loaded from defaults, files, JSON, or environment variables.
- `slatedb_db_builder_t*` represents an in-progress open configuration that is consumed by `slatedb_db_builder_build`.

Each handle type has a matching `*_close` API and should be closed exactly once when no longer needed.

Every public function returns `slatedb_result_t`.

### Error Handling

Success is reported with `result.kind == SLATEDB_ERROR_KIND_NONE`. On failures, inspect
`result.kind` first, then inspect `result.close_reason` when `result.kind` is
`SLATEDB_ERROR_KIND_CLOSED` to distinguish clean close/fenced/panic cases. The optional
`result.message` is a UTF-8 string owned by SlateDB and must always be released via
`slatedb_result_free`, even on success.

```c
static bool check_result(const char* op, struct slatedb_result_t result) {
    if (result.kind == SLATEDB_ERROR_KIND_NONE) {
        slatedb_result_free(result);
        return true;
    }

    fprintf(stderr, "%s failed: kind=%d", op, (int)result.kind);
    if (result.kind == SLATEDB_ERROR_KIND_CLOSED) {
        fprintf(stderr, " close_reason=%d", (int)result.close_reason);
    }
    fprintf(stderr, " message=%s\n", result.message ? result.message : "(none)");

    slatedb_result_free(result);
    return false;
}

if (!check_result("db_flush", slatedb_db_flush(db))) {
    return 1;
}
```

### Memory Ownership

Buffers returned by SlateDB (for example values, iterator keys/values, JSON blobs) are Rust-allocated.
Free them with:

```c
slatedb_bytes_free(ptr, len);
```

Opaque handles must be released with their corresponding `*_close` function.

## End-To-End Example (Open DB, Put/Get, Scan)

```c
#include "slatedb.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int check_result(const char* op, struct slatedb_result_t r) {
    if (r.kind != SLATEDB_ERROR_KIND_NONE) {
        fprintf(stderr, "%s failed (kind=%d close_reason=%d): %s\n",
                op, (int)r.kind, (int)r.close_reason,
                r.message ? r.message : "(no message)");
        slatedb_result_free(r);
        return 0;
    }
    slatedb_result_free(r);
    return 1;
}

int main(void) {
    struct slatedb_object_store_t* store = NULL;
    struct slatedb_db_t* db = NULL;
    struct slatedb_iterator_t* it = NULL;

    if (!check_result("resolve_object_store",
            slatedb_db_resolve_object_store("memory:///", &store))) {
        return 1;
    }

    if (!check_result("db_open",
            slatedb_db_open("demo-db", store, &db))) {
        slatedb_object_store_close(store);
        return 1;
    }

    if (!check_result("db_put",
            slatedb_db_put(db, (const uint8_t*)"k1", 2, (const uint8_t*)"v1", 2))) {
        goto cleanup;
    }

    if (!check_result("db_flush", slatedb_db_flush(db))) {
        goto cleanup;
    }

    bool present = false;
    uint8_t* value = NULL;
    uintptr_t value_len = 0;
    if (!check_result("db_get",
            slatedb_db_get(db, (const uint8_t*)"k1", 2, &present, &value, &value_len))) {
        goto cleanup;
    }
    if (present) {
        printf("k1 => %.*s\n", (int)value_len, (const char*)value);
        slatedb_bytes_free(value, value_len);
    }

    struct slatedb_range_t range = {
        .start = { .kind = SLATEDB_BOUND_KIND_INCLUDED, .data = (const uint8_t*)"k", .len = 1 },
        .end = { .kind = SLATEDB_BOUND_KIND_UNBOUNDED, .data = NULL, .len = 0 }
    };

    if (!check_result("db_scan", slatedb_db_scan(db, range, &it))) {
        goto cleanup;
    }

    while (1) {
        bool row_present = false;
        uint8_t* key = NULL;
        uintptr_t key_len = 0;
        uint8_t* val = NULL;
        uintptr_t val_len = 0;

        if (!check_result("iterator_next",
                slatedb_iterator_next(it, &row_present, &key, &key_len, &val, &val_len))) {
            break;
        }
        if (!row_present) {
            break;
        }

        printf("scan: %.*s => %.*s\n", (int)key_len, (const char*)key, (int)val_len, (const char*)val);
        slatedb_bytes_free(key, key_len);
        slatedb_bytes_free(val, val_len);
    }

cleanup:
    if (it) {
        check_result("iterator_close", slatedb_iterator_close(it));
    }
    if (db) {
        check_result("db_close", slatedb_db_close(db));
    }
    if (store) {
        check_result("object_store_close", slatedb_object_store_close(store));
    }
    return 0;
}
```

## Settings

Settings in `slatedb-c` are implemented as an opaque handle (`slatedb_settings_t*`) that wraps
the Rust settings model. You create a settings handle from one of the supported sources, optionally
apply targeted key/value patches, and then pass that handle to
`slatedb_db_builder_with_settings`. The handle owns the resolved configuration snapshot until it is
released with `slatedb_settings_close`. Settings can be loaded from config files (`.json`, `.toml`,
`.yaml`, `.yml`), JSON payloads, environment variables (with `SLATEDB_` prefix), or via
`slatedb_settings_default`. See the Rust [`config.rs`](../slatedb/src/config.rs) file for the full
list of supported configuration keys and their expected types.

## Builder

The builder API lets you stage database-open options before constructing a `slatedb_db_t*`.
Create a builder with path + object store, apply optional configuration methods, and then call
`slatedb_db_builder_build`. Build consumes the builder handle on success, so only call
`slatedb_db_builder_close` when build fails or when you intentionally abandon the builder.

```c
struct slatedb_db_builder_t* builder = NULL;
struct slatedb_db_t* db = NULL;
struct slatedb_settings_t* settings = NULL;

check_result("settings_default", slatedb_settings_default(&settings));
struct slatedb_settings_kv_t patch = {
    .key = "compactor_options.max_sst_size",
    .value_json = "268435456",
};
check_result("settings_apply_kv", slatedb_settings_apply_kv(settings, &patch, 1));
check_result("builder_new", slatedb_db_builder_new("demo-db", store, &builder));
check_result("builder_with_settings",
             slatedb_db_builder_with_settings(builder, settings));
check_result("builder_with_sst_block_size",
             slatedb_db_builder_with_sst_block_size(builder, SLATEDB_SST_BLOCK_SIZE_16KIB));

if (check_result("builder_build", slatedb_db_builder_build(builder, &db))) {
    builder = NULL; // consumed by build on success
}

if (builder) {
    check_result("builder_close", slatedb_db_builder_close(builder));
}
check_result("settings_close", slatedb_settings_close(settings));
```

## Operations

### Logging

SlateDB logging is process-global in `slatedb-c`. Initialize it once with `slatedb_logging_init`,
change verbosity with `slatedb_logging_set_level`, and optionally register a callback to route log
events into your own logging system. Call `slatedb_logging_clear_callback` when the callback is no
longer needed.

```c
static void on_log(slatedb_log_level_t level,
                   const char* target, uintptr_t target_len,
                   const char* message, uintptr_t message_len,
                   const char* module_path, uintptr_t module_path_len,
                   const char* file, uintptr_t file_len,
                   uint32_t line, void* context) {
    (void)module_path;
    (void)module_path_len;
    (void)file;
    (void)file_len;
    (void)line;
    (void)context;
    fprintf(stderr, "[%u] %.*s: %.*s\n",
            (unsigned)level, (int)target_len, target, (int)message_len, message);
}

check_result("logging_init", slatedb_logging_init(SLATEDB_LOG_LEVEL_INFO));
check_result("logging_set_level", slatedb_logging_set_level(SLATEDB_LOG_LEVEL_DEBUG));
check_result("logging_set_callback", slatedb_logging_set_callback(on_log, NULL, NULL));

/* ... run DB operations ... */

check_result("logging_clear_callback", slatedb_logging_clear_callback());
```

### Metrics

Metrics are collected per database handle and can be read at runtime. Use
`slatedb_db_metric_get` when you want one metric by name, or `slatedb_db_metrics` for a full JSON
snapshot that you can ship to your telemetry pipeline. No additional initialization is required
beyond opening the database.

```c
bool present = false;
int64_t value = 0;
check_result("db_metric_get",
             slatedb_db_metric_get(db, "db/get_requests", &present, &value));
if (present) {
    printf("db/get_requests=%lld\n", (long long)value);
}

uint8_t* json = NULL;
uintptr_t json_len = 0;
check_result("db_metrics", slatedb_db_metrics(db, &json, &json_len));
printf("metrics snapshot: %.*s\n", (int)json_len, (const char*)json);
slatedb_bytes_free(json, json_len);
```

## API Reference

See [`include/slatedb.h`](include/slatedb.h) for complete type and function docs.
