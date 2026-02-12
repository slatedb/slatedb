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

SlateDB C uses opaque handles:
- `slatedb_object_store_t*`
- `slatedb_db_t*`
- `slatedb_db_reader_t*`
- `slatedb_iterator_t*`
- `slatedb_write_batch_t*`
- `slatedb_settings_t*`
- `slatedb_db_builder_t*`

Every public function returns `slatedb_result_t`.

### Error Handling

- Success: `result.kind == SLATEDB_ERROR_KIND_NONE`
- On error:
  - Inspect `result.kind`
  - For closed DB errors, inspect `result.close_reason`
  - Inspect `result.message` (UTF-8 C string)
- Always free result payloads with `slatedb_result_free(result)`

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

## Read-Only Reader Example (`DbReader`)

Use `slatedb_db_reader_open` for read-only access:

```c
struct slatedb_db_reader_t* reader = NULL;
struct slatedb_db_reader_options_t ropts = {
    .manifest_poll_interval_ms = 5000,
    .checkpoint_lifetime_ms = 60000,
    .max_memtable_bytes = 64 * 1024 * 1024,
    .skip_wal_replay = false,
};

check_result("reader_open", slatedb_db_reader_open(
    "demo-db",
    store,
    NULL,      // optional checkpoint UUID string
    &ropts,    // optional reader options
    &reader));

// reader_get / reader_scan / reader_scan_prefix
check_result("reader_close", slatedb_db_reader_close(reader));
```

## Builder + Settings

### Settings Sources

- Defaults: `slatedb_settings_default`
- File: `slatedb_settings_from_file`
- JSON string: `slatedb_settings_from_json`
- Env: `slatedb_settings_from_env`
- Env + default handle: `slatedb_settings_from_env_with_default`
- Auto-load (`SlateDb.{json,toml,yaml,yml}` + `SLATEDB_`): `slatedb_settings_load`

Apply JSON key/value patches:
- `slatedb_settings_apply_kv`

Serialize settings:
- `slatedb_settings_to_json`

Destroy settings handle:
- `slatedb_settings_close`

### Builder Flow

1. `slatedb_db_builder_new(path, object_store, &builder)`
2. Optional:
   - `slatedb_db_builder_with_settings(builder, settings)`
   - `slatedb_db_builder_with_seed(builder, seed)`
   - `slatedb_db_builder_with_sst_block_size(builder, SLATEDB_SST_BLOCK_SIZE_*)`
   - `slatedb_db_builder_with_wal_object_store(builder, wal_store)`
   - `slatedb_db_builder_with_merge_operator(...)`
3. `slatedb_db_builder_build(builder, &db)` (consumes builder)
4. `slatedb_db_builder_close(builder)` if not consumed

## Write Batches

- `slatedb_write_batch_new`
- mutate:
  - `slatedb_write_batch_put`
  - `slatedb_write_batch_put_with_options`
  - `slatedb_write_batch_merge`
  - `slatedb_write_batch_merge_with_options`
  - `slatedb_write_batch_delete`
- commit:
  - `slatedb_db_write`
  - `slatedb_db_write_with_options`
- release:
  - `slatedb_write_batch_close`

`slatedb_db_write*` consumes the inner batch.

## Logging

Process-global logging APIs:
- `slatedb_logging_init(level)`
- `slatedb_logging_set_level(level)`
- `slatedb_logging_set_callback(...)`
- `slatedb_logging_clear_callback()`

## API Reference

See `slatedb-c/include/slatedb.h` for complete type and function docs.
