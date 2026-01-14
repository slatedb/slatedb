# SlateDB Common

Common utilities and types shared across SlateDB.

## Usage

Add to `Cargo.toml`:

```toml
[dependencies]
slatedb-common = "*"
tokio = { version = "*", features = ["macros", "rt", "time"] }
```

### Examples

Clock with a timeout:

```rust
use slatedb_common::clock::DefaultSystemClock;
use slatedb_common::timeout;
use std::sync::Arc;
use std::time::Duration;

async fn do_work() -> Result<u64, &'static str> {
    Ok(42)
}

async fn run() -> Result<u64, &'static str> {
    let clock = Arc::new(DefaultSystemClock::new());
    timeout(
        clock,
        Duration::from_secs(1),
        || "timed out",
        do_work(),
    )
    .await
}
```
