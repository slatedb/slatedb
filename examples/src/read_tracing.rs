use slatedb::config::{ReadOptions, TracingOptions};
use slatedb::{bytes::Bytes, object_store::memory::InMemory, Db};
use std::sync::Arc;
use tracing_chrome::{ChromeLayerBuilder, TraceStyle};
use tracing_subscriber::layer::SubscriberExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Setup
    let object_store = Arc::new(InMemory::new());
    let db = Db::open("/tmp/slatedb_tracing_subscriber", object_store).await?;

    // Put
    let key = b"test_key";
    let value = b"test_value";
    db.put(key, value).await?;

    // Create tracing options with a custom trace ID
    let tracing_options = TracingOptions::new("my-trace-id");

    // Pass it to the read operation
    let read_options = ReadOptions::default().with_tracing_options(Some(tracing_options));

    // Register tracing-chrome subscriber.
    // Subscriber tracing-chrome outputs traces in Chrome’s trace viewer format.
    // The generated file `trace-<unix timestamp>` with the traces can be found in
    // the root directory of the project. Dump that file into
    // https://ui.perfetto.dev/ to visualize the spans.
    let (chrome_layer, _chrome_guard) = ChromeLayerBuilder::new()
        .include_args(true)
        .trace_style(TraceStyle::Async)
        .build();
    let subscriber = tracing_subscriber::registry().with(chrome_layer);
    let _subscriber_guard = tracing::subscriber::set_default(subscriber);

    //Get
    let result = db.get_with_options(key, &read_options).await?;
    assert_eq!(result, Some(Bytes::from_static(value)));

    // Close
    db.close().await?;

    Ok(())
}
