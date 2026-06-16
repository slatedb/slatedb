//! Integration tests for `slatedb scan`.

use std::process::Command;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use slatedb::Db;

async fn populate(root: &std::path::Path) {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(root).unwrap());
    let db = Db::builder("db", store).build().await.unwrap();
    db.put(b"user/1", b"alice").await.unwrap();
    db.put(b"user/2", b"bob").await.unwrap();
    db.put(b"user/3", b"carol").await.unwrap();
    db.put(b"config", b"v1").await.unwrap();
    db.put([0x00u8, 0x42], [0u8, 1, 2, 3]).await.unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();
}

fn run(root: &std::path::Path, extra: &[&str]) -> std::process::Output {
    let mut args = vec!["--path", "db", "scan"];
    args.extend_from_slice(extra);
    Command::new(env!("CARGO_BIN_EXE_slatedb"))
        .args(args)
        // The CLI reads object-store config from the environment, so set up a filesystem
        // store at `root`.
        .env("CLOUD_PROVIDER", "local")
        .env("LOCAL_PATH", root)
        .env("RUST_LOG", "error")
        .output()
        .unwrap()
}

fn stdout(out: &std::process::Output) -> String {
    assert!(out.status.success(), "slatedb scan failed: {out:?}");
    String::from_utf8(out.stdout.clone()).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn full_scan_renders_text_and_binary() {
    let tmp = tempfile::tempdir().unwrap();
    populate(tmp.path()).await;

    let out = stdout(&run(tmp.path(), &[]));
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 5);
    // Keys sort by raw bytes, so the 0x00-prefixed binary key comes first and
    // renders as 0x hex with the binary-value marker.
    assert_eq!(lines[0], "0x0042\t<binary 4 bytes>");
    // Text keys and values render bare.
    assert!(out.contains("config\tv1"));
    assert!(out.contains("user/1\talice"));
}

#[tokio::test(flavor = "multi_thread")]
async fn prefix_filters_keys() {
    let tmp = tempfile::tempdir().unwrap();
    populate(tmp.path()).await;

    let out = stdout(&run(tmp.path(), &["--prefix", "user"]));
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 3);
    assert!(lines.iter().all(|l| l.starts_with("user/")));
}

#[tokio::test(flavor = "multi_thread")]
async fn from_to_bounds_filter_range() {
    let tmp = tempfile::tempdir().unwrap();
    populate(tmp.path()).await;

    // [config, user/2) includes config and user/1 (inclusive lower) but stops
    // before user/2 (exclusive upper); the 0x00 binary key sorts below config.
    let out = stdout(&run(
        tmp.path(),
        &["--from", "config", "--to", "user/2", "--value", "none"],
    ));
    assert_eq!(out.lines().collect::<Vec<_>>(), vec!["config", "user/1"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn value_hex_dumps_raw_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    populate(tmp.path()).await;

    // A 0x-prefixed prefix is hex-decoded in auto mode, selecting the binary key.
    let out = stdout(&run(tmp.path(), &["--prefix", "0x00", "--value", "hex"]));
    assert_eq!(out.trim_end(), "0x0042\t0x00010203");
}

#[tokio::test(flavor = "multi_thread")]
async fn value_none_prints_keys_only() {
    let tmp = tempfile::tempdir().unwrap();
    populate(tmp.path()).await;

    let out = stdout(&run(tmp.path(), &["--prefix", "user", "--value", "none"]));
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 3);
    assert!(lines.iter().all(|l| !l.contains('\t')));
    assert_eq!(lines[0], "user/1");
}

#[tokio::test(flavor = "multi_thread")]
async fn count_reports_totals() {
    let tmp = tempfile::tempdir().unwrap();
    populate(tmp.path()).await;

    assert!(stdout(&run(tmp.path(), &["--count"])).starts_with("5 entries"));
    assert!(stdout(&run(tmp.path(), &["--prefix", "user", "--count"])).starts_with("3 entries"));
}

#[tokio::test(flavor = "multi_thread")]
async fn max_keys_caps_output() {
    let tmp = tempfile::tempdir().unwrap();
    populate(tmp.path()).await;

    let out = stdout(&run(tmp.path(), &["--value", "none", "--max-keys", "2"]));
    assert_eq!(out.lines().count(), 2);
}
