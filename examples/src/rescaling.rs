//! Rescaling a database by splitting and merging key ranges.
//!
//! Scale-up (split) projects one source into two clones with disjoint key
//! ranges. Scale-down (merge) unions those clones back into one database.
//! Both are O(1) manifest views over shared SSTs — no SST data is copied.
//!
//! Projection and union reject sources that still have data in the WAL, so
//! this example flushes the WAL and memtables into L0, then checkpoints with
//! [`CheckpointScope::Durable`] before cloning. The same APIs work for
//! segmented and non-segmented stores.
//!
//! Union requires its sources to be non-overlapping. For a segmented store
//! that rule applies per segment, so the two shards below merge in one call
//! even though each holds part of both segments.
//!
//! Clone construction uses:
//! [`AdminBuilder::new`] → [`Admin::create_clone_builder_from_source`] →
//! [`CloneBuilder::with_source`] → [`CloneBuilder::build`].

use slatedb::admin::{AdminBuilder, CloneSourceSpec};
use slatedb::bytes::Bytes;
use slatedb::config::{CheckpointOptions, CheckpointScope, FlushOptions, FlushType};
use slatedb::object_store::memory::InMemory;
use slatedb::{CheckpointCreateResult, Db, Error, PrefixExtractor, PrefixTarget};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

type ProjectionRange = (Bound<Bytes>, Bound<Bytes>);

/// Tenant IDs sort as bytes, so zoos `< "metro"` land on the left shard.
const SPLIT_TENANT: &[u8] = b"metro";

fn left_tenants() -> ProjectionRange {
    (
        Bound::Unbounded,
        Bound::Excluded(Bytes::from_static(SPLIT_TENANT)),
    )
}

fn right_tenants() -> ProjectionRange {
    (
        Bound::Included(Bytes::from_static(SPLIT_TENANT)),
        Bound::Unbounded,
    )
}

/// Two LSM segments — bulky animal records vs a smaller owner index.
///
/// Keys are kind-first (`data/…`, `idx/…`) so the extractor names the segments
/// `data` and `idx`. Tenants live in the next path component, so each zoo's
/// rows are not one contiguous byte range; tenant splits use
/// [`CloneBuilder::with_segment_projection`].
struct DataIdxSegmentExtractor;

impl PrefixExtractor for DataIdxSegmentExtractor {
    fn name(&self) -> &str {
        "data_idx"
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        let key = match target {
            PrefixTarget::Point(key) | PrefixTarget::Prefix(key) => key.as_ref(),
        };
        if key == b"data" || key.starts_with(b"data/") {
            Some(b"data".len())
        } else if key == b"idx" || key.starts_with(b"idx/") {
            Some(b"idx".len())
        } else {
            None
        }
    }
}

fn animal_key(zoo: &[u8], animal_id: &[u8]) -> Vec<u8> {
    [b"data/", zoo, b"/animal/", animal_id].concat()
}

fn owner_index_key(zoo: &[u8], owner: &[u8], animal_id: &[u8]) -> Vec<u8> {
    [b"idx/", zoo, b"/owner/", owner, b"/", animal_id].concat()
}

fn kind_tenant(kind: &[u8], tenant: &[u8]) -> Bytes {
    Bytes::from([kind, b"/", tenant].concat())
}

/// Per-segment view of zoos `< metro` (valid inside `[prefix, prefix++)`).
fn left_tenant_in_segment(prefix: &[u8]) -> ProjectionRange {
    (
        Bound::Unbounded,
        Bound::Excluded(kind_tenant(prefix, SPLIT_TENANT)),
    )
}

/// Per-segment view of zoos `>= metro`.
fn right_tenant_in_segment(prefix: &[u8]) -> ProjectionRange {
    (
        Bound::Included(kind_tenant(prefix, SPLIT_TENANT)),
        Bound::Unbounded,
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let object_store = Arc::new(InMemory::new());

    println!("=== Non-segmented rescaling ===");
    rescale_non_segmented(object_store.clone()).await?;

    println!("\n=== Segmented rescaling (data + idx segments, split by zoo) ===");
    rescale_segmented(object_store).await?;

    Ok(())
}

async fn rescale_non_segmented(object_store: Arc<InMemory>) -> anyhow::Result<()> {
    let root_path = "/tmp/slatedb_rescaling/plain/root";
    let left_path = "/tmp/slatedb_rescaling/plain/left";
    let right_path = "/tmp/slatedb_rescaling/plain/right";
    let merged_path = "/tmp/slatedb_rescaling/plain/merged";

    // Tenant-prefixed keys without a segment extractor — still split by zoo.
    let db = Db::open(root_path, object_store.clone()).await?;
    db.put(b"bronx/lion", b"Leo").await?;
    db.put(b"lincoln/otter", b"Ollie").await?;
    db.put(b"metro/panda", b"Mei").await?;
    db.put(b"oakland/zebra", b"Ziggy").await?;
    let checkpoint = checkpoint_for_rescale(&db).await?;
    db.close().await?;

    create_clone(
        left_path,
        vec![CloneSourceSpec::with_checkpoint(root_path, checkpoint.id)
            .with_projection_range(left_tenants())],
        object_store.clone(),
    )
    .await?;
    create_clone(
        right_path,
        vec![CloneSourceSpec::with_checkpoint(root_path, checkpoint.id)
            .with_projection_range(right_tenants())],
        object_store.clone(),
    )
    .await?;

    let left = Db::open(left_path, object_store.clone()).await?;
    let right = Db::open(right_path, object_store.clone()).await?;
    assert_eq!(
        left.get(b"bronx/lion").await?,
        Some(b"Leo".as_slice().into())
    );
    assert_eq!(left.get(b"metro/panda").await?, None);
    assert_eq!(
        right.get(b"metro/panda").await?,
        Some(b"Mei".as_slice().into())
    );
    assert_eq!(right.get(b"bronx/lion").await?, None);
    println!("split by tenant: left has bronx/lincoln; right has metro/oakland");
    left.close().await?;
    right.close().await?;

    create_clone(
        merged_path,
        vec![
            CloneSourceSpec::new(left_path).with_projection_range(left_tenants()),
            CloneSourceSpec::new(right_path).with_projection_range(right_tenants()),
        ],
        object_store.clone(),
    )
    .await?;

    let merged = Db::open(merged_path, object_store).await?;
    assert_eq!(
        merged.get(b"bronx/lion").await?,
        Some(b"Leo".as_slice().into())
    );
    assert_eq!(
        merged.get(b"oakland/zebra").await?,
        Some(b"Ziggy".as_slice().into())
    );
    println!("merged: all zoos are visible again");
    merged.close().await?;

    Ok(())
}

async fn rescale_segmented(object_store: Arc<InMemory>) -> anyhow::Result<()> {
    let extractor = Arc::new(DataIdxSegmentExtractor);
    let root_path = "/tmp/slatedb_rescaling/segmented/root";
    let left_path = "/tmp/slatedb_rescaling/segmented/left";
    let right_path = "/tmp/slatedb_rescaling/segmented/right";
    let merged_path = "/tmp/slatedb_rescaling/segmented/merged";

    let db = Db::builder(root_path, object_store.clone())
        .with_segment_extractor(extractor.clone())
        .build()
        .await?;

    // bronx + lincoln → left of the split; metro + oakland → right.
    put_animal(&db, b"bronx", b"lion-1", b"alice", b"Leo the lion").await?;
    put_animal(&db, b"lincoln", b"otter-1", b"bob", b"Ollie the otter").await?;
    put_animal(&db, b"metro", b"panda-1", b"carol", b"Mei the panda").await?;
    put_animal(&db, b"oakland", b"zebra-1", b"dave", b"Ziggy the zebra").await?;

    let checkpoint = checkpoint_for_rescale(&db).await?;
    db.close().await?;

    // Scale up: keep each zoo's data + owner-index together via per-segment
    // projection (`data/{zoo}/…` and `idx/{zoo}/…` are not one byte range).
    create_clone_with_segment_projection(
        left_path,
        CloneSourceSpec::with_checkpoint(root_path, checkpoint.id),
        object_store.clone(),
        left_tenant_in_segment,
    )
    .await?;
    create_clone_with_segment_projection(
        right_path,
        CloneSourceSpec::with_checkpoint(root_path, checkpoint.id),
        object_store.clone(),
        right_tenant_in_segment,
    )
    .await?;

    let left = Db::builder(left_path, object_store.clone())
        .with_segment_extractor(extractor.clone())
        .build()
        .await?;
    let right = Db::builder(right_path, object_store.clone())
        .with_segment_extractor(extractor.clone())
        .build()
        .await?;

    assert_eq!(
        left.get(animal_key(b"bronx", b"lion-1")).await?,
        Some(b"Leo the lion".as_slice().into())
    );
    assert_eq!(
        left.get(owner_index_key(b"bronx", b"alice", b"lion-1"))
            .await?,
        Some(Bytes::new())
    );
    assert_eq!(left.get(animal_key(b"metro", b"panda-1")).await?, None);
    assert_eq!(
        left.get(owner_index_key(b"metro", b"carol", b"panda-1"))
            .await?,
        None
    );
    assert_eq!(
        right.get(animal_key(b"metro", b"panda-1")).await?,
        Some(b"Mei the panda".as_slice().into())
    );
    assert_eq!(
        right
            .get(owner_index_key(b"metro", b"carol", b"panda-1"))
            .await?,
        Some(Bytes::new())
    );
    assert_eq!(right.get(animal_key(b"bronx", b"lion-1")).await?, None);
    println!("split by tenant: left has bronx/lincoln (data+idx); right has metro/oakland");
    left.close().await?;
    right.close().await?;

    // Scale down: one union merges both shards. Union requires the sources to
    // be non-overlapping per segment, not overall — each shard holds the lower
    // or upper zoos of both `data` and `idx`, so no segment is claimed twice.
    create_clone(
        merged_path,
        vec![
            CloneSourceSpec::new(left_path),
            CloneSourceSpec::new(right_path),
        ],
        object_store.clone(),
    )
    .await?;

    let merged = Db::builder(merged_path, object_store)
        .with_segment_extractor(extractor)
        .build()
        .await?;
    assert_eq!(
        merged.get(animal_key(b"bronx", b"lion-1")).await?,
        Some(b"Leo the lion".as_slice().into())
    );
    assert_eq!(
        merged
            .get(owner_index_key(b"oakland", b"dave", b"zebra-1"))
            .await?,
        Some(Bytes::new())
    );
    println!("merged: all zoo data and owner-index rows are visible again");
    merged.close().await?;

    Ok(())
}

async fn put_animal(
    db: &Db,
    zoo: &[u8],
    animal_id: &[u8],
    owner: &[u8],
    record: &[u8],
) -> Result<(), Error> {
    db.put(animal_key(zoo, animal_id), record).await?;
    db.put(owner_index_key(zoo, owner, animal_id), b"").await?;
    Ok(())
}

/// Build a clone from one or more sources.
async fn create_clone(
    clone_path: &str,
    sources: Vec<CloneSourceSpec<ProjectionRange>>,
    object_store: Arc<InMemory>,
) -> Result<(), Error> {
    let admin = AdminBuilder::new(clone_path, object_store).build();
    let mut sources = sources.into_iter();
    let first = sources
        .next()
        .expect("rescaling clone requires at least one source");
    let mut builder = admin.create_clone_builder_from_source(first);
    for source in sources {
        builder = builder.with_source(source);
    }
    builder.build().await
}

/// Build a single-source clone with a per-segment projection.
async fn create_clone_with_segment_projection<F, R>(
    clone_path: &str,
    source: CloneSourceSpec<ProjectionRange>,
    object_store: Arc<InMemory>,
    segment_projection: F,
) -> Result<(), Error>
where
    F: Fn(&[u8]) -> R + Send + Sync + 'static,
    R: RangeBounds<Bytes>,
{
    AdminBuilder::new(clone_path, object_store)
        .build()
        .create_clone_builder_from_source(source)
        .with_segment_projection(segment_projection)
        .build()
        .await
}

/// Flush every write into L0, then pin that state. Projection and union reject
/// sources that still have non-empty WAL SSTs.
async fn checkpoint_for_rescale(db: &Db) -> anyhow::Result<CheckpointCreateResult> {
    db.flush().await?;
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await?;
    Ok(db
        .create_checkpoint(CheckpointScope::Durable, &CheckpointOptions::default())
        .await?)
}
