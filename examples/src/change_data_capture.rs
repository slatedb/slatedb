use slatedb::config::{FlushOptions, FlushType};
use slatedb::object_store::{memory::InMemory, path::Path};
use slatedb::wal::{SlateDbWalReaderBuilder, WalReader as _, WalRows};
use slatedb::{Db, RowEntry, ValueDeletable};
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let object_store = Arc::new(InMemory::new());
    let path = "/change-data-capture-example";

    let db = Db::open(path, object_store.clone()).await?;
    db.put(b"user:1", b"alice").await?;
    db.put(b"user:2", b"bob").await?;
    db.delete(b"user:2").await?;
    flush_wal(&db).await?;

    let wal_reader = SlateDbWalReaderBuilder::new()
        .with_object_store(object_store)
        .with_path(Path::from(path))
        .build()?;
    let mut cursor = 0_u64;
    let start_wal_id = cursor
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("WAL cursor cannot advance"))?;
    let mut iterator = wal_reader.iterator((start_wal_id..).into()).await?;

    // Drain the writes that already exist. Empty fence WALs still advance the
    // cursor, so stop based on emitted rows only after persisting every batch.
    let mut emitted_rows = 0;
    while emitted_rows < 3 {
        let batch = iterator
            .next()
            .await?
            .ok_or_else(|| anyhow::anyhow!("live WAL iterator ended unexpectedly"))?;
        emitted_rows += emit_batch(&batch, &mut cursor);
    }

    // Keep the same iterator alive. Its next call observes this later WAL;
    // callers do not need to discover a new tail or create another iterator.
    db.put(b"user:3", b"carol").await?;
    flush_wal(&db).await?;
    while emitted_rows < 4 {
        let batch = iterator
            .next()
            .await?
            .ok_or_else(|| anyhow::anyhow!("live WAL iterator ended unexpectedly"))?;
        emitted_rows += emit_batch(&batch, &mut cursor);
    }

    db.close().await?;
    Ok(())
}

async fn flush_wal(db: &Db) -> Result<(), slatedb::Error> {
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::Wal,
    })
    .await
}

fn emit_batch(batch: &WalRows, cursor: &mut u64) -> usize {
    for row in &batch.rows {
        emit_row(batch.last_consumed_wal_file_id, row);
    }

    // Persist only after every row in this WAL file has been emitted. This
    // also advances across empty fence WALs.
    *cursor = batch.last_consumed_wal_file_id;
    println!("persist cursor={cursor}");
    batch.rows.len()
}

fn emit_row(wal_id: u64, row: &RowEntry) {
    let key = String::from_utf8_lossy(row.key.as_ref());
    match &row.value {
        ValueDeletable::Value(value) => {
            let value = String::from_utf8_lossy(value.as_ref());
            println!("wal_id={wal_id} seq={} upsert {key}={value}", row.seq);
        }
        ValueDeletable::Merge(value) => {
            let value = String::from_utf8_lossy(value.as_ref());
            println!("wal_id={wal_id} seq={} merge {key}+={value}", row.seq);
        }
        ValueDeletable::Tombstone => {
            println!("wal_id={wal_id} seq={} delete {key}", row.seq);
        }
    }
}
