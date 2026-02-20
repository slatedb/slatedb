use slatedb::config::{FlushOptions, FlushType};
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, RowEntry, ValueDeletable, WalFile, WalReader};
use std::sync::Arc;

#[derive(Debug, Default)]
struct CdcCursor {
    wal_id: u64,
    last_seq: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let object_store = Arc::new(InMemory::new());
    let path = "/change-data-capture-example";

    let db = Db::open(path, object_store.clone()).await?;
    db.put(b"user:1", b"alice").await?;
    db.put(b"user:2", b"bob").await?;
    db.delete(b"user:2").await?;
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::Wal,
    })
    .await?;

    let wal_reader = WalReader::new(path, object_store);
    let mut cursor = CdcCursor::default();

    // Use list() once for discovery (or after a long outage).
    for wal_file in wal_reader.list(cursor.wal_id..).await? {
        emit_wal_file(&wal_file, &mut cursor).await?;
    }

    // Poll by ID to avoid repeated full prefix listings.
    let next_file = wal_reader.get(cursor.wal_id + 1);
    emit_wal_file(&next_file, &mut cursor).await?;
    println!("Persist cursor periodically: {:?}", cursor);

    db.close().await?;
    Ok(())
}

async fn emit_wal_file(wal_file: &WalFile, cursor: &mut CdcCursor) -> anyhow::Result<()> {
    let mut iter = wal_file.iterator().await?;
    while let Some(row) = iter.next_entry().await? {
        if wal_file.id == cursor.wal_id && row.seq <= cursor.last_seq {
            continue;
        }

        emit_row(wal_file.id, &row);
        cursor.wal_id = wal_file.id;
        cursor.last_seq = row.seq;
    }

    Ok(())
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
