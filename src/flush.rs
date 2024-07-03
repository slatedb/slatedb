use crate::db::{DbInner, DbState};
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::mem_table::{ImmutableWal, KVTable, WritableKVTable};
use crate::tablestore::{self, SSTableHandle};
use crate::types::ValueDeletable;
use futures::executor::block_on;
use parking_lot::RwLockWriteGuard;
use std::sync::Arc;
use std::time::Duration;

impl DbInner {
    pub(crate) async fn flush(&self) -> Result<(), SlateDBError> {
        self.freeze_wal(&mut self.state.write());
        self.flush_imm_wals().await?;
        Ok(())
    }

    fn freeze_wal(&self, guard: &mut RwLockWriteGuard<DbState>) -> Option<u64> {
        if guard.wal.table().is_empty() {
            return None;
        }
        let old_wal = std::mem::replace(&mut guard.wal, WritableKVTable::new());
        let mut compacted_snapshot = guard.compacted.as_ref().clone();
        let imm_wal = Arc::new(ImmutableWal::new(
            compacted_snapshot.next_wal_sst_id,
            old_wal,
        ));
        let id = imm_wal.id();
        compacted_snapshot.imm_wal.push_front(imm_wal);
        compacted_snapshot.next_wal_sst_id += 1;
        guard.compacted = Arc::new(compacted_snapshot);
        Some(id)
    }

    pub(crate) async fn write_manifest(&self) -> Result<(), SlateDBError> {
        // get the update manifest if there are any updates.
        let updated_manifest: Option<crate::flatbuffer_types::ManifestV1Owned> = {
            let db_state = self.state.read();
            let mut wguard_manifest = self.manifest.write();
            if wguard_manifest.borrow().wal_id_last_seen() != db_state.compacted.next_wal_sst_id - 1
            {
                let new_manifest = wguard_manifest.get_updated_manifest(&db_state.compacted);
                *wguard_manifest = new_manifest.clone();
                Some(new_manifest)
            } else {
                None
            }
        };

        if let Some(manifest) = updated_manifest {
            self.table_store.write_manifest(&manifest).await?
        }

        Ok(())
    }

    // todo: move me
    pub(crate) async fn flush_imm_table(
        &self,
        id: &tablestore::SsTableId,
        imm_table: Arc<KVTable>,
    ) -> Result<SSTableHandle, SlateDBError> {
        let mut sst_builder = self.table_store.table_builder();
        let mut iter = imm_table.iter();
        while let Some(kv) = iter.next_entry().await? {
            match kv.value {
                ValueDeletable::Value(v) => {
                    sst_builder.add(&kv.key, Some(&v))?;
                }
                ValueDeletable::Tombstone => {
                    sst_builder.add(&kv.key, None)?;
                }
            }
        }

        let encoded_sst = sst_builder.build()?;
        let handle = self.table_store.write_sst(id, encoded_sst).await?;
        Ok(handle)
    }

    async fn flush_imm_wal(&self, imm: Arc<ImmutableWal>) -> Result<SSTableHandle, SlateDBError> {
        let wal_id = tablestore::SsTableId::Wal(imm.id());
        self.flush_imm_table(&wal_id, imm.table()).await
    }

    fn flush_imm_wal_to_memtable(&self, mem_table: &mut WritableKVTable, imm_table: Arc<KVTable>) {
        let mut iter = imm_table.iter();
        while let Some(kv) = iter.next_entry_sync() {
            match kv.value {
                ValueDeletable::Value(v) => {
                    mem_table.put(&kv.key, &v);
                }
                ValueDeletable::Tombstone => {
                    mem_table.delete(&kv.key);
                }
            }
        }
    }

    async fn flush_imm_wals(&self) -> Result<(), SlateDBError> {
        while let Some(imm) = {
            let rguard = self.state.read();
            let snapshot = rguard.compacted.as_ref().clone();
            snapshot.imm_wal.back().cloned()
        } {
            self.flush_imm_wal(imm.clone()).await?;
            let mut wguard = self.state.write();
            let mut snapshot = wguard.compacted.as_ref().clone();
            snapshot.imm_wal.pop_back();
            wguard.compacted = Arc::new(snapshot);
            // flush to the memtable before notifying so that data is available for reads
            // once we support committed read isolation, reads should read from memtable first
            self.flush_imm_wal_to_memtable(&mut wguard.memtable, imm.table());
            self.maybe_freeze_memtable(&mut wguard, imm.id());
            imm.table().notify_flush();
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Option<std::thread::JoinHandle<()>> {
        let this = Arc::clone(self);
        Some(std::thread::spawn(move || {
            let ticker =
                crossbeam_channel::tick(Duration::from_millis(this.options.flush_ms as u64));
            loop {
                crossbeam_channel::select! {
                  // Tick to freeze and flush the memtable
                  recv(ticker) -> _ => {
                    let _ = block_on(this.flush());
                  }
                  // Stop the thread.
                  recv(rx) -> _ => return
                }
            }
        }))
    }
}
