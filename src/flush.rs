use crate::db::DbInner;
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::mem_table::{ImmutableWal, KVTable};
use crate::tablestore::{self, SSTableHandle};
use crate::types::ValueDeletable;
use futures::executor::block_on;
use std::sync::Arc;
use std::time::Duration;

impl DbInner {
    pub(crate) async fn flush(&self) -> Result<(), SlateDBError> {
        self.freeze_wal(&mut self.state.write());
        self.flush_imm_wals().await?;
        Ok(())
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

    async fn flush_imm_wals(&self) -> Result<(), SlateDBError> {
        while let Some(imm) = {
            let rguard = self.state.read();
            let snapshot = rguard.compacted.as_ref().clone();
            snapshot.imm_wal.back().cloned()
        } {
            let sst = self.flush_imm_wal(imm.clone()).await?;
            let mut wguard = self.state.write();
            let mut snapshot = wguard.compacted.as_ref().clone();
            snapshot.imm_wal.pop_back();
            // always put the new sst at the front of l0
            snapshot.wal_ssts.push_front(sst);
            wguard.compacted = Arc::new(snapshot);
            imm.table().notify_flush()
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
