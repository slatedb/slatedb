use crate::db::{DbInner, DbState};
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::mem_table::MemTable;
use crate::tablestore::SSTableHandle;
use crate::types::ValueDeletable;
use futures::executor::block_on;
use std::sync::Arc;
use std::time::Duration;

impl<'a> DbInner<'a> {
    pub(crate) async fn flush(&self) -> Result<(), SlateDBError> {
        self.freeze_memtable();
        self.flush_imms().await?;
        Ok(())
    }

    async fn flush_imm(
        &self,
        imm: Arc<MemTable>,
        id: usize,
    ) -> Result<SSTableHandle<'a>, SlateDBError> {
        let mut sst_builder = self.table_store.table_builder();
        let mut iter = imm.iter();
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

    async fn flush_imms(&self) -> Result<(), SlateDBError> {
        while let Some((imm, id)) = {
            let rguard = self.state.read();
            let snapshot: DbState = rguard.as_ref().clone();
            snapshot
                .imm_memtables
                .back()
                .map(|imm| (imm.clone(), snapshot.next_sst_id))
        } {
            let sst = self.flush_imm(imm.clone(), id).await?;
            let mut wguard = self.state.write();
            let mut snapshot = wguard.as_ref().clone();
            snapshot.imm_memtables.pop_back();
            // always put the new sst at the front of l0
            snapshot.l0.push_front(sst);
            snapshot.next_sst_id += 1;
            *wguard = Arc::new(snapshot);
            imm.flush_notify.notify_waiters();
        }
        Ok(())
    }

    /// Moves the current memtable to imm_memtables and creates a new memtable.
    fn freeze_memtable(&self) -> Option<Arc<MemTable>> {
        let mut guard = self.state.write();
        let mut snapshot = guard.as_ref().clone();

        // Skip if the memtable is empty.
        if snapshot.memtable.map.is_empty() {
            return None;
        }

        // Swap the current memtable with a new one.
        let old_memtable = std::mem::replace(&mut snapshot.memtable, Arc::new(MemTable::new()));
        // Add the memtable to the immutable memtables.
        snapshot.imm_memtables.push_front(old_memtable.clone());
        // Update the snapshot.
        *guard = Arc::new(snapshot);
        Some(old_memtable)
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
