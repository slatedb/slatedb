use std::sync::Arc;
use std::time::Duration;
use crate::mem_table::MemTable;
use crate::db::DbInner;

impl DbInner {
  pub(crate) fn flush(&self) {
    if let Some(imm_memtable) = self.freeze_memtable() {
      // TODO write memtable to disk
      // TODO upload memtable to obj storage

      // Notify clients that their writes have been flushed.
      imm_memtable.flush_notify.notify_waiters();

      // TODO remove memtable from imm_memtables
    }
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
    snapshot.imm_memtables.insert(0, old_memtable.clone());
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
      let ticker = crossbeam_channel::tick(Duration::from_millis(this.options.flush_ms as u64));
      loop {
        crossbeam_channel::select! {
          // Tick to freeze and flush the memtable
          recv(ticker) -> _ => this.flush(),
          // Stop the thread.
          recv(rx) -> _ => return
        }
      }
    }))
  }
}