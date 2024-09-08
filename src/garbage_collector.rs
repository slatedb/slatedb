use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use tokio::runtime::Handle;

use crate::config::GarbageCollectorOptions;
use crate::error::SlateDBError;
use crate::garbage_collector::GarbageCollectorMessage::*;
use crate::manifest_store::{ManifestStore, StoredManifest};
use crate::metrics::DbStats;
use crate::tablestore::TableStore;

enum GarbageCollectorMessage {
    Shutdown,
}

pub(crate) struct GarbageCollector {
    main_tx: crossbeam_channel::Sender<GarbageCollectorMessage>,
    main_thread: Option<JoinHandle<()>>,
}

impl GarbageCollector {
    pub(crate) async fn new(
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        options: GarbageCollectorOptions,
        tokio_handle: Handle,
        db_stats: Arc<DbStats>,
    ) -> Result<Self, SlateDBError> {
        let (external_tx, external_rx) = crossbeam_channel::unbounded();
        let (err_tx, err_rx) = tokio::sync::oneshot::channel();
        let tokio_handle = options.compaction_runtime.clone().unwrap_or(tokio_handle);
        let main_thread = thread::spawn(move || {
            let load_result = GarbageCollectorOrchestrator::new(
                manifest_store,
                table_store,
                options,
                tokio_handle,
                external_rx,
                db_stats,
            );
            let mut orchestrator = match load_result {
                Ok(orchestrator) => orchestrator,
                Err(err) => {
                    err_tx.send(Err(err)).expect("err channel failure");
                    return;
                }
            };
            err_tx.send(Ok(())).expect("err channel failure");
            orchestrator.run();
        });
        err_rx.await.expect("err channel failure")?;
        Ok(Self {
            main_thread: Some(main_thread),
            main_tx: external_tx,
        })
    }

    /// Close the garbage collector
    pub(crate) async fn close(mut self) {
        if let Some(main_thread) = self.main_thread.take() {
            self.main_tx.send(Shutdown).expect("main tx disconnected");
            main_thread
                .join()
                .expect("failed to stop main compactor thread");
        }
    }
}

struct GarbageCollectorOrchestrator {
    manifest_store: Arc<ManifestStore>,
    table_store: Arc<TableStore>,
    options: GarbageCollectorOptions,
    stored_manifest: StoredManifest,
    tokio_handle: Handle,
    external_rx: crossbeam_channel::Receiver<GarbageCollectorMessage>,
    db_stats: Arc<DbStats>,
}

impl GarbageCollectorOrchestrator {
    fn new(
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        options: GarbageCollectorOptions,
        tokio_handle: Handle,
        external_rx: crossbeam_channel::Receiver<GarbageCollectorMessage>,
        db_stats: Arc<DbStats>,
    ) -> Result<Self, SlateDBError> {
        let stored_manifest =
            tokio_handle.block_on(StoredManifest::load(manifest_store.clone()))?;
        let Some(stored_manifest) = stored_manifest else {
            return Err(SlateDBError::InvalidDBState);
        };
        Ok(Self {
            manifest_store,
            table_store,
            options,
            stored_manifest,
            tokio_handle,
            external_rx,
            db_stats,
        })
    }

    pub fn run(&mut self) {
        let manifest_ticker = self
            .options
            .manifest_options
            .map_or(crossbeam_channel::never(), |opts| {
                crossbeam_channel::tick(opts.poll_interval)
            });
        let wal_ticker = self
            .options
            .wal_options
            .map_or(crossbeam_channel::never(), |opts| {
                crossbeam_channel::tick(opts.poll_interval)
            });
        let compacted_ticker = self
            .options
            .compacted_options
            .map_or(crossbeam_channel::never(), |opts| {
                crossbeam_channel::tick(opts.poll_interval)
            });

        loop {
            crossbeam_channel::select! {
                recv(manifest_ticker) -> _ => {
                    let min_age = self.options.manifest_options.map_or(
                        std::time::Duration::from_secs(86400),
                        |opts| opts.min_age
                    );
                    match self.tokio_handle.block_on(self.manifest_store.collect_garbage(min_age)) {
                        Ok(_) => {
                            self.db_stats.gc_manifest_count.inc();
                        }
                        Err(err) => {
                            log::error!("Error collecting garbage manifests: {}", err);
                        }
                    }
                }
                recv(wal_ticker) -> _ => {
                    let min_age = self.options.wal_options.map_or(
                        std::time::Duration::from_secs(86400),
                        |opts| opts.min_age
                    );

                    // Refresh the manifest so we have the latest last compacted id
                    match self.tokio_handle.block_on(self.stored_manifest.refresh()) {
                        Ok(_) => {}
                        Err(err) => {
                            log::error!("Error refreshing manifest: {}", err);
                            continue;
                        }
                    }

                    // Collect garbage WALs
                    match self.tokio_handle.block_on(
                        self.table_store.collect_garbage_wal(
                            min_age,
                            self.stored_manifest.db_state().last_compacted_wal_sst_id
                        )
                    ) {
                        Ok(_) => {
                            self.db_stats.gc_wal_count.inc();
                        }
                        Err(err) => {
                            log::error!("Error collecting garbage WALs: {}", err);
                        }
                    }
                }
                recv(compacted_ticker) -> _ => {
                }
                recv(self.external_rx) -> _ => {
                    break;
                }
            }
        }
    }
}
