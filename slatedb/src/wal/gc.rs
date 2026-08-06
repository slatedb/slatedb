use crate::config::GarbageCollectorDirectoryOptions;
use crate::db_state::SsTableId;
use crate::garbage_collector::stats::GcStats;
use crate::garbage_collector::{retain_allowed_by_gc_filter, GcFilter, GC_DELETE_CONCURRENCY};
use crate::tablestore::TableStore;
use crate::wal::{WalError, WalFileRange, WalGC};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use log::error;
use slatedb_common::clock::SystemClock;
use slatedb_common::object_metadata::IdentifiedObjectMetadata;
use std::ops::Bound;
use std::sync::Arc;

/// Selects which class of SlateDB WAL object is collected.
///
/// Regular WAL SSTs and zero-byte WAL fence objects share the same WAL
/// directory and `SsTableId::Wal` identifier space, but they have separate
/// retention policies. This mode keeps a single implementation while
/// allowing regular WAL GC and fence WAL GC to run on independent schedules.
#[derive(Debug, Clone, Copy)]
pub(crate) enum WalGcMode {
    /// Collect non-empty WAL SSTs that are old enough for retention and unreferenced by active
    /// manifests.
    Regular,

    /// Collect zero-byte WAL fence objects under the same safety checks as regular WAL GC.
    Fence,
}

impl WalGcMode {
    pub(crate) fn resource(self) -> &'static str {
        match self {
            WalGcMode::Regular => "WAL",
            WalGcMode::Fence => "WAL fence",
        }
    }
}

#[derive(Clone)]
pub(crate) struct SlateDbWalGc {
    table_store: Arc<TableStore>,
    stats: Arc<GcStats>,
    wal_options: GarbageCollectorDirectoryOptions,
    mode: WalGcMode,
    gc_filter: Option<Arc<dyn GcFilter>>,
    system_clock: Arc<dyn SystemClock>,
}

impl std::fmt::Debug for SlateDbWalGc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlateDbWalGc")
            .field("wal_options", &self.wal_options)
            .field("mode", &self.mode)
            .finish()
    }
}

impl SlateDbWalGc {
    pub(crate) fn new(
        table_store: Arc<TableStore>,
        stats: Arc<GcStats>,
        wal_options: GarbageCollectorDirectoryOptions,
        mode: WalGcMode,
        gc_filter: Option<Arc<dyn GcFilter>>,
        system_clock: Arc<dyn SystemClock>,
    ) -> Self {
        Self {
            table_store,
            stats,
            wal_options,
            mode,
            gc_filter,
            system_clock,
        }
    }

    fn is_wal_sst_eligible_for_deletion(
        utc_now: &DateTime<Utc>,
        wal_sst: &IdentifiedObjectMetadata<SsTableId>,
        min_age: &chrono::Duration,
        referenced_ranges: &[WalFileRange],
    ) -> bool {
        if utc_now.signed_duration_since(wal_sst.metadata.last_modified) <= *min_age {
            return false;
        }

        let wal_sst_id = wal_sst.id.unwrap_wal_id();
        !referenced_ranges
            .iter()
            .any(|range| Self::range_contains(range, wal_sst_id))
    }

    fn range_contains(range: &WalFileRange, wal_sst_id: u64) -> bool {
        let after_start = match &range.0 {
            Bound::Included(start) => wal_sst_id >= *start,
            Bound::Excluded(start) => wal_sst_id > *start,
            Bound::Unbounded => true,
        };
        let before_end = match &range.1 {
            Bound::Included(end) => wal_sst_id <= *end,
            Bound::Excluded(end) => wal_sst_id < *end,
            Bound::Unbounded => true,
        };
        after_start && before_end
    }

    fn wal_sst_min_age(&self) -> chrono::Duration {
        chrono::Duration::from_std(self.wal_options.min_age).expect("invalid duration")
    }

    /// Deletes the given WAL SSTs from the table store.
    ///
    /// In case of dryrun, the actual deletion doesn't happen.
    async fn maybe_delete_wal_ssts(&self, sst_ids: Vec<SsTableId>) {
        if self.wal_options.dry_run {
            if !sst_ids.is_empty() {
                log::info!(
                    "dry run: skipping {} deletion [count={}]",
                    self.mode.resource(),
                    sst_ids.len()
                );
                if matches!(self.mode, WalGcMode::Fence) {
                    log::info!(
                        "WAL fence GC is dry-run by default. This is a conservative setting. \
                        Set wal_fence_options.dry_run=false and use a conservative min_age to enable. \
                        Silence this log with wal_fence_options=None. See #352 for details."
                    );
                }
            }
            for id in sst_ids {
                log::debug!(
                    "dry run: would delete {} but skipped [id={:?}]",
                    self.mode.resource(),
                    id
                );
            }
            return;
        }

        futures::stream::iter(sst_ids)
            .for_each_concurrent(GC_DELETE_CONCURRENCY, |id| async move {
                if let Err(e) = self.table_store.delete_sst(&id).await {
                    error!("error deleting WAL SST [id={:?}, error={}]", id, e);
                } else {
                    match self.mode {
                        WalGcMode::Regular => self.stats.gc_wal_count.increment(1),
                        WalGcMode::Fence => self.stats.gc_wal_fence_count.increment(1),
                    }
                }
            })
            .await;
    }
}

#[async_trait]
impl WalGC for SlateDbWalGc {
    async fn collect(&self, referenced_ranges: Vec<WalFileRange>) -> Result<(), WalError> {
        let utc_now = self.system_clock.now();
        let min_age = self.wal_sst_min_age();
        let ssts_to_delete = self
            .table_store
            .list_wal_ssts(..)
            .await?
            .into_iter()
            .filter(|wal_sst| match self.mode {
                // In regular mode, only consider WAL SSTs with size > 0 for deletion.
                WalGcMode::Regular => wal_sst.metadata.size > 0,
                // In fence mode, only consider zero-byte WAL SSTs for deletion.
                WalGcMode::Fence => wal_sst.metadata.size == 0,
            })
            .filter(|wal_sst| {
                Self::is_wal_sst_eligible_for_deletion(
                    &utc_now,
                    wal_sst,
                    &min_age,
                    &referenced_ranges,
                )
            })
            .collect::<Vec<_>>();
        let ssts_to_delete = retain_allowed_by_gc_filter(&self.gc_filter, ssts_to_delete).await;
        let sst_ids_to_delete = ssts_to_delete
            .into_iter()
            .map(|wal_sst| wal_sst.id)
            .collect::<Vec<_>>();

        self.maybe_delete_wal_ssts(sst_ids_to_delete).await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_cache_policy::BlockCachePolicy;
    use crate::format::sst::SsTableFormat;
    use crate::object_stores::ObjectStores;
    use crate::tablestore::TableStoreKind;
    use crate::RowEntry;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use slatedb_common::clock::MockSystemClock;
    use slatedb_common::metrics::MetricsRecorderHelper;
    use std::time::Duration;

    fn build_table_store() -> Arc<TableStore> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            SsTableFormat::default(),
            Path::from("/"),
            None,
            TableStoreKind::GC,
            BlockCachePolicy::default(),
        ))
    }

    fn build_collector(
        table_store: Arc<TableStore>,
        clock: Arc<MockSystemClock>,
        mode: WalGcMode,
        min_age: Duration,
    ) -> SlateDbWalGc {
        SlateDbWalGc::new(
            table_store,
            Arc::new(GcStats::new(&MetricsRecorderHelper::noop())),
            GarbageCollectorDirectoryOptions {
                interval: None,
                min_age,
                dry_run: false,
            },
            mode,
            None,
            clock,
        )
    }

    async fn write_regular_wal(table_store: &Arc<TableStore>, wal_id: u64) {
        let mut sst = table_store.wal_table_builder();
        sst.add(RowEntry::new_value(b"key", b"value", wal_id))
            .await
            .unwrap();
        let sst = sst.build().await.unwrap();
        table_store
            .write_sst(&SsTableId::Wal(wal_id), &sst)
            .await
            .unwrap();
    }

    async fn write_fence_wal(table_store: &Arc<TableStore>, wal_id: u64) {
        table_store.write_wal_fence(wal_id).await.unwrap();
    }

    async fn wal_ids(table_store: &Arc<TableStore>) -> Vec<u64> {
        table_store
            .list_wal_ssts(..)
            .await
            .unwrap()
            .into_iter()
            .map(|wal| wal.id.unwrap_wal_id())
            .collect()
    }

    async fn make_all_wals_older_than(
        table_store: &Arc<TableStore>,
        clock: &MockSystemClock,
        min_age: Duration,
    ) {
        let newest_wal = table_store
            .list_wal_ssts(..)
            .await
            .unwrap()
            .into_iter()
            .map(|wal| wal.metadata.last_modified)
            .max()
            .expect("expected at least one WAL");
        let min_age_millis =
            i64::try_from(min_age.as_millis()).expect("min_age should fit in i64 milliseconds");
        clock.set(newest_wal.timestamp_millis() + min_age_millis + 1_000);
    }

    fn protect_outer_wals() -> Vec<WalFileRange> {
        vec![
            WalFileRange(Bound::Included(1), Bound::Excluded(2)),
            WalFileRange(Bound::Included(4), Bound::Unbounded),
        ]
    }

    #[tokio::test]
    async fn regular_mode_deletes_unreferenced_range_and_keeps_referenced_wals() {
        let table_store = build_table_store();
        let clock = Arc::new(MockSystemClock::new());
        for wal_id in 1..=4 {
            write_regular_wal(&table_store, wal_id).await;
        }
        make_all_wals_older_than(&table_store, &clock, Duration::ZERO).await;
        let collector = build_collector(
            table_store.clone(),
            clock,
            WalGcMode::Regular,
            Duration::ZERO,
        );

        collector.collect(protect_outer_wals()).await.unwrap();

        assert_eq!(wal_ids(&table_store).await, vec![1, 4]);
    }

    #[tokio::test]
    async fn regular_mode_does_not_touch_fence_wals() {
        let table_store = build_table_store();
        let clock = Arc::new(MockSystemClock::new());
        write_regular_wal(&table_store, 1).await;
        write_fence_wal(&table_store, 2).await;
        make_all_wals_older_than(&table_store, &clock, Duration::ZERO).await;
        let collector = build_collector(
            table_store.clone(),
            clock,
            WalGcMode::Regular,
            Duration::ZERO,
        );

        collector.collect(vec![]).await.unwrap();

        assert_eq!(wal_ids(&table_store).await, vec![2]);
    }

    #[tokio::test]
    async fn regular_mode_respects_min_age() {
        let table_store = build_table_store();
        let clock = Arc::new(MockSystemClock::new());
        write_regular_wal(&table_store, 1).await;
        let last_modified = table_store
            .metadata(&SsTableId::Wal(1))
            .await
            .unwrap()
            .last_modified;
        let min_age = Duration::from_secs(60 * 60);
        let collector = build_collector(
            table_store.clone(),
            clock.clone(),
            WalGcMode::Regular,
            min_age,
        );

        clock.set((last_modified + chrono::Duration::minutes(30)).timestamp_millis());
        collector.collect(vec![]).await.unwrap();
        assert_eq!(wal_ids(&table_store).await, vec![1]);

        clock.set((last_modified + chrono::Duration::minutes(61)).timestamp_millis());
        collector.collect(vec![]).await.unwrap();
        assert!(wal_ids(&table_store).await.is_empty());
    }

    #[tokio::test]
    async fn fence_mode_deletes_unreferenced_range_and_keeps_referenced_wals() {
        let table_store = build_table_store();
        let clock = Arc::new(MockSystemClock::new());
        for wal_id in 1..=4 {
            write_fence_wal(&table_store, wal_id).await;
        }
        make_all_wals_older_than(&table_store, &clock, Duration::ZERO).await;
        let collector =
            build_collector(table_store.clone(), clock, WalGcMode::Fence, Duration::ZERO);

        collector.collect(protect_outer_wals()).await.unwrap();

        assert_eq!(wal_ids(&table_store).await, vec![1, 4]);
    }

    #[tokio::test]
    async fn fence_mode_does_not_touch_regular_wals() {
        let table_store = build_table_store();
        let clock = Arc::new(MockSystemClock::new());
        write_fence_wal(&table_store, 1).await;
        write_regular_wal(&table_store, 2).await;
        make_all_wals_older_than(&table_store, &clock, Duration::ZERO).await;
        let collector =
            build_collector(table_store.clone(), clock, WalGcMode::Fence, Duration::ZERO);

        collector.collect(vec![]).await.unwrap();

        assert_eq!(wal_ids(&table_store).await, vec![2]);
    }

    #[tokio::test]
    async fn fence_mode_respects_min_age() {
        let table_store = build_table_store();
        let clock = Arc::new(MockSystemClock::new());
        write_fence_wal(&table_store, 1).await;
        let last_modified = table_store
            .metadata(&SsTableId::Wal(1))
            .await
            .unwrap()
            .last_modified;
        let min_age = Duration::from_secs(60 * 60);
        let collector = build_collector(
            table_store.clone(),
            clock.clone(),
            WalGcMode::Fence,
            min_age,
        );

        clock.set((last_modified + chrono::Duration::minutes(30)).timestamp_millis());
        collector.collect(vec![]).await.unwrap();
        assert_eq!(wal_ids(&table_store).await, vec![1]);

        clock.set((last_modified + chrono::Duration::minutes(61)).timestamp_millis());
        collector.collect(vec![]).await.unwrap();
        assert!(wal_ids(&table_store).await.is_empty());
    }
}
