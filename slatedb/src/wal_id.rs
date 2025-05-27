pub(crate) trait WalIdStore: Send + Sync + 'static {
    fn next_wal_id(&self) -> u64;
}
