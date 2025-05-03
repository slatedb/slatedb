pub(crate) trait WalIdAccess: Send + Sync + 'static {
    fn increment_wal_id(&self) -> u64;
}
