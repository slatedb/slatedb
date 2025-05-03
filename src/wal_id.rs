pub(crate) trait WalIdIncrement: Send + Sync + 'static {
    fn increment(&self) -> u64;
}
