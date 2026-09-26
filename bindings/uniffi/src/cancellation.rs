use std::sync::Arc;

/// A handle that stops a foreground `Admin` loop such as `run_gc` or
/// `run_compactor`. Cancelling is idempotent and may happen from any thread
/// before or after the loop starts; a loop started with an already cancelled
/// token shuts down at once.
#[derive(uniffi::Object)]
pub struct CancellationToken {
    pub(crate) inner: tokio_util::sync::CancellationToken,
}

#[uniffi::export]
impl CancellationToken {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: tokio_util::sync::CancellationToken::new(),
        })
    }

    /// Requests shutdown of every loop holding this token.
    pub fn cancel(&self) {
        self.inner.cancel();
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_is_visible_and_idempotent() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
        token.cancel();
        token.cancel();
        assert!(token.is_cancelled());
        assert!(token.inner.is_cancelled());
    }
}
