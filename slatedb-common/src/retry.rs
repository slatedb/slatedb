//! Shared rules for retries of object store operations.

use std::error::Error;
use std::fmt;

/// Wraps an error to tell retry loops that they must return it without retries.
///
/// Stores can use this wrapper as the source of [`object_store::Error::Generic`].
/// The wrapper preserves the original error's display text and exposes it through
/// [`Error::source`].
#[derive(Debug)]
pub struct NonRetryable(pub Box<dyn Error + Send + Sync>);

impl fmt::Display for NonRetryable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl Error for NonRetryable {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}

impl NonRetryable {
    /// Finds this wrapper in an error or its sources, including inside [`std::io::Error`].
    pub fn find<'a>(mut err: &'a (dyn Error + 'static)) -> Option<&'a Self> {
        loop {
            if let Some(err) = err.downcast_ref::<Self>() {
                return Some(err);
            }
            // `io::Error::source()` skips the wrapped error, so visit it directly.
            err = match err.downcast_ref::<std::io::Error>() {
                Some(err) => err.get_ref()?,
                None => err.source()?,
            };
        }
    }
}

/// Returns whether an object store operation can be retried after this error.
///
/// Errors for missing objects, failed conditions, and unsupported operations stop
/// retries. A [`NonRetryable`] wrapper anywhere in the error's sources also stops retries.
pub fn should_retry(err: &object_store::Error) -> bool {
    !matches!(
        err,
        object_store::Error::AlreadyExists { .. }
            | object_store::Error::Precondition { .. }
            | object_store::Error::NotModified { .. }
            | object_store::Error::NotFound { .. }
            | object_store::Error::NotImplemented { .. }
            | object_store::Error::NotSupported { .. }
    ) && NonRetryable::find(err).is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generic(source: impl Error + Send + Sync + 'static) -> object_store::Error {
        object_store::Error::Generic {
            store: "test",
            source: Box::new(source),
        }
    }

    fn non_retryable() -> NonRetryable {
        NonRetryable(Box::new(std::io::Error::other("write already committed")))
    }

    #[test]
    fn should_preserve_display_and_original_error() {
        let err = non_retryable();
        assert_eq!(err.to_string(), "write already committed");
        let source = err.source().unwrap().downcast_ref::<std::io::Error>();
        assert!(source.is_some());
    }

    #[test]
    fn should_retry_transient_errors() {
        assert!(should_retry(&generic(std::io::Error::other("timeout"))));
    }

    #[test]
    fn should_not_retry_permanent_errors() {
        let errors = [
            object_store::Error::AlreadyExists {
                path: "a".into(),
                source: "exists".into(),
            },
            object_store::Error::Precondition {
                path: "a".into(),
                source: "condition failed".into(),
            },
            object_store::Error::NotModified {
                path: "a".into(),
                source: "unchanged".into(),
            },
            object_store::Error::NotFound {
                path: "a".into(),
                source: "missing".into(),
            },
            object_store::Error::NotImplemented {
                operation: "test".into(),
                implementer: "test".into(),
            },
            object_store::Error::NotSupported {
                source: "unsupported".into(),
            },
        ];
        for err in errors {
            assert!(!should_retry(&err), "unexpected retry for {err:?}");
        }
    }

    #[test]
    fn should_not_retry_tagged_errors_through_wrappers() {
        let errors = [
            generic(non_retryable()),
            generic(generic(non_retryable())),
            generic(std::io::Error::other(non_retryable())),
            generic(std::io::Error::other(generic(non_retryable()))),
            generic(std::io::Error::other(
                std::io::Error::other(non_retryable()),
            )),
        ];
        for err in errors {
            assert!(!should_retry(&err), "unexpected retry for {err:?}");
        }
    }
}
