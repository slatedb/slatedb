use std::error::Error;

use object_store::path::Path;

/// The `store` name on the [`object_store::Error::Generic`] that carries a
/// [`MirrorError`].
pub const MIRROR_STORE_NAME: &str = "ObjectStoreMirror";

/// Errors raised by the mirror itself, as opposed to errors from the wrapped
/// store.
///
/// The mirror returns these wrapped in [`object_store::Error::Generic`] with the
/// `MirrorError` as the source. Use [`MirrorError::find`] to get it back out.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum MirrorError {
    /// A `Local` route missed.
    #[error("object is not in the local mirror. path=`{path}`")]
    NotLocal { path: Path },

    /// Local disk I/O failed. Nothing was written remotely.
    #[error("local mirror I/O failed")]
    Local { source: std::io::Error },

    /// A write reached the remote store, but `observe` or local installation
    /// failed afterwards. The remote object exists.
    #[error("write reached the remote store but did not finish locally. path=`{path}`")]
    WriteCommitted {
        path: Path,
        source: Box<dyn Error + Send + Sync>,
    },

    /// The policy returned an error from a route or `observe` call.
    #[error("mirror policy failed")]
    Policy {
        source: Box<dyn Error + Send + Sync>,
    },

    /// The mirror doesn't support this operation: COPY, RENAME, an `Observe`
    /// multipart upload, or a local copy of an object whose file name (last
    /// path segment) is empty, ends in `.meta` or `.<digits>`, or is `meta` or
    /// all digits. Those names collide with the mirror's metadata and temp
    /// files.
    #[error("operation not supported by the mirror. operation=`{operation}`")]
    Unsupported { operation: &'static str },

    /// `ObjectStoreMirrorBuilder::build` was given an invalid option.
    #[error("invalid mirror configuration. message=`{message}`")]
    InvalidConfig { message: String },
}

impl MirrorError {
    /// Returns the first `MirrorError` in `err`'s source chain, including `err`
    /// itself.
    pub fn find<'a>(err: &'a (dyn Error + 'static)) -> Option<&'a MirrorError> {
        let mut current = Some(err);
        while let Some(err) = current {
            if let Some(mirror_err) = err.downcast_ref::<MirrorError>() {
                return Some(mirror_err);
            }
            // `io::Error::source` skips the wrapped error and returns its
            // source, so check the wrapped error directly.
            if let Some(mirror_err) = err
                .downcast_ref::<std::io::Error>()
                .and_then(|err| err.get_ref())
                .and_then(|inner| inner.downcast_ref::<MirrorError>())
            {
                return Some(mirror_err);
            }
            current = err.source();
        }
        None
    }
}

impl From<MirrorError> for object_store::Error {
    fn from(err: MirrorError) -> Self {
        object_store::Error::Generic {
            store: MIRROR_STORE_NAME,
            source: Box::new(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn not_local() -> MirrorError {
        MirrorError::NotLocal {
            path: Path::from("a/b.sst"),
        }
    }

    #[test]
    fn should_find_mirror_error_itself() {
        let err = not_local();
        assert!(matches!(
            MirrorError::find(&err),
            Some(MirrorError::NotLocal { .. })
        ));
    }

    #[test]
    fn should_find_mirror_error_in_object_store_error() {
        let err = object_store::Error::from(not_local());
        assert!(matches!(
            MirrorError::find(&err),
            Some(MirrorError::NotLocal { .. })
        ));
    }

    #[test]
    fn should_find_mirror_error_through_arc_and_io_error() {
        let err = Arc::new(object_store::Error::from(not_local()));
        assert!(MirrorError::find(&err).is_some());

        let err = std::io::Error::other(object_store::Error::from(not_local()));
        assert!(MirrorError::find(&err).is_some());

        let err = std::io::Error::other(not_local());
        assert!(MirrorError::find(&err).is_some());
    }

    #[test]
    fn should_not_find_mirror_error_in_other_errors() {
        let err = object_store::Error::Generic {
            store: "S3",
            source: Box::new(std::io::Error::other("timeout")),
        };
        assert!(MirrorError::find(&err).is_none());
    }
}
