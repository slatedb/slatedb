use std::sync::Arc;

use slatedb::bytes::Bytes;

use crate::error::BlockTransformerCallbackError;

/// Application-provided reversible transform of every SST block, data,
/// index, filter and stats alike, applied after compression and before the
/// checksum. `decode` must invert `encode` for every block the database
/// wrote; the engine records no transformer identity, key id or format
/// version, so a block that needs one carries it inside its own bytes.
///
/// The engine calls the methods from Tokio's blocking pool, one call per
/// block, so a slow transform holds no runtime worker but still delays the
/// block it transforms.
#[uniffi::export(with_foreign)]
pub trait BlockTransformer: Send + Sync {
    fn encode(&self, data: Vec<u8>) -> Result<Vec<u8>, BlockTransformerCallbackError>;

    fn decode(&self, data: Vec<u8>) -> Result<Vec<u8>, BlockTransformerCallbackError>;
}

struct BlockTransformerAdapter {
    inner: Arc<dyn BlockTransformer>,
}

#[async_trait::async_trait]
impl slatedb::BlockTransformer for BlockTransformerAdapter {
    async fn encode(&self, data: Bytes) -> Result<Bytes, slatedb::Error> {
        let inner = self.inner.clone();
        call_foreign(move || inner.encode(Vec::from(data)))
            .await
            .map_err(|error| slatedb::Error::data(format!("block transformer encode: {error}")))
    }

    async fn decode(&self, data: Bytes) -> Result<Bytes, slatedb::Error> {
        let inner = self.inner.clone();
        call_foreign(move || inner.decode(Vec::from(data)))
            .await
            .map_err(|error| slatedb::Error::data(format!("block transformer decode: {error}")))
    }
}

/// Runs the synchronous foreign method on Tokio's blocking pool so the
/// runtime worker polling the block stays free while the foreign runtime
/// (cgo, the GIL, JNA, Node) holds the call.
async fn call_foreign<F>(call: F) -> Result<Bytes, BlockTransformerCallbackError>
where
    F: FnOnce() -> Result<Vec<u8>, BlockTransformerCallbackError> + Send + 'static,
{
    tokio::task::spawn_blocking(call)
        .await
        .unwrap_or_else(|join_error| {
            Err(BlockTransformerCallbackError::Failed {
                message: join_error.to_string(),
            })
        })
        .map(Bytes::from)
}

pub(crate) fn adapt_block_transformer(
    inner: Arc<dyn BlockTransformer>,
) -> Arc<dyn slatedb::BlockTransformer> {
    Arc::new(BlockTransformerAdapter { inner })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Flips every byte: its own inverse, and any block it did not write
    /// decodes to garbage the block decoder refuses.
    pub(crate) struct Flip;

    impl BlockTransformer for Flip {
        fn encode(&self, data: Vec<u8>) -> Result<Vec<u8>, BlockTransformerCallbackError> {
            Ok(data.into_iter().map(|b| !b).collect())
        }

        fn decode(&self, data: Vec<u8>) -> Result<Vec<u8>, BlockTransformerCallbackError> {
            Ok(data.into_iter().map(|b| !b).collect())
        }
    }

    pub(crate) struct Refusing;

    impl BlockTransformer for Refusing {
        fn encode(&self, data: Vec<u8>) -> Result<Vec<u8>, BlockTransformerCallbackError> {
            Ok(data)
        }

        fn decode(&self, _: Vec<u8>) -> Result<Vec<u8>, BlockTransformerCallbackError> {
            Err(BlockTransformerCallbackError::Failed {
                message: "no key".to_owned(),
            })
        }
    }

    #[test]
    fn an_unknown_foreign_error_is_a_failed_callback_error() {
        let error = <BlockTransformerCallbackError as uniffi::ConvertError<crate::UniFfiTag>>::try_convert_unexpected_callback_error(
            uniffi::UnexpectedUniFFICallbackError::new("decrypt: wrong key"),
        )
        .expect("an unknown foreign error must convert instead of panicking the dispatcher");
        assert!(
            matches!(&error, BlockTransformerCallbackError::Failed { message } if message == "decrypt: wrong key"),
            "{error:?}"
        );
        let unnamed =
            BlockTransformerCallbackError::from(uniffi::UnexpectedUniFFICallbackError::new(""));
        assert!(
            matches!(&unnamed, BlockTransformerCallbackError::Failed { message } if !message.is_empty()),
            "{unnamed:?}"
        );
    }

    /// Holds every call until the test releases it from a task on the same
    /// runtime; a callback that blocks the runtime's only worker never sees
    /// the release.
    struct Gated {
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl BlockTransformer for Gated {
        fn encode(&self, data: Vec<u8>) -> Result<Vec<u8>, BlockTransformerCallbackError> {
            self.release
                .lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(5))
                .map(|()| data)
                .map_err(|_| BlockTransformerCallbackError::Failed {
                    message: "the runtime worker was blocked by the callback".to_owned(),
                })
        }

        fn decode(&self, data: Vec<u8>) -> Result<Vec<u8>, BlockTransformerCallbackError> {
            self.encode(data)
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_foreign_callback_does_not_block_the_runtime_worker() {
        let (release, wait) = std::sync::mpsc::channel();
        let gated = adapt_block_transformer(Arc::new(Gated {
            release: std::sync::Mutex::new(wait),
        }));
        let releaser = tokio::spawn(async move { release.send(()).unwrap() });
        let block = Bytes::from_static(b"block bytes");
        assert_eq!(gated.encode(block.clone()).await.unwrap(), block);
        releaser.await.unwrap();
    }

    #[tokio::test]
    async fn adapter_round_trips_and_names_a_refusal() {
        let flip = adapt_block_transformer(Arc::new(Flip));
        let block = Bytes::from_static(b"block bytes");
        let encoded = flip.encode(block.clone()).await.unwrap();
        assert_ne!(encoded, block);
        assert_eq!(flip.decode(encoded).await.unwrap(), block);

        let refusing = adapt_block_transformer(Arc::new(Refusing));
        let error = refusing.decode(block).await.unwrap_err();
        assert_eq!(error.kind(), slatedb::ErrorKind::Data);
        assert!(error.to_string().contains("no key"));
    }
}
