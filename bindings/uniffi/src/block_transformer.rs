use std::sync::Arc;

use slatedb::bytes::Bytes;

use crate::error::BlockTransformerCallbackError;

/// Application-provided reversible transform of every SST block, data,
/// index, filter and stats alike, applied after compression and before the
/// checksum. `decode` must invert `encode` for every block the database
/// wrote; the engine records no transformer identity, key id or format
/// version, so a block that needs one carries it inside its own bytes.
///
/// The methods run on the engine's runtime threads for the length of the
/// call: a transform is a block's worth of CPU, not a remote call.
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
        self.inner
            .encode(data.to_vec())
            .map(Bytes::from)
            .map_err(|error| slatedb::Error::data(format!("block transformer encode: {error}")))
    }

    async fn decode(&self, data: Bytes) -> Result<Bytes, slatedb::Error> {
        self.inner
            .decode(data.to_vec())
            .map(Bytes::from)
            .map_err(|error| slatedb::Error::data(format!("block transformer decode: {error}")))
    }
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
