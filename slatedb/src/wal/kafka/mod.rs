//! Kafka-backed implementation of SlateDB's pluggable WAL traits.
//!
//! A Kafka record is one WAL file, and each data record contains exactly one
//! SlateDB write batch. Empty fence records delimit writer epochs. A record's
//! partition offset is its WAL file ID. The topic must already exist with
//! exactly one partition; records are always sent explicitly to partition 0.
//! Writers sharing a topic must also share a transactional ID so Kafka can
//! fence the previous writer during [`WriterInit::fence_and_init`].

mod codec;
mod observer;
mod reader;
#[cfg(test)]
mod tests;
mod writer;

pub use observer::KafkaWalObserver;
pub use reader::{KafkaWalIterator, KafkaWalReader};
pub use writer::KafkaWalWriter;

use crate::wal::{
    WalError, WalFileRange, WalIterator, WalReader, WriterInit, WriterInitResult, WriterManifest,
};
use async_trait::async_trait;
use codec::encode_fence;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{FutureProducer, Producer};
use rdkafka::util::Timeout;
use slatedb_common::clock::{DefaultSystemClock, SystemClock};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;
use writer::enqueue_with_retry;

/// Default interval between Kafka WAL transaction commits.
pub const DEFAULT_COMMIT_INTERVAL: Duration = Duration::from_millis(100);
const PARTITION: i32 = 0;

/// Kafka WAL factory implementing both writer initialization and WAL reads.
#[derive(Clone)]
pub struct KafkaWal {
    topic: String,
    transactional_id: String,
    producer_config: ClientConfig,
    consumer_config: ClientConfig,
    commit_interval: Duration,
    clock: Arc<dyn SystemClock>,
}

impl KafkaWal {
    /// Creates a Kafka WAL with the default 100ms commit interval.
    pub fn new(
        bootstrap_servers: impl Into<String>,
        topic: impl Into<String>,
        transactional_id: impl Into<String>,
    ) -> Self {
        Self::new_with_commit_interval(
            bootstrap_servers,
            topic,
            transactional_id,
            DEFAULT_COMMIT_INTERVAL,
        )
    }

    /// Creates a Kafka WAL with a caller-specified transaction commit interval.
    pub fn new_with_commit_interval(
        bootstrap_servers: impl Into<String>,
        topic: impl Into<String>,
        transactional_id: impl Into<String>,
        commit_interval: Duration,
    ) -> Self {
        let bootstrap_servers = bootstrap_servers.into();
        let mut producer_config = ClientConfig::new();
        producer_config.set("bootstrap.servers", &bootstrap_servers);
        let mut consumer_config = ClientConfig::new();
        consumer_config.set("bootstrap.servers", bootstrap_servers);
        Self::from_configs(
            topic,
            transactional_id,
            producer_config,
            consumer_config,
            commit_interval,
        )
    }

    /// Creates a Kafka WAL from custom librdkafka configurations.
    ///
    /// This is useful for TLS, SASL, and other cluster-specific settings. WAL
    /// correctness settings such as idempotence, `read_committed`, and the
    /// transactional ID are enforced when clients are created.
    pub fn from_configs(
        topic: impl Into<String>,
        transactional_id: impl Into<String>,
        producer_config: ClientConfig,
        consumer_config: ClientConfig,
        commit_interval: Duration,
    ) -> Self {
        assert!(
            !commit_interval.is_zero(),
            "Kafka WAL commit interval must be non-zero"
        );
        Self {
            topic: topic.into(),
            transactional_id: transactional_id.into(),
            producer_config,
            consumer_config,
            commit_interval,
            clock: Arc::new(DefaultSystemClock::new()),
        }
    }

    /// Returns an independently usable Kafka WAL reader.
    pub fn reader(&self) -> KafkaWalReader {
        KafkaWalReader::new(
            self.topic.clone(),
            self.consumer_config.clone(),
            self.clock.clone(),
        )
    }

    fn create_producer(&self) -> Result<FutureProducer, WalError> {
        let mut config = self.producer_config.clone();
        config
            .set("transactional.id", &self.transactional_id)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("allow.auto.create.topics", "false");
        config.create().map_err(kafka_to_wal_error)
    }

    async fn initialize_transactions(&self, producer: &FutureProducer) -> Result<(), WalError> {
        let blocking_producer = producer.clone();
        #[allow(clippy::disallowed_methods)]
        tokio::task::spawn_blocking(move || blocking_producer.init_transactions(Timeout::Never))
            .await
            .map_err(|error| {
                internal_error(format!(
                    "Kafka transaction initialization task failed: {error}"
                ))
            })?
            .map_err(|error| producer_to_wal_error(producer, error))
    }

    async fn verify_topic(&self, producer: &FutureProducer) -> Result<(), WalError> {
        let producer = producer.clone();
        let topic_name = self.topic.clone();
        #[allow(clippy::disallowed_methods)]
        tokio::task::spawn_blocking(move || {
            let metadata = producer
                .client()
                .fetch_metadata(Some(&topic_name), Timeout::Never)
                .map_err(kafka_to_wal_error)?;
            let topic = metadata
                .topics()
                .iter()
                .find(|topic| topic.name() == topic_name)
                .ok_or_else(|| {
                    internal_error(format!("Kafka WAL topic '{topic_name}' does not exist"))
                })?;
            if let Some(error) = topic.error() {
                return Err(internal_error(format!(
                    "unable to access Kafka WAL topic '{topic_name}': {error:?}"
                )));
            }
            if topic.partitions().len() != 1 || topic.partitions()[0].id() != PARTITION {
                return Err(internal_error(format!(
                    "Kafka WAL topic '{topic_name}' must contain exactly partition 0"
                )));
            }
            Ok(())
        })
        .await
        .map_err(|error| internal_error(format!("Kafka metadata task failed: {error}")))?
    }

    async fn commit_transaction(&self, producer: &FutureProducer) -> Result<(), WalError> {
        let blocking_producer = producer.clone();
        #[allow(clippy::disallowed_methods)]
        tokio::task::spawn_blocking(move || blocking_producer.commit_transaction(Timeout::Never))
            .await
            .map_err(|error| {
                internal_error(format!("Kafka transaction commit task failed: {error}"))
            })?
            .map_err(|error| producer_to_wal_error(producer, error))
    }
}

#[async_trait]
impl WriterInit for KafkaWal {
    async fn fence_and_init(
        &self,
        manifest: &mut WriterManifest,
    ) -> Result<WriterInitResult, WalError> {
        let epoch = manifest.epoch();
        let replay_after_wal_id = manifest.replay_after_wal_id();
        let producer = self.create_producer()?;

        // init_transactions obtains a new producer epoch and fences any older
        // producer using this transactional ID.
        self.initialize_transactions(&producer).await?;
        self.verify_topic(&producer).await?;
        producer
            .begin_transaction()
            .map_err(|error| producer_to_wal_error(&producer, error))?;

        // A committed empty record establishes an exact, visible upper bound
        // for replay even when prior transactions were aborted.
        let fence_record = encode_fence(epoch);
        let delivery =
            enqueue_with_retry(&producer, &self.topic, &fence_record, self.clock.as_ref()).await?;
        self.commit_transaction(&producer).await?;
        let delivery = delivery
            .await
            .map_err(|_| internal_error("Kafka producer dropped before fence delivery"))?
            .map_err(|(error, _)| producer_to_wal_error(&producer, error))?;
        if delivery.partition != PARTITION || delivery.offset < 0 {
            return Err(internal_error(format!(
                "Kafka WAL fence was delivered to invalid partition/offset {}/{}",
                delivery.partition, delivery.offset
            )));
        }
        let fencing_wal_id = delivery.offset as u64;
        if replay_after_wal_id > fencing_wal_id {
            return Err(internal_error(format!(
                "manifest replay WAL ID {replay_after_wal_id} is past Kafka WAL end {fencing_wal_id}"
            )));
        }

        let replay_start = replay_after_wal_id
            .checked_add(1)
            .ok_or_else(|| internal_error("Kafka WAL replay start overflow"))?;
        let replay_end = fencing_wal_id
            .checked_add(1)
            .ok_or_else(|| internal_error("Kafka WAL replay end overflow"))?;
        let replay_iterator = self
            .reader()
            .iterator((replay_start..replay_end).into())
            .await?;

        producer
            .begin_transaction()
            .map_err(|error| producer_to_wal_error(&producer, error))?;
        let wal_writer = KafkaWalWriter::start(
            producer,
            self.topic.clone(),
            epoch,
            self.commit_interval,
            fencing_wal_id,
            self.clock.clone(),
        );
        Ok(WriterInitResult {
            replay_iterator,
            wal_writer: Box::new(wal_writer),
        })
    }
}

#[async_trait]
impl WalReader for KafkaWal {
    async fn iterator(
        &self,
        wal_file_id_range: WalFileRange,
    ) -> Result<Box<dyn WalIterator>, WalError> {
        self.reader().iterator(wal_file_id_range).await
    }
}

fn kafka_to_wal_error(error: KafkaError) -> WalError {
    match error.rdkafka_error_code() {
        Some(RDKafkaErrorCode::ProducerFenced | RDKafkaErrorCode::InvalidProducerEpoch) => {
            WalError::Fenced
        }
        _ => WalError::InternalError(Arc::new(error)),
    }
}

fn producer_to_wal_error(producer: &FutureProducer, error: KafkaError) -> WalError {
    if is_fencing_error(error.rdkafka_error_code())
        || producer
            .client()
            .fatal_error()
            .is_some_and(|(code, _)| is_fencing_error(Some(code)))
    {
        WalError::Fenced
    } else {
        WalError::InternalError(Arc::new(error))
    }
}

fn is_fencing_error(error: Option<RDKafkaErrorCode>) -> bool {
    matches!(
        error,
        Some(RDKafkaErrorCode::ProducerFenced | RDKafkaErrorCode::InvalidProducerEpoch)
    )
}

fn internal_error(message: impl Into<String>) -> WalError {
    WalError::InternalError(Arc::new(KafkaWalInternalError(message.into())))
}

#[derive(Debug)]
struct KafkaWalInternalError(String);

impl Display for KafkaWalInternalError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for KafkaWalInternalError {}
