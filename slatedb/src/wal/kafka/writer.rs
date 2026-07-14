use super::codec::encode_write_batch;
use super::observer::SharedWalStatus;
use super::{internal_error, producer_to_wal_error, PARTITION};
use crate::wal::{FlushResultFuture, WalError, WalObserver, WalStatus, WalWriter};
use crate::RowEntry;
use futures::FutureExt;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use slatedb_common::clock::SystemClock;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

const COMMAND_BUFFER_SIZE: usize = 128;
const QUEUE_FULL_RETRY_DELAY: Duration = Duration::from_millis(10);

pub struct KafkaWalWriter {
    commands: mpsc::Sender<WriterCommand>,
    status: SharedWalStatus,
    task: Option<tokio::task::JoinHandle<()>>,
    epoch: u64,
    closed: bool,
}

impl KafkaWalWriter {
    pub(super) fn start(
        producer: FutureProducer,
        topic: String,
        epoch: u64,
        commit_interval: Duration,
        last_flushed_wal_id: u64,
        clock: Arc<dyn SystemClock>,
    ) -> Self {
        let status = SharedWalStatus::new(last_flushed_wal_id);
        let (commands, command_rx) = mpsc::channel(COMMAND_BUFFER_SIZE);
        let worker = WriterWorker {
            producer,
            topic,
            commit_interval,
            commands: command_rx,
            pending: Vec::new(),
            status: status.clone(),
            clock,
        };
        let task = tokio::spawn(worker.run());
        Self {
            commands,
            status,
            task: Some(task),
            epoch,
            closed: false,
        }
    }

    fn closed_error(&self) -> WalError {
        match self.status.status() {
            Err(status) => status.closed_reason.unwrap_or(WalError::Closed),
            Ok(_) => WalError::Closed,
        }
    }
}

#[async_trait::async_trait]
impl WalWriter for KafkaWalWriter {
    async fn append(&mut self, write_batch: &[RowEntry]) -> Result<(), WalError> {
        if self.closed {
            return Err(self.closed_error());
        }
        let seq = write_batch.first().map(|row| row.seq);
        if let Some(seq) = seq {
            if write_batch.iter().any(|row| row.seq != seq) {
                return Err(internal_error(
                    "a Kafka WAL append contained multiple sequence numbers",
                ));
            }
        }
        // Serialization happens here, and append does not return until this
        // exact batch has been enqueued as one Kafka record.
        let encoded = encode_write_batch(self.epoch, write_batch)?;
        let (result_tx, result_rx) = oneshot::channel();
        self.commands
            .send(WriterCommand::Append {
                encoded,
                seq,
                result: result_tx,
            })
            .await
            .map_err(|_| self.closed_error())?;
        result_rx.await.unwrap_or_else(|_| Err(self.closed_error()))
    }

    async fn flush(&mut self) -> Result<FlushResultFuture, WalError> {
        if self.closed {
            return Err(self.closed_error());
        }
        let (result_tx, result_rx) = oneshot::channel();
        self.commands
            .send(WriterCommand::Flush { result: result_tx })
            .await
            .map_err(|_| self.closed_error())?;
        Ok(async move { result_rx.await.unwrap_or(Err(WalError::Closed)) }.boxed())
    }

    fn observer(&self) -> Box<dyn WalObserver> {
        Box::new(self.status.observer())
    }

    fn status(&self) -> Result<WalStatus, WalStatus> {
        self.status.status()
    }

    async fn close(&mut self) -> Result<(), WalError> {
        if self.closed {
            return match self.status.status() {
                Err(status) if matches!(status.closed_reason, Some(WalError::Closed)) => Ok(()),
                Err(status) => Err(status.into()),
                Ok(_) => Ok(()),
            };
        }
        self.closed = true;
        let (result_tx, result_rx) = oneshot::channel();
        let result = if self
            .commands
            .send(WriterCommand::Close { result: result_tx })
            .await
            .is_err()
        {
            Err(self.closed_error())
        } else {
            result_rx.await.unwrap_or_else(|_| Err(self.closed_error()))
        };
        if let Some(task) = self.task.take() {
            if let Err(error) = task.await {
                return Err(internal_error(format!(
                    "Kafka WAL writer task failed: {error}"
                )));
            }
        }
        result
    }
}

enum WriterCommand {
    Append {
        encoded: Vec<u8>,
        seq: Option<u64>,
        result: oneshot::Sender<Result<(), WalError>>,
    },
    Flush {
        result: oneshot::Sender<Result<(), WalError>>,
    },
    Close {
        result: oneshot::Sender<Result<(), WalError>>,
    },
}

struct PendingRecord {
    delivery: DeliveryFuture,
    seq: Option<u64>,
}

struct WriterWorker {
    producer: FutureProducer,
    topic: String,
    commit_interval: Duration,
    commands: mpsc::Receiver<WriterCommand>,
    pending: Vec<PendingRecord>,
    status: SharedWalStatus,
    clock: Arc<dyn SystemClock>,
}

impl WriterWorker {
    async fn run(mut self) {
        let result = self.run_loop().await;
        if let Err(error) = result {
            self.status.close(error.clone());
            self.commands.close();
            while let Some(command) = self.commands.recv().await {
                command.complete(Err(error.clone()));
            }
        }
    }

    async fn run_loop(&mut self) -> Result<(), WalError> {
        let clock = self.clock.clone();
        let mut commits = clock.ticker(self.commit_interval);
        // SystemClock tickers, like Tokio intervals, tick immediately once.
        // Consume that tick so the first commit happens after a full interval.
        commits.tick().await;

        loop {
            tokio::select! {
                _ = commits.tick() => {
                    self.commit_pending(true).await?;
                }
                command = self.commands.recv() => {
                    match command {
                        Some(WriterCommand::Append { encoded, seq, result }) => {
                            let send_result = self.enqueue(encoded, seq).await;
                            let failed = send_result.is_err();
                            let _ = result.send(send_result.clone());
                            if failed {
                                return send_result;
                            }
                        }
                        Some(WriterCommand::Flush { result }) => {
                            let flush_result = self.commit_pending(true).await;
                            let failed = flush_result.is_err();
                            let _ = result.send(flush_result.clone());
                            if failed {
                                return flush_result;
                            }
                        }
                        Some(WriterCommand::Close { result }) => {
                            let close_result = self.commit_pending(false).await;
                            let _ = result.send(close_result.clone());
                            match close_result {
                                Ok(()) => {
                                    self.status.close(WalError::Closed);
                                    return Ok(());
                                }
                                Err(error) => return Err(error),
                            }
                        }
                        None => {
                            self.commit_pending(false).await?;
                            self.status.close(WalError::Closed);
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    async fn enqueue(&mut self, encoded: Vec<u8>, seq: Option<u64>) -> Result<(), WalError> {
        let delivery =
            enqueue_with_retry(&self.producer, &self.topic, &encoded, self.clock.as_ref()).await?;
        self.status.record_buffered(encoded.len());
        self.pending.push(PendingRecord { delivery, seq });
        Ok(())
    }

    async fn commit_pending(&mut self, begin_next: bool) -> Result<(), WalError> {
        if self.pending.is_empty() {
            return Ok(());
        }

        let producer = self.producer.clone();
        #[allow(clippy::disallowed_methods)]
        tokio::task::spawn_blocking(move || producer.commit_transaction(Timeout::Never))
            .await
            .map_err(|error| {
                internal_error(format!("Kafka transaction commit task failed: {error}"))
            })?
            .map_err(|error| producer_to_wal_error(&self.producer, error))?;

        let pending = std::mem::take(&mut self.pending);
        let mut last_offset = None;
        let mut last_seq = None;
        for record in pending {
            let delivery = record
                .delivery
                .await
                .map_err(|_| internal_error("Kafka producer dropped before record delivery"))?
                .map_err(|(error, _)| producer_to_wal_error(&self.producer, error))?;
            if delivery.partition != PARTITION || delivery.offset < 0 {
                return Err(internal_error(format!(
                    "Kafka WAL record was delivered to invalid partition/offset {}/{}",
                    delivery.partition, delivery.offset
                )));
            }
            last_offset = Some(delivery.offset as u64);
            if record.seq.is_some() {
                last_seq = record.seq;
            }
        }

        let last_offset = last_offset.expect("a non-empty pending transaction has a delivery");
        self.status.record_flushed(last_offset, last_seq);
        if begin_next {
            self.producer
                .begin_transaction()
                .map_err(|error| producer_to_wal_error(&self.producer, error))?;
        }
        Ok(())
    }
}

impl WriterCommand {
    fn complete(self, result: Result<(), WalError>) {
        match self {
            WriterCommand::Append { result: tx, .. }
            | WriterCommand::Flush { result: tx }
            | WriterCommand::Close { result: tx } => {
                let _ = tx.send(result);
            }
        }
    }
}

pub(super) async fn enqueue_with_retry(
    producer: &FutureProducer,
    topic: &str,
    encoded: &[u8],
    clock: &dyn SystemClock,
) -> Result<DeliveryFuture, WalError> {
    loop {
        let record = FutureRecord::<(), [u8]>::to(topic)
            .partition(PARTITION)
            .payload(encoded);
        match producer.send_result(record) {
            Ok(delivery) => return Ok(delivery),
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
                clock.sleep(QUEUE_FULL_RETRY_DELAY).await;
            }
            Err((error, _)) => return Err(producer_to_wal_error(producer, error)),
        }
    }
}

impl From<super::codec::CodecError> for WalError {
    fn from(error: super::codec::CodecError) -> Self {
        WalError::InternalError(std::sync::Arc::new(error))
    }
}
