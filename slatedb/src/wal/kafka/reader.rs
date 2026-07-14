use super::codec::{decode_record, DecodedRecord};
use super::{internal_error, kafka_to_wal_error, PARTITION};
use crate::wal::{WalError, WalFileRange, WalIterator, WalReader, WalRows};
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;
use slatedb_common::clock::SystemClock;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const EOF_RETRY_DELAY: Duration = Duration::from_millis(10);
static NEXT_READER_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct KafkaWalReader {
    topic: String,
    consumer_config: ClientConfig,
    clock: Arc<dyn SystemClock>,
}

impl KafkaWalReader {
    pub(super) fn new(
        topic: String,
        consumer_config: ClientConfig,
        clock: Arc<dyn SystemClock>,
    ) -> Self {
        Self {
            topic,
            consumer_config,
            clock,
        }
    }

    fn create_consumer(&self) -> Result<StreamConsumer, WalError> {
        let reader_id = NEXT_READER_ID.fetch_add(1, Ordering::Relaxed);
        let mut config = self.consumer_config.clone();
        config
            .set(
                "group.id",
                format!(
                    "slatedb-kafka-wal-reader-{}-{reader_id}",
                    std::process::id()
                ),
            )
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "earliest")
            .set("isolation.level", "read_committed")
            .set("enable.partition.eof", "true")
            .set("allow.auto.create.topics", "false");
        config.create().map_err(kafka_to_wal_error)
    }
}

#[async_trait]
impl WalReader for KafkaWalReader {
    async fn iterator(
        &self,
        wal_file_id_range: WalFileRange,
    ) -> Result<Box<dyn WalIterator>, WalError> {
        let (start, end) = parse_range(wal_file_id_range)?;
        let kafka_start = i64::try_from(start)
            .map_err(|_| internal_error("Kafka WAL start offset exceeds i64::MAX"))?;
        if let Some(end) = end {
            i64::try_from(end)
                .map_err(|_| internal_error("Kafka WAL end offset exceeds i64::MAX"))?;
        }

        let consumer = Arc::new(self.create_consumer()?);
        let mut assignment = TopicPartitionList::new();
        assignment
            .add_partition_offset(&self.topic, PARTITION, Offset::Offset(kafka_start))
            .map_err(kafka_to_wal_error)?;
        consumer.assign(&assignment).map_err(kafka_to_wal_error)?;

        let iterator = KafkaWalIterator {
            consumer,
            topic: self.topic.clone(),
            next_offset: start,
            end_offset: end,
            clock: self.clock.clone(),
        };
        if !iterator.exhausted() {
            let (low, _) = iterator.watermarks().await?;
            if start < low {
                return Err(WalError::WalTruncated);
            }
        }
        Ok(Box::new(iterator))
    }
}

pub struct KafkaWalIterator {
    consumer: Arc<StreamConsumer>,
    topic: String,
    next_offset: u64,
    end_offset: Option<u64>,
    clock: Arc<dyn SystemClock>,
}

impl KafkaWalIterator {
    async fn watermarks(&self) -> Result<(u64, u64), WalError> {
        let consumer = self.consumer.clone();
        let topic = self.topic.clone();
        #[allow(clippy::disallowed_methods)]
        let (low, high) = tokio::task::spawn_blocking(move || {
            consumer.fetch_watermarks(&topic, PARTITION, Timeout::Never)
        })
        .await
        .map_err(|error| internal_error(format!("Kafka watermark fetch task failed: {error}")))?
        .map_err(kafka_to_wal_error)?;
        if low < 0 || high < 0 {
            return Err(internal_error(format!(
                "Kafka returned invalid watermarks {low}..{high}"
            )));
        }
        Ok((low as u64, high as u64))
    }

    fn exhausted(&self) -> bool {
        self.end_offset.is_some_and(|end| self.next_offset >= end)
    }
}

#[async_trait]
impl WalIterator for KafkaWalIterator {
    async fn next(&mut self) -> Result<Option<WalRows>, WalError> {
        loop {
            if self.exhausted() {
                return Ok(None);
            }

            match self.consumer.recv().await {
                Ok(message) => {
                    let offset = message.offset();
                    if offset < 0 {
                        return Err(internal_error(format!(
                            "Kafka returned invalid WAL offset {offset}"
                        )));
                    }
                    let offset = offset as u64;
                    if offset < self.next_offset {
                        continue;
                    }
                    if offset > self.next_offset {
                        // Kafka offsets may legitimately have gaps for aborted
                        // transactions and control batches. A gap below the
                        // low watermark, however, means retention deleted WAL
                        // files while this iterator was active.
                        let (low, _) = self.watermarks().await?;
                        if self.next_offset < low {
                            return Err(WalError::WalTruncated);
                        }
                    }
                    if self.end_offset.is_some_and(|end| offset >= end) {
                        self.next_offset = offset;
                        return Ok(None);
                    }
                    self.next_offset = offset
                        .checked_add(1)
                        .ok_or_else(|| internal_error("Kafka WAL offset overflow"))?;
                    let payload = message
                        .payload()
                        .ok_or_else(|| internal_error("Kafka WAL record has no payload"))?;
                    match decode_record(payload)? {
                        DecodedRecord::Fence { .. } => continue,
                        DecodedRecord::WriteBatch { rows, .. } => {
                            return Ok(Some(WalRows {
                                rows,
                                last_wal_file_id: offset,
                                last_in_file: true,
                            }));
                        }
                    }
                }
                Err(KafkaError::PartitionEOF(_)) => {
                    let (low, high) = self.watermarks().await?;
                    if self.next_offset < low {
                        return Err(WalError::WalTruncated);
                    }
                    if self.end_offset.is_some_and(|end| high >= end) {
                        self.next_offset = self.end_offset.expect("checked above");
                        return Ok(None);
                    }
                    self.clock.sleep(EOF_RETRY_DELAY).await;
                }
                Err(error) => return Err(kafka_to_wal_error(error)),
            }
        }
    }
}

fn parse_range(range: WalFileRange) -> Result<(u64, Option<u64>), WalError> {
    let WalFileRange(start, end) = range;
    let start = match start {
        Bound::Included(start) => start,
        Bound::Excluded(start) => start
            .checked_add(1)
            .ok_or_else(|| internal_error("Kafka WAL range start overflow"))?,
        Bound::Unbounded => {
            return Err(internal_error(
                "Kafka WAL iterator ranges must have a bounded start",
            ));
        }
    };
    let end = match end {
        Bound::Included(end) => Some(
            end.checked_add(1)
                .ok_or_else(|| internal_error("Kafka WAL range end overflow"))?,
        ),
        Bound::Excluded(end) => Some(end),
        Bound::Unbounded => None,
    };
    if end.is_some_and(|end| end < start) {
        return Err(internal_error("Kafka WAL iterator range is reversed"));
    }
    Ok((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_range_bounds() {
        assert_eq!(
            parse_range(WalFileRange(Bound::Included(2), Bound::Excluded(5))).unwrap(),
            (2, Some(5))
        );
        assert_eq!(
            parse_range(WalFileRange(Bound::Excluded(2), Bound::Included(5))).unwrap(),
            (3, Some(6))
        );
        assert_eq!(
            parse_range(WalFileRange(Bound::Included(2), Bound::Unbounded)).unwrap(),
            (2, None)
        );
    }

    #[test]
    fn rejects_unbounded_and_reversed_ranges() {
        assert!(parse_range(WalFileRange(Bound::Unbounded, Bound::Excluded(5))).is_err());
        assert!(parse_range(WalFileRange(Bound::Included(5), Bound::Excluded(2))).is_err());
    }
}
