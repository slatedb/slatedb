use std::time::Duration;

use tokio::sync::watch;

use crate::error::{CloseReason, Error};
use crate::types::DbStatus;

/// A subscription to database status changes.
#[derive(uniffi::Object)]
pub struct DbStatusSubscription {
    receiver: watch::Receiver<slatedb::DbStatus>,
}

impl DbStatusSubscription {
    pub(crate) fn new(receiver: watch::Receiver<slatedb::DbStatus>) -> Self {
        Self { receiver }
    }
}

#[uniffi::export]
impl DbStatusSubscription {
    /// Returns the current status.
    pub fn status(&self) -> DbStatus {
        self.receiver.borrow().clone().into()
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl DbStatusSubscription {
    /// Waits for durability to advance or for the database to close.
    /// Returns None when the timeout expires.
    pub async fn wait_for_change(
        &self,
        durable_seq: u64,
        timeout_ms: u32,
    ) -> Result<Option<DbStatus>, Error> {
        let mut receiver = self.receiver.clone();
        let result = tokio::time::timeout(Duration::from_millis(timeout_ms.into()), async {
            receiver
                .wait_for(|status| {
                    status.durable_seq > durable_seq || status.close_reason.is_some()
                })
                .await
                .map(|status| status.clone())
        })
        .await;
        match result {
            Ok(Ok(status)) => Ok(Some(status.into())),
            Ok(Err(_)) => Err(Error::Closed {
                reason: CloseReason::Unknown,
                message: "Database status subscription closed".to_string(),
            }),
            Err(_) => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::task::{Context, Waker};

    use object_store::memory::InMemory;

    use super::*;

    async fn channel() -> (watch::Sender<slatedb::DbStatus>, DbStatusSubscription) {
        let db = slatedb::Db::builder("subscription", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap();
        let initial = db.status();
        db.close().await.unwrap();
        let (sender, receiver) = watch::channel(initial);
        (sender, DbStatusSubscription::new(receiver))
    }

    #[tokio::test]
    async fn timeout_and_cancellation_release_receivers() {
        let (sender, subscription) = channel().await;
        let sequence = subscription.status().durable_seq;
        for _ in 0..100 {
            assert!(subscription
                .wait_for_change(sequence, 1)
                .await
                .unwrap()
                .is_none());
            assert_eq!(sender.receiver_count(), 1);

            let mut waiting = Box::pin(subscription.wait_for_change(sequence, 5000));
            let mut context = Context::from_waker(Waker::noop());
            assert!(waiting.as_mut().poll(&mut context).is_pending());
            assert_eq!(sender.receiver_count(), 2);
            drop(waiting);
            assert_eq!(sender.receiver_count(), 1);
        }
        drop(subscription);
        assert_eq!(sender.receiver_count(), 0);
    }

    #[tokio::test]
    async fn updates_before_and_during_wait_are_observed() {
        let (sender, subscription) = channel().await;
        let sequence = subscription.status().durable_seq;
        let mut waiting = Box::pin(subscription.wait_for_change(sequence, 5000));
        let mut context = Context::from_waker(Waker::noop());
        assert!(waiting.as_mut().poll(&mut context).is_pending());
        sender.send_modify(|status| status.durable_seq += 1);
        assert_eq!(waiting.await.unwrap().unwrap().durable_seq, sequence + 1);
        assert_eq!(sender.receiver_count(), 1);
        assert_eq!(
            subscription
                .wait_for_change(sequence, 0)
                .await
                .unwrap()
                .unwrap()
                .durable_seq,
            sequence + 1
        );
        drop(sender);
        assert!(matches!(
            subscription.wait_for_change(sequence + 1, 5000).await,
            Err(Error::Closed { .. })
        ));
    }

    #[tokio::test]
    async fn native_subscription_observes_durable_writes_and_shutdown() {
        let inner = slatedb::Db::builder("native-subscription", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap();
        let db = crate::db::Db::new(inner);
        let subscription = db.subscribe();
        let sequence = subscription.status().durable_seq;
        let mut waiting = Box::pin(subscription.wait_for_change(sequence, 5000));
        let mut context = Context::from_waker(Waker::noop());
        assert!(waiting.as_mut().poll(&mut context).is_pending());
        let write = db.put(b"key".to_vec(), b"value".to_vec()).await.unwrap();
        write.await_durable().await.unwrap();
        let status = waiting.await.unwrap().unwrap();
        assert!(status.durable_seq > sequence);
        db.close().await.unwrap();
        let closed = subscription
            .wait_for_change(status.durable_seq, 5000)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(closed.close_reason, Some(CloseReason::Clean));
    }
}
