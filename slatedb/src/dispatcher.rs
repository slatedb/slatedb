//! SlateDB makes use of [mpsc::channel] to offload work to background tasks when
//! possible. Examples of these tasks include [crate::mem_table_flush],
//! [crate::checkpoint], and [crate::compactor]. Each task's lifecycle is fairly
//! similar; they:
//!
//! 1. Receive messages and perform some work
//! 2. Perform work based on a fixed schedule
//! 3. Manage error states when failures occur
//! 4. Drain messages on shutdown
//! 5. Clean up resources on shutdown
//!
//! [MessageDispatcher] unifies this pattern into a single event loop implementation.
//!
//! Logic is then implemented in a [MessageHandler]. Handlers receive callbacks from
//! the dispatcher based on lifetime events (messages, ticks, etc.) and perform work
//! based on those events.
//!
//! ## Example
//!
//! ```ignore
//! # use slatedb::dispatcher::{MessageDispatcher, MessageHandler};
//! # use slatedb::error::SlateDBError;
//! # use slatedb::clock::SystemClock;
//! # use tokio::sync::mpsc;
//! # use tokio_util::sync::CancellationToken;
//! # #[tokio::main]
//! # async fn main() {
//! enum Message {
//!     Say(String),
//! }
//!
//! struct PrintMessageHandler;
//!
//! impl MessageHandler<Message> for PrintMessageHandler {
//!     async fn handle(
//!         &mut self,
//!         message: Message,
//!     ) -> Result<(), SlateDBError> {
//!         match message {
//!             Message::Say(msg) => println!("{}", msg),
//!         }
//!         Ok(())
//!     }
//!
//!     async fn tickers(&mut self) -> Vec<(Duration, Box<MessageFactory<Message>>)> {
//!         let mut tickers = Vec::new();
//!         tickers.push((Duration::from_secs(1), Box::new(|| Message::Say("tick".to_string()))));
//!         tickers
//!     }
//!
//!     async fn cleanup(
//!         &mut self,
//!         messages: BoxStream<'async_trait, T>,
//!         result: Result<(), SlateDBError>,
//!     ) -> Result<(), SlateDBError> {
//!         match result {
//!             Ok(_) | Err(SlateDBError::BackgroundTaskShutdown) => {
//!                 messages.for_each(|m| self.handle(m)).await;
//!             },
//!             // skipping drain messages on unclean shutdown
//!             _ => {},
//!         }
//!         Ok(())
//!     }
//! }
//!
//! let clock = SystemClock::default();
//! let cancellation_token = CancellationToken::new();
//! let (tx, rx) = mpsc::unbounded_channel();
//! let dispatcher = MessageDispatcher::new_with_channel(
//!     Box::new(PrintMessageHandler),
//!     clock,
//!     cancellation_token,
//!     None,
//! );
//! let join_handle = tokio::spawn(async move { dispatcher.run().await });
//! tx.send(Message::Say("hello".to_string())).await;
//! cancellation_token.cancel();
//! join_handle.await;
//! # }
//! ```

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::{future::select_all, stream::BoxStream};
use log::warn;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    clock::{SystemClock, SystemClockTicker},
    error::SlateDBError,
    utils::WatchableOnceCell,
};

/// A factory for creating messages when a [MessageDispatcherTicker] ticks.
pub(crate) type MessageFactory<T> = dyn Fn() -> T + Send;

/// A dispatcher that invokes [MessageHandler] callbacks when events occur.
///
/// [MessageDispatcher] receives a [MessageHandler] and optional
/// [mpsc::UnboundedReceiver<T>]. Messages sent to the receiver are passed to the
/// [MessageHandler] for processing.
///
/// [MessageDispatcher::run] is the primary entry point for running the dispatcher;
/// it is responsible for running the main event loop ([MessageDispatcher::run_loop])
/// and handling cleaning up after the event loop exits.
///
/// [MessageDispatcher::run_loop] implements the main event loop for the dispatcher.
/// The function receives messages from the [mpsc::UnboundedReceiver<T>] and from any
/// [MessageDispatcherTicker]s, and passes them to the [MessageHandler] for processing.
/// It also shuts down when the either [crate::db_state::DbState::error] is set or the
/// [tokio_util::sync::CancellationToken] is cancelled. See [MessageDispatcher::run_loop]
/// for more details on its behavior.
///
/// [crate::dispatcher] contains a complete code example.
pub(crate) struct MessageDispatcher<T: Send + std::fmt::Debug> {
    handler: Box<dyn MessageHandler<T>>,
    rx: mpsc::UnboundedReceiver<T>,
    clock: Arc<dyn SystemClock>,
    cancellation_token: CancellationToken,
    error_state: WatchableOnceCell<SlateDBError>,
}

impl<T: Send + std::fmt::Debug> MessageDispatcher<T> {
    /// Creates a new [MessageDispatcher] without a channel. This is useful for
    /// event handlers that only use tickers.
    ///
    /// ## Arguments
    ///
    /// * `handler`: The [MessageHandler] to use for processing messages.
    /// * `clock`: The [SystemClock] to use for time.
    /// * `cancellation_token`: The [CancellationToken] to use for shutdown.
    /// * `state`: The [DbState] to use for error tracking. If provided, the dispatcher
    ///   will set [DbState::error] when an error is encountered.
    pub(crate) fn new(
        handler: Box<dyn MessageHandler<T>>,
        clock: Arc<dyn SystemClock>,
        cancellation_token: CancellationToken,
        error_state: WatchableOnceCell<SlateDBError>,
    ) -> Self {
        let (_tx, rx) = mpsc::unbounded_channel();
        Self::new_with_channel(handler, rx, clock, cancellation_token, error_state)
    }

    /// Creates a new [MessageDispatcher] with a channel. This is the primary way to
    /// create a dispatcher. Messages sent to the channel are passed to the
    /// [MessageHandler] for processing.
    ///
    /// ## Arguments
    ///
    /// * `handler`: The [MessageHandler] to use for processing messages.
    /// * `rx`: The [mpsc::UnboundedReceiver<T>] to use for receiving messages.
    /// * `clock`: The [SystemClock] to use for time.
    /// * `cancellation_token`: The [CancellationToken] to use for shutdown.
    /// * `state`: The [DbState] to use for error tracking. If provided, the dispatcher
    ///   will set [DbState::error] when an error is encountered.
    pub(crate) fn new_with_channel(
        handler: Box<dyn MessageHandler<T>>,
        rx: mpsc::UnboundedReceiver<T>,
        clock: Arc<dyn SystemClock>,
        cancellation_token: CancellationToken,
        error_state: WatchableOnceCell<SlateDBError>,
    ) -> Self {
        Self {
            handler,
            rx,
            clock,
            cancellation_token,
            error_state,
        }
    }

    /// Runs the dispatcher. This is the primary entry point for running the dispatcher.
    ///
    /// [MessageDispatcher::run] is the primary entry point for running the dispatcher;
    /// it is responsible for:
    ///
    /// 1. Running the main event loop ([MessageDispatcher::run_loop]).
    /// 2. Processing the final result when the event loop exits
    ///   ([MessageDispatcher::handle_result]).
    /// 4. Cleaning up resources and processing any remaining messages
    ///   ([MessageDispatcher::cleanup]).
    ///
    /// ## Returns
    ///
    /// A [Result] containing the [DbState::error] if it was already set, or the result
    /// of [MessageDispatcher::handle_result] otherwise.
    pub(crate) async fn run(&mut self) -> Result<(), SlateDBError> {
        // TODO: handle panic here (similar to spawn_bg_task)
        let result = self.run_loop().await;
        let result = self.handle_result(result);
        if let Err(e) = self.cleanup(result.clone()).await {
            warn!("failed to cleanup dispatcher on shutdown [error={:?}]", e);
        }
        result
    }

    /// Runs the main event loop for the dispatcher. This is where messages and ticker
    /// events are processed.
    ///
    /// [MessageDispatcher::run_loop] contains a message loop with the following control
    /// flow:
    ///
    /// 1. Break immediately if we're in an error state or cancelled, and return
    ///    [SlateDBError::BackgroundTaskShutdown].
    /// 2. Else, if there is a message, read it and invoke [MessageHandler::handle].
    /// 3. Else, if there is a ticker event, read it and invoke [MessageHandler::handle].
    ///
    /// If there is an uncaught error at any point, the error is returned immediately (in
    /// lieu of [SlateDBError::BackgroundTaskShutdown] on a clean shutdown).
    ///
    /// ## Returns
    ///
    /// A [Result] containing [SlateDBError::BackgroundTaskShutdown] on clean shutdown,
    /// or an uncaught error.
    async fn run_loop(&mut self) -> Result<(), SlateDBError> {
        let cancellation_token = self.cancellation_token.clone();
        let mut error_watcher = self.error_state.reader();
        let mut tickers = self
            .handler
            .tickers()
            .into_iter()
            .map(|(dur, factory)| MessageDispatcherTicker::new(self.clock.ticker(dur), factory))
            .collect::<Vec<_>>();
        // stop if we're in an error state or cancelled
        let mut check_shutdown = async move || {
            tokio::select! {
                _ = cancellation_token.cancelled() => {},
                _ = error_watcher.await_value() => {},
            }
        };
        loop {
            let ticker_futures = tickers.iter_mut().map(|t| t.tick()).collect::<Vec<_>>();
            let tickers_not_empty = !ticker_futures.is_empty();
            tokio::select! {
                biased;
                // stop the loop if we're in an error state or cancelled
                _ = check_shutdown() => {
                    break;
                }
                // if no errors, prioritize messages
                Some(message) = self.rx.recv() => {
                    self.handler.handle(message).await?;
                },
                // if no messages, check tickers (select_all barfs if ticker_futures is empty, so check)
                (message, _, _) = async { select_all(ticker_futures).await }, if tickers_not_empty => {
                    self.handler.handle(message?).await?;
                },
            }
        }
        Err(SlateDBError::BackgroundTaskShutdown)
    }

    /// Handles the result of [MessageDispatcher::run_loop] using the following logic:
    ///
    /// 1. If [DbState::error] is set, return it.
    /// 2. Else if `result` is an error, set [DbState::error] and return it.
    /// 3. Else, return `result`.
    ///
    /// ## Arguments
    ///
    /// * `result`: The result of [MessageDispatcher::run_loop].
    ///
    /// ## Returns
    ///
    /// A [Result] containing the [DbState::error] if it was already set, or the result
    /// of [MessageDispatcher::handle_result] otherwise.
    fn handle_result(&self, result: Result<(), SlateDBError>) -> Result<(), SlateDBError> {
        // if we failed, notify state (won't overwrite if state is already failed)
        if let Err(ref e) = result {
            self.error_state.write(e.clone());
        }
        // use the first error that occurred (could be ours, or a previous failure)
        self.error_state.reader().read().map_or(result, Err)
    }

    /// Tells the handler to clean up any resources.
    ///
    /// If cleanup fails, the error is returned.
    ///
    /// ## Arguments
    ///
    /// * `maybe_error`: An optional error to pass to the handler. Setting this argument
    ///   signals to the [MessageHandler] that the database is in an error state during
    ///   shutdown. An option is used instead of `Result`` because we convert
    ///   [SlateDBError::BackgroundTaskShutdown] to None, so handlers handle messages
    ///   cleanly during shutdown when the database is not in an error state.
    ///
    /// ## Returns
    ///
    /// The [Result] after cleaning up resources.
    async fn cleanup(&mut self, result: Result<(), SlateDBError>) -> Result<(), SlateDBError> {
        self.rx.close();
        let messages = futures::stream::unfold(&mut self.rx, |rx| async move {
            rx.recv().await.map(|message| (message, rx))
        });
        self.handler.cleanup(Box::pin(messages), result).await
    }
}

/// A ticker that generates messages at regular intervals.
///
/// On each [MessageDispatcherTicker::tick], the message factory is called to generate a
/// message, and the message is returned as a [Future].
pub(crate) struct MessageDispatcherTicker<'a, T: Send> {
    inner: SystemClockTicker<'a>,
    message_factory: Box<MessageFactory<T>>,
}

impl<'a, T: Send> MessageDispatcherTicker<'a, T> {
    /// Creates a new [MessageDispatcherTicker].
    ///
    /// ## Arguments
    ///
    /// * `inner`: The [SystemClockTicker] to use.
    /// * `message_factory`: A factory for generating messages.
    ///
    /// ## Returns
    ///
    /// The new [MessageDispatcherTicker].
    pub(crate) fn new(
        inner: SystemClockTicker<'a>,
        message_factory: Box<MessageFactory<T>>,
    ) -> Self {
        Self {
            inner,
            message_factory,
        }
    }

    /// Returns a [Future] that resolves when the ticker ticks, and returns the message
    /// generated by the message factory.
    ///
    /// ## Returns
    ///
    /// A [Future] that resolves when the ticker ticks.
    pub(crate) fn tick(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<T, SlateDBError>> + Send + '_>> {
        let message = (self.message_factory)();
        Box::pin(async move {
            self.inner.tick().await;
            Ok(message)
        })
    }
}

/// [MessageDispatcher] event loop callbacks are implemented in a [MessageHandler].
/// Handlers are responsible for:
///
/// 1. Processing messages ([MessageHandler::handle])
/// 2. Defining ticker schedules (when to tick a certain message) ([MessageHandler::tickers])
/// 3. Cleaning up resources ([MessageHandler::cleanup])
///
/// It is safe to return errors on failure; the [MessageDispatcher] will handle failures
/// appropriately.
#[async_trait]
pub(crate) trait MessageHandler<T: Send>: Send {
    /// Defines message ticker schedules. [MessageDispatcher::run_loop] instantiates a
    /// [MessageDispatcherTicker] for each ticker defined here. Whenever each ticker
    /// ticks, the message factory generates a message, and [MessageDispatcher] sends the
    /// message to this [MessageHandler].
    ///
    /// ## Returns
    ///
    /// A vector of tuples continaing the duration when a message should be sent to the
    /// [MessageDispatcher], and a message factory to generate a new message on each tick.
    fn tickers(&mut self) -> Vec<(Duration, Box<MessageFactory<T>>)> {
        vec![]
    }

    /// Handles a message. Messages can come from either a channel or a ticker. See
    /// [crate::dispatcher] for details.
    ///
    /// ## Arguments
    ///
    /// * `message`: The message to handle.
    /// * `error`: An optional error to pass to the handler. If set, this argument
    ///   signals to the [MessageHandler] that the database is in an error state during
    ///   shutdown.
    ///
    /// ## Returns
    ///
    /// The [Result] after handling the message.
    async fn handle(&mut self, message: T) -> Result<(), SlateDBError>;

    /// Cleans up resources.
    ///
    /// ## Arguments
    ///
    /// * `messages`: An iterator of messages still in the channel after
    ///   [MessageDispatcher::run_loop] returns.
    /// * `error`: An optional error to pass to the handler. If set, this argument
    ///   signals to the [MessageHandler] that the database is in an error state during
    ///   shutdown.
    ///
    /// ## Returns
    ///
    /// The [Result] after cleaning up resources.
    async fn cleanup(
        &mut self,
        messages: BoxStream<'async_trait, T>,
        result: Result<(), SlateDBError>,
    ) -> Result<(), SlateDBError>;
}

#[cfg(test)]
mod test {
    use super::{MessageDispatcher, MessageHandler};
    use crate::clock::DefaultSystemClock;
    use crate::error::SlateDBError;
    use crate::utils::WatchableOnceCell;
    use futures::StreamExt;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum TestMessage {
        Channel(i32),
        Tick,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Phase {
        Pre,
        Cleanup,
    }

    #[derive(Clone)]
    struct TestHandler {
        log: Arc<Mutex<Vec<(Phase, TestMessage)>>>,
        cleanup_called: WatchableOnceCell<()>,
        fail_on: Option<TestMessage>,
        ticker_interval: Duration,
    }

    impl TestHandler {
        fn new(
            log: Arc<Mutex<Vec<(Phase, TestMessage)>>>,
            cleanup_called: WatchableOnceCell<()>,
        ) -> Self {
            Self {
                log,
                cleanup_called,
                fail_on: None,
                ticker_interval: Duration::from_millis(10),
            }
        }

        fn with_fail_on(mut self, msg: TestMessage) -> Self {
            self.fail_on = Some(msg);
            self
        }

        fn with_ticker_interval(mut self, d: Duration) -> Self {
            self.ticker_interval = d;
            self
        }
    }

    #[async_trait::async_trait]
    impl MessageHandler<TestMessage> for TestHandler {
        fn tickers(
            &mut self,
        ) -> Vec<(
            Duration,
            Box<crate::dispatcher::MessageFactory<TestMessage>>,
        )> {
            vec![(self.ticker_interval, Box::new(|| TestMessage::Tick))]
        }

        async fn handle(&mut self, message: TestMessage) -> Result<(), SlateDBError> {
            if let Some(ref target) = self.fail_on {
                if target == &message {
                    return Err(SlateDBError::Fenced);
                }
            }
            self.log.lock().unwrap().push((Phase::Pre, message));
            Ok(())
        }

        async fn cleanup(
            &mut self,
            mut messages: futures::stream::BoxStream<'async_trait, TestMessage>,
            result: Result<(), SlateDBError>,
        ) -> Result<(), SlateDBError> {
            // Signal that cleanup has started so tests can synchronize safely
            self.cleanup_called.write(());

            match &result {
                Ok(()) | Err(SlateDBError::BackgroundTaskShutdown) => {
                    while let Some(m) = messages.next().await {
                        self.log.lock().unwrap().push((Phase::Cleanup, m));
                    }
                }
                _ => {
                    // Skip draining on unclean shutdown
                }
            }
            result
        }
    }

    fn extract_channel_messages(log: &[(Phase, TestMessage)], phase: Phase) -> Vec<i32> {
        log.iter()
            .filter_map(|(p, m)| match (p, m) {
                (p2, TestMessage::Channel(v)) if *p2 == phase => Some(*v),
                _ => None,
            })
            .collect()
    }

    fn contains_tick(log: &[(Phase, TestMessage)]) -> bool {
        log.iter().any(|(_, m)| matches!(m, TestMessage::Tick))
    }

    #[tokio::test]
    async fn test_dispatcher_run_happy_path() {
        let log = Arc::new(Mutex::new(Vec::<(Phase, TestMessage)>::new()));
        let cleanup_called = WatchableOnceCell::new();
        let mut cleanup_reader = cleanup_called.reader();

        let (tx, rx) = mpsc::unbounded_channel();
        // Pre-enqueue two channel messages so they are handled before the first tick
        tx.send(TestMessage::Channel(10)).unwrap();
        tx.send(TestMessage::Channel(20)).unwrap();
        let handler = TestHandler::new(log.clone(), cleanup_called.clone())
            // Keep ticker reasonably quick so at least one tick occurs
            .with_ticker_interval(Duration::from_millis(5));
        let clock = Arc::new(DefaultSystemClock::new());
        let cancellation_token = CancellationToken::new();
        let error_state = WatchableOnceCell::new();
        let mut dispatcher = MessageDispatcher::new_with_channel(
            Box::new(handler),
            rx,
            clock,
            cancellation_token.clone(),
            error_state.clone(),
        );
        let join = tokio::spawn(async move { dispatcher.run().await });

        // Wait until the first two pre-phase channel messages are logged
        let _ = timeout(Duration::from_secs(30), async {
            loop {
                let snapshot = log.lock().unwrap().clone();
                let pre_channels = extract_channel_messages(&snapshot, Phase::Pre);
                if pre_channels.len() >= 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed out waiting for pre-phase messages");

        let snapshot = log.lock().unwrap().clone();
        let pre_channels = extract_channel_messages(&snapshot, Phase::Pre);
        assert_eq!(pre_channels[0], 10);
        assert_eq!(pre_channels[1], 20);

        // Wait for at least one tick to be handled
        let _ = timeout(Duration::from_secs(3), async {
            loop {
                if contains_tick(&log.lock().unwrap()) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed out waiting for a tick");

        // Cancel and wait for cleanup to start
        cancellation_token.cancel();
        cleanup_reader.await_value().await;

        // Ensure run() completes and returns clean shutdown
        let result = timeout(Duration::from_secs(3), join)
            .await
            .expect("dispatcher did not stop in time")
            .expect("join failed");
        assert!(matches!(result, Err(SlateDBError::BackgroundTaskShutdown)));

        // Error state should be set on shutdown
        assert!(matches!(
            error_state.reader().read(),
            Some(SlateDBError::BackgroundTaskShutdown)
        ));
    }

    #[tokio::test]
    async fn test_dispatcher_propagates_handler_error_and_skips_drain() {
        let log = Arc::new(Mutex::new(Vec::<(Phase, TestMessage)>::new()));
        let cleanup_called = WatchableOnceCell::new();
        let mut cleanup_reader = cleanup_called.reader();

        let (tx, rx) = mpsc::unbounded_channel();
        let handler = TestHandler::new(log.clone(), cleanup_called.clone())
            .with_fail_on(TestMessage::Channel(42))
            .with_ticker_interval(Duration::from_millis(50));
        let clock = Arc::new(DefaultSystemClock::new());
        let cancellation_token = CancellationToken::new();
        let error_state = WatchableOnceCell::new();

        let mut dispatcher = MessageDispatcher::new_with_channel(
            Box::new(handler),
            rx,
            clock,
            cancellation_token.clone(),
            error_state.clone(),
        );
        let join = tokio::spawn(async move { dispatcher.run().await });

        // Trigger handler error
        tx.send(TestMessage::Channel(42)).unwrap();
        // Extra message that would be drained on clean shutdown, but should be skipped here
        tx.send(TestMessage::Channel(77)).unwrap();

        // Wait for cleanup to start (unclean shutdown)
        cleanup_reader.await_value().await;

        let result = timeout(Duration::from_secs(3), join)
            .await
            .expect("dispatcher did not stop in time")
            .expect("join failed");
        assert!(matches!(result, Err(SlateDBError::Fenced)));
        assert!(matches!(
            error_state.reader().read(),
            Some(SlateDBError::Fenced)
        ));

        // On unclean shutdown, cleanup should not drain channel messages
        let cleanup_channels = extract_channel_messages(&log.lock().unwrap(), Phase::Cleanup);
        assert!(cleanup_channels.is_empty());
    }

    #[tokio::test]
    async fn test_dispatcher_prefers_preexisting_error_state() {
        let log = Arc::new(Mutex::new(Vec::<(Phase, TestMessage)>::new()));
        let cleanup_called = WatchableOnceCell::new();
        let mut cleanup_reader = cleanup_called.reader();

        let (_tx, rx) = mpsc::unbounded_channel::<TestMessage>();
        let handler = TestHandler::new(log.clone(), cleanup_called.clone())
            .with_ticker_interval(Duration::from_millis(50));
        let clock = Arc::new(DefaultSystemClock::new());
        let cancellation_token = CancellationToken::new();
        let error_state = WatchableOnceCell::new();
        // Pre-set error state to simulate prior failure
        error_state.write(SlateDBError::Fenced);

        let mut dispatcher = MessageDispatcher::new_with_channel(
            Box::new(handler),
            rx,
            clock,
            cancellation_token.clone(),
            error_state.clone(),
        );
        let join = tokio::spawn(async move { dispatcher.run().await });

        // Cleanup should start promptly because run_loop exits immediately on existing error
        cleanup_reader.await_value().await;

        let result = timeout(Duration::from_secs(3), join)
            .await
            .expect("dispatcher did not stop in time")
            .expect("join failed");
        assert!(matches!(result, Err(SlateDBError::Fenced)));
        assert!(matches!(
            error_state.reader().read(),
            Some(SlateDBError::Fenced)
        ));
    }

    #[tokio::test]
    async fn test_dispatcher_prioritizes_messages_over_tickers() {
        let log = Arc::new(Mutex::new(Vec::<(Phase, TestMessage)>::new()));
        let cleanup_called = WatchableOnceCell::new();

        let (tx, rx) = mpsc::unbounded_channel();
        // Pre-enqueue a channel message that races with the immediate first tick
        // The dispatcher should handle the message first due to select! bias
        tx.send(TestMessage::Channel(99)).unwrap();

        let handler = TestHandler::new(log.clone(), cleanup_called.clone())
            .with_ticker_interval(Duration::from_millis(5));
        let clock = Arc::new(DefaultSystemClock::new());
        let cancellation_token = CancellationToken::new();
        let error_state = WatchableOnceCell::new();

        let mut dispatcher = MessageDispatcher::new_with_channel(
            Box::new(handler),
            rx,
            clock,
            cancellation_token.clone(),
            error_state.clone(),
        );

        let join = tokio::spawn(async move { dispatcher.run().await });

        // Wait for the first pre-phase entry
        let _ = timeout(Duration::from_secs(3), async {
            loop {
                let snapshot = log.lock().unwrap().clone();
                if !snapshot.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed out waiting for first log entry");

        // Ensure a tick eventually appears
        let _ = timeout(Duration::from_secs(3), async {
            loop {
                if contains_tick(&log.lock().unwrap()) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed out waiting for a tick");

        // Verify first handled item was the channel message (not a tick)
        let first = log.lock().unwrap().first().cloned().unwrap();
        assert_eq!(first, (Phase::Pre, TestMessage::Channel(99)));

        // Shutdown cleanly
        cancellation_token.cancel();
        let result = timeout(Duration::from_secs(3), join)
            .await
            .expect("dispatcher did not stop in time")
            .expect("join failed");
        assert!(matches!(result, Err(SlateDBError::BackgroundTaskShutdown)));
        assert!(matches!(
            error_state.reader().read(),
            Some(SlateDBError::BackgroundTaskShutdown)
        ));
    }

    #[derive(Clone)]
    struct NoTickerHandler {
        log: Arc<Mutex<Vec<(Phase, TestMessage)>>>,
        cleanup_called: WatchableOnceCell<()>,
    }

    #[async_trait::async_trait]
    impl MessageHandler<TestMessage> for NoTickerHandler {
        fn tickers(
            &mut self,
        ) -> Vec<(
            Duration,
            Box<crate::dispatcher::MessageFactory<TestMessage>>,
        )> {
            vec![]
        }

        async fn handle(&mut self, message: TestMessage) -> Result<(), SlateDBError> {
            self.log.lock().unwrap().push((Phase::Pre, message));
            Ok(())
        }

        async fn cleanup(
            &mut self,
            mut messages: futures::stream::BoxStream<'async_trait, TestMessage>,
            result: Result<(), SlateDBError>,
        ) -> Result<(), SlateDBError> {
            self.cleanup_called.write(());
            match &result {
                Ok(()) | Err(SlateDBError::BackgroundTaskShutdown) => {
                    while let Some(m) = messages.next().await {
                        self.log.lock().unwrap().push((Phase::Cleanup, m));
                    }
                }
                _ => {}
            }
            result
        }
    }

    #[tokio::test]
    async fn test_dispatcher_handles_no_tickers() {
        let log = Arc::new(Mutex::new(Vec::<(Phase, TestMessage)>::new()));
        let cleanup_called = WatchableOnceCell::new();
        let mut cleanup_reader = cleanup_called.reader();

        let (tx, rx) = mpsc::unbounded_channel();
        // Pre-enqueue a couple of messages
        tx.send(TestMessage::Channel(1)).unwrap();
        tx.send(TestMessage::Channel(2)).unwrap();

        let handler = NoTickerHandler {
            log: log.clone(),
            cleanup_called: cleanup_called.clone(),
        };
        let clock = Arc::new(DefaultSystemClock::new());
        let cancellation_token = CancellationToken::new();
        let error_state = WatchableOnceCell::new();

        let mut dispatcher = MessageDispatcher::new_with_channel(
            Box::new(handler),
            rx,
            clock,
            cancellation_token.clone(),
            error_state.clone(),
        );
        let join = tokio::spawn(async move { dispatcher.run().await });

        // Wait until both pre-phase messages are handled
        let _ = timeout(Duration::from_secs(3), async {
            loop {
                let snapshot = log.lock().unwrap().clone();
                let pre_channels = extract_channel_messages(&snapshot, Phase::Pre);
                if pre_channels.len() >= 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed out waiting for pre-phase messages");

        // Shutdown and ensure cleanup runs
        cancellation_token.cancel();
        cleanup_reader.await_value().await;

        let result = timeout(Duration::from_secs(3), join)
            .await
            .expect("dispatcher did not stop in time")
            .expect("join failed");
        assert!(matches!(result, Err(SlateDBError::BackgroundTaskShutdown)));
        assert!(matches!(
            error_state.reader().read(),
            Some(SlateDBError::BackgroundTaskShutdown)
        ));
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum MTMessage {
        TickA,
        TickB,
    }

    #[derive(Clone)]
    struct MultiTickerHandler {
        log: Arc<Mutex<Vec<MTMessage>>>,
        cleanup_called: WatchableOnceCell<()>,
        a: Duration,
        b: Duration,
    }

    #[async_trait::async_trait]
    impl MessageHandler<MTMessage> for MultiTickerHandler {
        fn tickers(
            &mut self,
        ) -> Vec<(Duration, Box<crate::dispatcher::MessageFactory<MTMessage>>)> {
            vec![
                (self.a, Box::new(|| MTMessage::TickA)),
                (self.b, Box::new(|| MTMessage::TickB)),
            ]
        }

        async fn handle(&mut self, message: MTMessage) -> Result<(), SlateDBError> {
            self.log.lock().unwrap().push(message);
            Ok(())
        }

        async fn cleanup(
            &mut self,
            _messages: futures::stream::BoxStream<'async_trait, MTMessage>,
            result: Result<(), SlateDBError>,
        ) -> Result<(), SlateDBError> {
            self.cleanup_called.write(());
            result
        }
    }

    #[tokio::test]
    async fn test_dispatcher_supports_multiple_tickers() {
        let log = Arc::new(Mutex::new(Vec::<MTMessage>::new()));
        let cleanup_called = WatchableOnceCell::new();
        let mut cleanup_reader = cleanup_called.reader();

        let (_tx, rx) = mpsc::unbounded_channel::<MTMessage>();
        let handler = MultiTickerHandler {
            log: log.clone(),
            cleanup_called: cleanup_called.clone(),
            a: Duration::from_millis(5),
            b: Duration::from_millis(7),
        };
        let clock = Arc::new(DefaultSystemClock::new());
        let cancellation_token = CancellationToken::new();
        let error_state = WatchableOnceCell::new();

        let mut dispatcher = MessageDispatcher::new_with_channel(
            Box::new(handler),
            rx,
            clock,
            cancellation_token.clone(),
            error_state.clone(),
        );
        let join = tokio::spawn(async move { dispatcher.run().await });

        // Wait until we observe at least one of each tick
        let _ = timeout(Duration::from_secs(3), async {
            loop {
                let snapshot = log.lock().unwrap().clone();
                let saw_a = snapshot.iter().any(|m| matches!(m, MTMessage::TickA));
                let saw_b = snapshot.iter().any(|m| matches!(m, MTMessage::TickB));
                if saw_a && saw_b {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed out waiting for both tickers to tick");

        // Shutdown and wait for cleanup to start
        cancellation_token.cancel();
        cleanup_reader.await_value().await;

        let result = timeout(Duration::from_secs(3), join)
            .await
            .expect("dispatcher did not stop in time")
            .expect("join failed");
        assert!(matches!(result, Err(SlateDBError::BackgroundTaskShutdown)));
        assert!(matches!(
            error_state.reader().read(),
            Some(SlateDBError::BackgroundTaskShutdown)
        ));
    }
}
