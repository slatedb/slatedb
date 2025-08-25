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
//! # use crate::dispatcher::{MessageDispatcher, MessageHandler};
//! # use crate::error::SlateDBError;
//! # use crate::clock::DefaultSystemClock;
//! # use crate::watchable_once_cell::WatchableOnceCell;
//! # use tokio::sync::mpsc;
//! # use tokio_util::sync::CancellationToken;
//! # use futures::stream::BoxStream;
//! # #[tokio::main]
//! # async fn main() {
//! enum Message {
//!     Say(String),
//! }
//!
//! struct PrintMessageHandler;
//!
//! #[async_trait::async_trait]
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
//! let clock = DefaultSystemClock::default();
//! let cancellation_token = CancellationToken::new();
//! let (tx, rx) = mpsc::unbounded_channel();
//! let error_state = WatchableOnceCell::new(None);
//! let dispatcher = MessageDispatcher::new(
//!     Box::new(PrintMessageHandler),
//!     rx,
//!     clock,
//!     cancellation_token,
//!     error_state,
//! );
//! let join_handle = tokio::spawn(async move { dispatcher.run().await });
//! tx.send(Message::Say("hello".to_string())).await;
//! cancellation_token.cancel();
//! join_handle.await;
//! # }
//! ```

// TODO Remove once we've migrated to MessgaeDispatcher
#![allow(dead_code)]

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use async_trait::async_trait;
use fail_parallel::{fail_point, FailPointRegistry};
use futures::{
    stream::{BoxStream, FuturesUnordered},
    StreamExt,
};
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
/// [MessageDispatcher] receives a [MessageHandler] and [mpsc::UnboundedReceiver<T>].
/// Messages sent to the receiver are passed to the [MessageHandler] for processing.
///
/// [MessageDispatcher::run] is the primary entry point for running the dispatcher;
/// it is responsible for running the main event loop ([MessageDispatcher::run_loop])
/// and handling cleaning up after the event loop exits.
///
/// [MessageDispatcher::run_loop] implements the main event loop for the dispatcher.
/// The function receives messages from the [mpsc::UnboundedReceiver<T>] and from any
/// [MessageDispatcherTicker]s, and passes them to the [MessageHandler] for processing.
/// It also shuts down when the either [crate::db_state::DbState::error] is set or the
/// [CancellationToken] is cancelled. See [MessageDispatcher::run_loop] for more
/// details on its behavior.
///
/// [crate::dispatcher] contains a complete code example.
pub(crate) struct MessageDispatcher<T: Send + std::fmt::Debug> {
    handler: Box<dyn MessageHandler<T>>,
    rx: mpsc::UnboundedReceiver<T>,
    clock: Arc<dyn SystemClock>,
    cancellation_token: CancellationToken,
    error_state: WatchableOnceCell<SlateDBError>,
    fp_registry: Arc<FailPointRegistry>,
}

impl<T: Send + std::fmt::Debug> MessageDispatcher<T> {
    /// Creates a new [MessageDispatcher]. Messages sent to the channel are passed to
    /// the [MessageHandler] for processing.
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
        rx: mpsc::UnboundedReceiver<T>,
        clock: Arc<dyn SystemClock>,
        cancellation_token: CancellationToken,
        error_state: WatchableOnceCell<SlateDBError>,
    ) -> Self {
        Self::new_with_fp_registry(
            handler,
            rx,
            clock,
            cancellation_token,
            error_state,
            Arc::new(FailPointRegistry::new()),
        )
    }

    /// Creates a new [MessageDispatcher]. Messages sent to the channel are passed to
    /// the [MessageHandler] for processing. A fail point registry is provided for
    /// fail points.
    ///
    /// ## Arguments
    ///
    /// * `handler`: The [MessageHandler] to use for processing messages.
    /// * `rx`: The [mpsc::UnboundedReceiver<T>] to use for receiving messages.
    /// * `clock`: The [SystemClock] to use for time.
    /// * `cancellation_token`: The [CancellationToken] to use for shutdown.
    /// * `state`: The [DbState] to use for error tracking. If provided, the dispatcher
    ///   will set [DbState::error] when an error is encountered.
    pub(crate) fn new_with_fp_registry(
        handler: Box<dyn MessageHandler<T>>,
        rx: mpsc::UnboundedReceiver<T>,
        clock: Arc<dyn SystemClock>,
        cancellation_token: CancellationToken,
        error_state: WatchableOnceCell<SlateDBError>,
        fp_registry: Arc<FailPointRegistry>,
    ) -> Self {
        Self {
            handler,
            rx,
            clock,
            cancellation_token,
            error_state,
            fp_registry,
        }
    }

    /// Runs the dispatcher. This is the primary entry point for running the dispatcher.
    ///
    /// [MessageDispatcher::run] is the primary entry point for running the dispatcher;
    /// it is responsible for:
    ///
    /// 1. Running the main event loop ([MessageDispatcher::run_loop]).
    /// 2. Processing the final result when the event loop exits
    ///    ([MessageDispatcher::handle_result]).
    /// 4. Cleaning up resources and processing any remaining messages
    ///    ([MessageDispatcher::cleanup]).
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
                e = error_watcher.await_value() => {
                    warn!("halting message loop because db is in error state [error={:#?}]", e);
                },
            }
        };
        let mut ticker_futures: FuturesUnordered<_> =
            tickers.iter_mut().map(|t| t.tick()).collect();
        loop {
            fail_point!(Arc::clone(&self.fp_registry), "dispatcher-run-loop", |_| {
                Err(SlateDBError::Fenced)
            });
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
                // if no messages, check tickers
                Some((message, ticker)) = ticker_futures.next() => {
                    self.handler.handle(message).await?;
                    ticker_futures.push(ticker.tick());
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
        fail_point!(Arc::clone(&self.fp_registry), "dispatcher-cleanup", |_| {
            Err(SlateDBError::Fenced)
        });
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
    pub(crate) fn tick(&mut self) -> Pin<Box<dyn Future<Output = (T, &mut Self)> + Send + '_>> {
        let message = (self.message_factory)();
        Box::pin(async move {
            self.inner.tick().await;
            (message, self)
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

#[cfg(all(test, feature = "test-util"))]
mod test {
    use super::{MessageDispatcher, MessageHandler};
    use crate::clock::{DefaultSystemClock, MockSystemClock, SystemClock};
    use crate::dispatcher::MessageFactory;
    use crate::error::SlateDBError;
    use crate::utils::WatchableOnceCell;
    use fail_parallel::FailPointRegistry;
    use futures::StreamExt;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::task::yield_now;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum TestMessage {
        Channel(i32),
        Tick(i32),
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Phase {
        Pre,
        Cleanup,
    }

    #[derive(Clone)]
    struct TestHandler {
        log: Arc<Mutex<Vec<(Phase, TestMessage)>>>,
        cleanup_called: WatchableOnceCell<Result<(), SlateDBError>>,
        tickers: Vec<(Duration, u8)>,
        clock: Arc<dyn SystemClock>,
        clock_schedule: VecDeque<Duration>,
    }

    impl TestHandler {
        fn new(
            log: Arc<Mutex<Vec<(Phase, TestMessage)>>>,
            cleanup_called: WatchableOnceCell<Result<(), SlateDBError>>,
            clock: Arc<dyn SystemClock>,
        ) -> Self {
            Self {
                log,
                cleanup_called,
                tickers: vec![],
                clock,
                clock_schedule: VecDeque::new(),
            }
        }

        fn add_ticker(mut self, d: Duration, id: u8) -> Self {
            self.tickers.push((d, id));
            self
        }

        /// Add a clock schedule to the handler. The clock will pop the first duration from the
        /// schedule and advance the clock by that duration after each message is processed.
        fn add_clock_schedule(mut self, ts: u64) -> Self {
            self.clock_schedule.push_back(Duration::from_millis(ts));
            self
        }
    }

    #[async_trait::async_trait]
    impl MessageHandler<TestMessage> for TestHandler {
        fn tickers(&mut self) -> Vec<(Duration, Box<MessageFactory<TestMessage>>)> {
            let mut tickers: Vec<(Duration, Box<MessageFactory<_>>)> = vec![];
            for (interval, id) in self.tickers.iter() {
                let id = *id as i32;
                tickers.push((*interval, Box::new(move || TestMessage::Tick(id))));
            }
            tickers
        }

        async fn handle(&mut self, message: TestMessage) -> Result<(), SlateDBError> {
            self.log.lock().unwrap().push((Phase::Pre, message));
            if let Some(advance_duration) = self.clock_schedule.pop_front() {
                self.clock.advance(advance_duration).await;
            }
            Ok(())
        }

        async fn cleanup(
            &mut self,
            mut messages: futures::stream::BoxStream<'async_trait, TestMessage>,
            result: Result<(), SlateDBError>,
        ) -> Result<(), SlateDBError> {
            self.cleanup_called.write(result);
            while let Some(m) = messages.next().await {
                self.log.lock().unwrap().push((Phase::Cleanup, m));
            }
            Ok(())
        }
    }

    async fn wait_for_message_count(log: Arc<Mutex<Vec<(Phase, TestMessage)>>>, count: usize) {
        timeout(Duration::from_secs(30), async move {
            while log.lock().unwrap().len() < count {
                yield_now().await;
            }
        })
        .await
        .expect("timeout waiting for message count");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_dispatcher_run_happy_path() {
        let log = Arc::new(Mutex::new(Vec::<(Phase, TestMessage)>::new()));
        let cleanup_called = WatchableOnceCell::new();
        let (tx, rx) = mpsc::unbounded_channel();
        let clock = Arc::new(MockSystemClock::new());
        let handler = TestHandler::new(log.clone(), cleanup_called.clone(), clock.clone())
            .add_ticker(Duration::from_millis(5), 1)
            .add_clock_schedule(5); // Advance clock by 5ms after first message
        let cancellation_token = CancellationToken::new();
        let error_state = WatchableOnceCell::new();
        let mut dispatcher = MessageDispatcher::new(
            Box::new(handler),
            rx,
            clock.clone(),
            cancellation_token.clone(),
            error_state.clone(),
        );
        let join = tokio::spawn(async move { dispatcher.run().await });

        // Send a message successfully, then trigger a tick before processing the next message
        tx.send(TestMessage::Channel(10)).unwrap();
        wait_for_message_count(log.clone(), 2).await;
        tx.send(TestMessage::Channel(20)).unwrap();
        wait_for_message_count(log.clone(), 3).await;

        // Cancel and wait for cleanup to start
        cancellation_token.cancel();

        // Ensure run() completes and returns clean shutdown
        let result = timeout(Duration::from_secs(30), join)
            .await
            .expect("dispatcher did not stop in time")
            .expect("join failed");

        // Verify final state
        assert!(matches!(result, Err(SlateDBError::BackgroundTaskShutdown)));
        assert!(matches!(
            error_state.reader().read(),
            Some(SlateDBError::BackgroundTaskShutdown)
        ));
        assert!(matches!(
            cleanup_called
                .reader()
                .read()
                .expect("cleanup result not set"),
            Err(SlateDBError::BackgroundTaskShutdown)
        ));
        let messages = log.lock().unwrap().clone();
        assert_eq!(
            messages,
            vec![
                (Phase::Pre, TestMessage::Channel(10)),
                (Phase::Pre, TestMessage::Tick(1)),
                (Phase::Pre, TestMessage::Channel(20))
            ]
        );
    }

    #[cfg(feature = "test-util")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_dispatcher_propagates_handler_error_drains_messages() {
        let log = Arc::new(Mutex::new(Vec::<(Phase, TestMessage)>::new()));
        let cleanup_called = WatchableOnceCell::new();
        let mut cleanup_reader = cleanup_called.reader();
        let (tx, rx) = mpsc::unbounded_channel();
        let clock = Arc::new(MockSystemClock::new());
        let handler = TestHandler::new(log.clone(), cleanup_called.clone(), clock.clone());
        let cancellation_token = CancellationToken::new();
        let error_state = WatchableOnceCell::new();
        let fp_registry = Arc::new(FailPointRegistry::default());
        let mut dispatcher = MessageDispatcher::new_with_fp_registry(
            Box::new(handler),
            rx,
            clock.clone(),
            cancellation_token.clone(),
            error_state.clone(),
            fp_registry.clone(),
        );
        let join = tokio::spawn(async move { dispatcher.run().await });

        // Stop before cleanup so we can send a message and ensure it is drained
        fail_parallel::cfg(fp_registry.clone(), "dispatcher-cleanup", "pause").unwrap();

        // Send a regular message successfully
        tx.send(TestMessage::Channel(42)).unwrap();
        wait_for_message_count(log.clone(), 1).await;

        // Force a return by setting an error message
        error_state.write(SlateDBError::Fenced);

        // Send another message after error state is set
        tx.send(TestMessage::Channel(77)).unwrap();

        // Now proceed with cleanup
        fail_parallel::cfg(fp_registry.clone(), "dispatcher-cleanup", "off").unwrap();

        // Wait for cleanup to start (unclean shutdown)
        let _ = cleanup_reader.await_value().await;

        let result = timeout(Duration::from_secs(30), join)
            .await
            .expect("dispatcher did not stop in time")
            .expect("join failed");

        // Verify final state
        assert!(matches!(result, Err(SlateDBError::Fenced)));
        assert!(matches!(
            error_state.reader().read(),
            Some(SlateDBError::Fenced)
        ));
        assert!(matches!(
            cleanup_reader.read(),
            Some(Err(SlateDBError::Fenced))
        ));
        assert!(matches!(result, Err(SlateDBError::Fenced)));
        let messages = log.lock().unwrap().clone();
        assert_eq!(
            messages,
            vec![
                (Phase::Pre, TestMessage::Channel(42)),
                (Phase::Cleanup, TestMessage::Channel(77))
            ]
        );
    }

    #[cfg(feature = "test-util")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_dispatcher_prefers_preexisting_error_state() {
        let log = Arc::new(Mutex::new(Vec::<(Phase, TestMessage)>::new()));
        let cleanup_called = WatchableOnceCell::new();
        let mut cleanup_reader = cleanup_called.reader();
        let (_tx, rx) = mpsc::unbounded_channel::<TestMessage>();
        let clock = Arc::new(DefaultSystemClock::new());
        let handler = TestHandler::new(log.clone(), cleanup_called.clone(), clock.clone());
        let cancellation_token = CancellationToken::new();
        let error_state = WatchableOnceCell::new();
        // Pre-set error state to simulate prior failure
        error_state.write(SlateDBError::Fenced);
        let mut dispatcher = MessageDispatcher::new(
            Box::new(handler),
            rx,
            clock,
            cancellation_token.clone(),
            error_state.clone(),
        );
        let join = tokio::spawn(async move { dispatcher.run().await });

        // Cleanup should start promptly because run_loop exits immediately on existing error
        let _ = cleanup_reader.await_value().await;

        let result = timeout(Duration::from_secs(30), join)
            .await
            .expect("dispatcher did not stop in time")
            .expect("join failed");
        assert!(matches!(result, Err(SlateDBError::Fenced)));
        assert!(matches!(
            error_state.reader().read(),
            Some(SlateDBError::Fenced)
        ));
        assert!(matches!(
            cleanup_reader.read(),
            Some(Err(SlateDBError::Fenced))
        ));
        let messages = log.lock().unwrap().clone();
        assert_eq!(messages, vec![]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_dispatcher_prioritizes_messages_over_tickers() {
        let log = Arc::new(Mutex::new(Vec::<(Phase, TestMessage)>::new()));
        let cleanup_called = WatchableOnceCell::new();
        let (tx, rx) = mpsc::unbounded_channel();
        let clock = Arc::new(MockSystemClock::new());
        let handler = TestHandler::new(log.clone(), cleanup_called.clone(), clock.clone())
            .add_ticker(Duration::from_millis(5), 1);
        let cancellation_token = CancellationToken::new();
        let error_state = WatchableOnceCell::new();
        let fp_registry = Arc::new(FailPointRegistry::default());
        let mut dispatcher = MessageDispatcher::new_with_fp_registry(
            Box::new(handler),
            rx,
            clock.clone(),
            cancellation_token.clone(),
            error_state.clone(),
            fp_registry.clone(),
        );
        fail_parallel::cfg(fp_registry.clone(), "dispatcher-run-loop", "pause").unwrap();
        let join = tokio::spawn(async move { dispatcher.run().await });

        // Trigger a tick and a message
        clock.advance(Duration::from_millis(5)).await;
        tx.send(TestMessage::Channel(99)).unwrap();
        fail_parallel::cfg(fp_registry.clone(), "dispatcher-run-loop", "off").unwrap();
        wait_for_message_count(log.clone(), 2).await;

        // Shutdown cleanly
        cancellation_token.cancel();
        let result = timeout(Duration::from_secs(30), join)
            .await
            .expect("dispatcher did not stop in time")
            .expect("join failed");

        // Verify final state
        assert!(matches!(result, Err(SlateDBError::BackgroundTaskShutdown)));
        assert!(matches!(
            error_state.reader().read(),
            Some(SlateDBError::BackgroundTaskShutdown)
        ));
        assert!(matches!(
            cleanup_called.reader().read(),
            Some(Err(SlateDBError::BackgroundTaskShutdown))
        ));
        let messages = log.lock().unwrap().clone();
        assert_eq!(
            messages,
            vec![
                (Phase::Pre, TestMessage::Channel(99)),
                (Phase::Pre, TestMessage::Tick(1)),
            ]
        );
    }

    // This test simulates the following timeline:
    // immediate tick(3) (tickers always return first ticks immediately)
    // immediate tick(5)
    // advance clock to 5ms
    // tick(5)
    // advance clock to 7ms
    // tick(7)
    // advance clock to 10ms
    // tick(5)
    // advance clock to 14ms
    // tick(7)
    // advance clock to 15ms
    // tick(5)
    // advance clock to 20ms
    // tick(5)
    // advance clock to 21ms
    // tick(7)
    #[cfg(feature = "test-util")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_dispatcher_supports_multiple_tickers() {
        let log = Arc::new(Mutex::new(Vec::<(Phase, TestMessage)>::new()));
        let cleanup_called = WatchableOnceCell::new();
        let (_tx, rx) = mpsc::unbounded_channel::<TestMessage>();
        let clock = Arc::new(MockSystemClock::new());
        let handler = TestHandler::new(log.clone(), cleanup_called.clone(), clock.clone())
            .add_ticker(Duration::from_millis(5), 1)
            .add_ticker(Duration::from_millis(7), 2)
            .add_clock_schedule(0) // 0ms (initial two ticks are immediate, so don't advance clock until second tick)
            .add_clock_schedule(5) // 5ms
            .add_clock_schedule(2) // 7ms
            .add_clock_schedule(3) // 10ms
            .add_clock_schedule(4) // 14ms
            .add_clock_schedule(1) // 15ms
            .add_clock_schedule(5) // 20ms
            .add_clock_schedule(1); // 21ms
        let cancellation_token = CancellationToken::new();
        let error_state = WatchableOnceCell::new();
        let mut dispatcher = MessageDispatcher::new(
            Box::new(handler),
            rx,
            clock.clone(),
            cancellation_token.clone(),
            error_state.clone(),
        );
        let join = tokio::spawn(async move { dispatcher.run().await });

        assert_eq!(log.lock().unwrap().clone(), vec![]);
        wait_for_message_count(log.clone(), 9).await;
        assert_eq!(
            log.lock().unwrap().clone(),
            vec![
                (Phase::Pre, TestMessage::Tick(1)), // immediate tick
                (Phase::Pre, TestMessage::Tick(2)), // immediate tick
                (Phase::Pre, TestMessage::Tick(1)), // 5
                (Phase::Pre, TestMessage::Tick(2)), // 7
                (Phase::Pre, TestMessage::Tick(1)), // 10
                (Phase::Pre, TestMessage::Tick(2)), // 14
                (Phase::Pre, TestMessage::Tick(1)), // 15
                (Phase::Pre, TestMessage::Tick(1)), // 20
                (Phase::Pre, TestMessage::Tick(2)), // 21
            ]
        );

        // Shutdown and wait for cleanup to start
        cancellation_token.cancel();
        let _ = cleanup_called.reader().await_value().await;

        let result = timeout(Duration::from_secs(30), join)
            .await
            .expect("dispatcher did not stop in time")
            .expect("join failed");

        // Verify final state
        assert!(matches!(result, Err(SlateDBError::BackgroundTaskShutdown)));
        assert!(matches!(
            error_state.reader().read(),
            Some(SlateDBError::BackgroundTaskShutdown)
        ));
        assert!(matches!(cleanup_called.reader().read(), Some(Err(_))));
        assert_eq!(log.lock().unwrap().len(), 9);
    }

    // This test simulates the following timeline:
    // immediate tick(3) (tickers always return first ticks immediately)
    // immediate tick(5)
    // advance clock to 3ms
    // tick(3)
    // advance clock to 5ms
    // tick(5)
    // advance clock to 6ms
    // tick(3)
    // advance clock to 9ms
    // tick(3)
    // advance clock to 10ms
    // tick(5)
    // advance clock to 12ms
    // tick(3)
    // clock = 15ms: tick(3), tick(5)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_dispatcher_supports_overlapping_tickers() {
        let log = Arc::new(Mutex::new(Vec::<(Phase, TestMessage)>::new()));
        let cleanup_called = WatchableOnceCell::new();
        let (_tx, rx) = mpsc::unbounded_channel::<TestMessage>();
        let clock = Arc::new(MockSystemClock::new());
        let handler = TestHandler::new(log.clone(), cleanup_called.clone(), clock.clone())
            .add_ticker(Duration::from_millis(3), 3)
            .add_ticker(Duration::from_millis(5), 5)
            .add_clock_schedule(0) // 0 (initial two ticks are immediate, so don't advance clock until second tick)
            .add_clock_schedule(3) // 3
            .add_clock_schedule(2) // 5
            .add_clock_schedule(1) // 6
            .add_clock_schedule(3) // 9
            .add_clock_schedule(1) // 10
            .add_clock_schedule(2) // 12
            .add_clock_schedule(3); // 15
        let cancellation_token = CancellationToken::new();
        let error_state = WatchableOnceCell::new();
        let fp_registry = Arc::new(FailPointRegistry::default());
        let mut dispatcher = MessageDispatcher::new_with_fp_registry(
            Box::new(handler),
            rx,
            clock.clone(),
            cancellation_token.clone(),
            error_state.clone(),
            fp_registry.clone(),
        );
        let join = tokio::spawn(async move { dispatcher.run().await });
        assert_eq!(log.lock().unwrap().len(), 0);

        wait_for_message_count(log.clone(), 10).await;
        assert_eq!(
            log.lock().unwrap().clone()[..8],
            vec![
                (Phase::Pre, TestMessage::Tick(3)), // immediate tick
                (Phase::Pre, TestMessage::Tick(5)), // immediate tick
                (Phase::Pre, TestMessage::Tick(3)), // 3
                (Phase::Pre, TestMessage::Tick(5)), // 5
                (Phase::Pre, TestMessage::Tick(3)), // 6
                (Phase::Pre, TestMessage::Tick(3)), // 9
                (Phase::Pre, TestMessage::Tick(5)), // 10
                (Phase::Pre, TestMessage::Tick(3)), // 12
            ]
        );
        // expect two back-to-back ticks at 15
        let mut last_two_ticks = log.lock().unwrap().clone()[8..].to_vec();
        last_two_ticks.sort_by(|a, b| match (a.1.clone(), b.1.clone()) {
            (TestMessage::Tick(a), TestMessage::Tick(b)) => a.cmp(&b),
            _ => panic!("expected ticks"),
        });
        assert_eq!(
            last_two_ticks,
            vec![
                (Phase::Pre, TestMessage::Tick(3)),
                (Phase::Pre, TestMessage::Tick(5))
            ]
        );

        // Shutdown cleanly
        cancellation_token.cancel();
        let _ = cleanup_called.reader().await_value().await;
        let result = timeout(Duration::from_secs(30), join)
            .await
            .expect("dispatcher did not stop in time")
            .expect("join failed");
        assert!(matches!(result, Err(SlateDBError::BackgroundTaskShutdown)));
        assert!(matches!(
            error_state.reader().read(),
            Some(SlateDBError::BackgroundTaskShutdown)
        ));
        assert!(matches!(cleanup_called.reader().read(), Some(Err(_))));
        assert_eq!(log.lock().unwrap().len(), 10);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_dispatcher_supports_identical_tickers() {
        let log = Arc::new(Mutex::new(Vec::<(Phase, TestMessage)>::new()));
        let cleanup_called = WatchableOnceCell::new();
        let (_tx, rx) = mpsc::unbounded_channel::<TestMessage>();
        let clock = Arc::new(MockSystemClock::new());
        let handler = TestHandler::new(log.clone(), cleanup_called.clone(), clock.clone())
            .add_ticker(Duration::from_millis(3), 1)
            .add_ticker(Duration::from_millis(3), 2)
            .add_clock_schedule(0) // 0 (initial two ticks are immediate, so don't advance clock until second tick)
            .add_clock_schedule(3) // 3
            .add_clock_schedule(0) // 3 (process second)
            .add_clock_schedule(3) // 6
            .add_clock_schedule(0) // 6 (process second)
            .add_clock_schedule(3) // 9
            .add_clock_schedule(0) // 9 (process second)
            .add_clock_schedule(3) // 12
            .add_clock_schedule(0) // 12 (process second)
            .add_clock_schedule(3) // 15
            .add_clock_schedule(0) // 15 (process second)
            .add_clock_schedule(3) // 18
            .add_clock_schedule(0) // 18 (process second)
            .add_clock_schedule(3) // 21
            .add_clock_schedule(0); // 21 (process second)
        let cancellation_token = CancellationToken::new();
        let error_state = WatchableOnceCell::new();
        let fp_registry = Arc::new(FailPointRegistry::default());
        let mut dispatcher = MessageDispatcher::new_with_fp_registry(
            Box::new(handler),
            rx,
            clock.clone(),
            cancellation_token.clone(),
            error_state.clone(),
            fp_registry.clone(),
        );
        let join = tokio::spawn(async move { dispatcher.run().await });
        assert_eq!(log.lock().unwrap().len(), 0);

        wait_for_message_count(log.clone(), 16).await;
        assert_eq!(
            log.lock().unwrap().clone(),
            vec![
                (Phase::Pre, TestMessage::Tick(1)), // immediate tick
                (Phase::Pre, TestMessage::Tick(2)), // immediate tick
                (Phase::Pre, TestMessage::Tick(1)), // 3
                (Phase::Pre, TestMessage::Tick(2)), // 3
                (Phase::Pre, TestMessage::Tick(1)), // 6
                (Phase::Pre, TestMessage::Tick(2)), // 6
                (Phase::Pre, TestMessage::Tick(1)), // 9
                (Phase::Pre, TestMessage::Tick(2)), // 9
                (Phase::Pre, TestMessage::Tick(1)), // 12
                (Phase::Pre, TestMessage::Tick(2)), // 12
                (Phase::Pre, TestMessage::Tick(1)), // 15
                (Phase::Pre, TestMessage::Tick(2)), // 15
                (Phase::Pre, TestMessage::Tick(1)), // 18
                (Phase::Pre, TestMessage::Tick(2)), // 18
                (Phase::Pre, TestMessage::Tick(1)), // 21
                (Phase::Pre, TestMessage::Tick(2)), // 21
            ]
        );

        // Shutdown cleanly
        cancellation_token.cancel();
        let _ = cleanup_called.reader().await_value().await;
        let result = timeout(Duration::from_secs(30), join)
            .await
            .expect("dispatcher did not stop in time")
            .expect("join failed");
        assert!(matches!(result, Err(SlateDBError::BackgroundTaskShutdown)));
        assert!(matches!(
            error_state.reader().read(),
            Some(SlateDBError::BackgroundTaskShutdown)
        ));
        assert!(matches!(cleanup_called.reader().read(), Some(Err(_))));
        assert_eq!(log.lock().unwrap().len(), 16);
    }
}
