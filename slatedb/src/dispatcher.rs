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
//!         error: Option<SlateDBError>,
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
use futures::{future::select_all, stream::BoxStream, Stream};
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
        let maybe_error = Self::to_option(result.clone());
        if let Err(e) = self.cleanup(maybe_error).await {
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
                    self.handler.handle(message, None).await?;
                },
                // if no messages, check tickers (select_all barfs if ticker_futures is empty, so check)
                (message, _, _) = async { select_all(ticker_futures).await }, if tickers_not_empty => {
                    self.handler.handle(message?, None).await?;
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
    async fn cleanup(&mut self, maybe_error: Option<SlateDBError>) -> Result<(), SlateDBError> {
        let messages = futures::stream::unfold(&mut self.rx, |rx| async move {
            match rx.recv().await {
                Some(message) => Some((message, rx)),
                None => None,
            }
        });
        self.handler.cleanup(Box::pin(messages), maybe_error).await
    }

    /// Converts a [Result] to an [Option].
    ///
    /// If the result is [Ok] or [SlateDBError::BackgroundTaskShutdown], returns [None].
    /// Otherwise, returns [Some(e)] where `e` is the error.
    fn to_option(result: Result<(), SlateDBError>) -> Option<SlateDBError> {
        match result {
            Ok(_) | Err(SlateDBError::BackgroundTaskShutdown) => None,
            Err(e) => Some(e),
        }
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
    async fn handle(&mut self, message: T, error: Option<SlateDBError>)
        -> Result<(), SlateDBError>;

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
        error: Option<SlateDBError>,
    ) -> Result<(), SlateDBError>;
}
