use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use log::LevelFilter as LogLevelFilter;
use tracing::{Event, Subscriber};
use tracing_log::LogTracer;
use tracing_subscriber::filter::LevelFilter as TracingLevelFilter;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::Layer;

use crate::error::Error;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, uniffi::Enum)]
pub enum LogLevel {
    #[default]
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    fn into_log_level_filter(self) -> LogLevelFilter {
        match self {
            Self::Off => LogLevelFilter::Off,
            Self::Error => LogLevelFilter::Error,
            Self::Warn => LogLevelFilter::Warn,
            Self::Info => LogLevelFilter::Info,
            Self::Debug => LogLevelFilter::Debug,
            Self::Trace => LogLevelFilter::Trace,
        }
    }

    fn into_tracing_level_filter(self) -> TracingLevelFilter {
        match self {
            Self::Off => TracingLevelFilter::OFF,
            Self::Error => TracingLevelFilter::ERROR,
            Self::Warn => TracingLevelFilter::WARN,
            Self::Info => TracingLevelFilter::INFO,
            Self::Debug => TracingLevelFilter::DEBUG,
            Self::Trace => TracingLevelFilter::TRACE,
        }
    }
}

impl From<&tracing::Level> for LogLevel {
    fn from(level: &tracing::Level) -> Self {
        match *level {
            tracing::Level::ERROR => Self::Error,
            tracing::Level::WARN => Self::Warn,
            tracing::Level::INFO => Self::Info,
            tracing::Level::DEBUG => Self::Debug,
            tracing::Level::TRACE => Self::Trace,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, uniffi::Record)]
pub struct LogRecord {
    pub level: LogLevel,
    pub target: String,
    pub message: String,
    pub module_path: Option<String>,
    pub file: Option<String>,
    pub line: Option<u32>,
}

impl LogRecord {
    fn from_event(event: &Event<'_>) -> Self {
        let metadata = event.metadata();
        let mut visitor = EventFieldVisitor::default();
        event.record(&mut visitor);

        Self {
            level: metadata.level().into(),
            target: visitor
                .target
                .unwrap_or_else(|| metadata.target().to_owned()),
            message: visitor.message,
            module_path: visitor
                .module_path
                .or_else(|| metadata.module_path().map(str::to_owned)),
            file: visitor.file.or_else(|| metadata.file().map(str::to_owned)),
            line: visitor.line.or(metadata.line()),
        }
    }
}

#[uniffi::export(with_foreign)]
pub trait LogCallback: Send + Sync {
    fn log(&self, record: LogRecord);
}

fn init_lock() -> &'static Mutex<()> {
    static INIT_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    INIT_LOCK.get_or_init(|| Mutex::new(()))
}

static LOGGING_INITIALIZED: AtomicBool = AtomicBool::new(false);

#[uniffi::export]
pub fn init_logging(level: LogLevel, callback: Option<Arc<dyn LogCallback>>) -> Result<(), Error> {
    if LOGGING_INITIALIZED.load(Ordering::Acquire) {
        return Err(logging_already_initialized_error());
    }

    let _guard = init_lock().lock().expect("lock poisoned");
    if LOGGING_INITIALIZED.load(Ordering::Acquire) {
        return Err(logging_already_initialized_error());
    }

    if tracing::dispatcher::has_been_set() {
        return Err(invalid_logging_error(
            "global tracing subscriber already initialized by another library",
        ));
    }

    install_log_tracer(level)?;
    install_subscriber(level, callback)?;

    LOGGING_INITIALIZED.store(true, Ordering::Release);
    Ok(())
}

fn install_log_tracer(level: LogLevel) -> Result<(), Error> {
    LogTracer::builder()
        .with_max_level(level.into_log_level_filter())
        .init()
        .map_err(|_| invalid_logging_error("global logger already initialized by another library"))
}

fn install_subscriber(
    level: LogLevel,
    callback: Option<Arc<dyn LogCallback>>,
) -> Result<(), Error> {
    let filter = level.into_tracing_level_filter();
    if let Some(callback) = callback {
        let subscriber =
            tracing_subscriber::registry().with(LogCallbackLayer { callback }.with_filter(filter));
        tracing::subscriber::set_global_default(subscriber).map_err(|_| {
            invalid_logging_error(
                "global tracing subscriber already initialized by another library",
            )
        })
    } else {
        let subscriber = tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_max_level(filter)
            .finish();
        tracing::subscriber::set_global_default(subscriber).map_err(|_| {
            invalid_logging_error(
                "global tracing subscriber already initialized by another library",
            )
        })
    }
}

fn logging_already_initialized_error() -> Error {
    invalid_logging_error("logging already initialized")
}

fn invalid_logging_error(message: impl Into<String>) -> Error {
    Error::Invalid {
        message: message.into(),
    }
}

#[derive(Default)]
struct EventFieldVisitor {
    message: String,
    target: Option<String>,
    module_path: Option<String>,
    file: Option<String>,
    line: Option<u32>,
}

impl EventFieldVisitor {
    fn field_name(field: &tracing::field::Field) -> &str {
        field.name()
    }
}

impl tracing::field::Visit for EventFieldVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match Self::field_name(field) {
            "message" => {
                self.message.clear();
                self.message.push_str(value);
            }
            "log.target" => self.target = Some(value.to_owned()),
            "log.module_path" => self.module_path = Some(value.to_owned()),
            "log.file" => self.file = Some(value.to_owned()),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        if Self::field_name(field) == "message" {
            self.message = format!("{value:?}");
        }
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if Self::field_name(field) == "log.line" {
            self.line = u32::try_from(value).ok();
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if Self::field_name(field) == "log.line" {
            self.line = u32::try_from(value).ok();
        }
    }
}

struct LogCallbackLayer {
    callback: Arc<dyn LogCallback>,
}

impl<S> Layer<S> for LogCallbackLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        self.callback.log(LogRecord::from_event(event));
    }
}
