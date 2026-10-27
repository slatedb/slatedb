use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpResponseBody, HttpService,
    ReqwestConnector,
};
use object_store::ClientOptions;
use parking_lot::Mutex;
use tracing::{Instrument, Span};

use crate::read_trace::{read_parent, ReadDependencyRecord, ReadSpanRecord};

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum HttpMethod {
    Get,
    Head,
    Put,
    Delete,
    Post,
    Patch,
    Options,
    Other,
}

impl HttpMethod {
    fn from_method(method: &str) -> Self {
        match method {
            "GET" => Self::Get,
            "HEAD" => Self::Head,
            "PUT" => Self::Put,
            "DELETE" => Self::Delete,
            "POST" => Self::Post,
            "PATCH" => Self::Patch,
            "OPTIONS" => Self::Options,
            _ => Self::Other,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum HttpCallOutcome {
    Complete,
    TransportError,
    BodyError,
    Cancelled,
    BodyDropped,
}

/// Records contain no URLs, headers, error messages or body content.
/// Times use the capture's native monotonic epoch. Span IDs belong to its subscriber.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct HttpCallRecord {
    pub call: u64,
    pub method: HttpMethod,
    pub status: Option<u16>,
    pub start_us: u64,
    pub headers_us: Option<u64>,
    pub finish_us: u64,
    pub consumed_bytes: u64,
    pub outcome: HttpCallOutcome,
    pub span_id: Option<u64>,
    pub parent_span_id: Option<u64>,
    /// The nearest retained read ancestor in this capture, not a subscriber ID.
    pub read_span: Option<u64>,
}

/// A count is available only when every started call is retained and terminal.
#[derive(Clone, Debug, uniffi::Record)]
pub struct HttpTraceSnapshot {
    pub records: Vec<HttpCallRecord>,
    pub dropped_calls: u64,
    pub open_calls: u64,
    pub completed_call_count: Option<u64>,
    pub read_spans: Vec<ReadSpanRecord>,
    pub open_read_spans: u64,
    pub dropped_read_spans: u64,
    /// Unselected endpoints do not reach the filtered capture layer.
    pub read_dependencies: Vec<ReadDependencyRecord>,
    pub dropped_read_dependencies: u64,
    pub unresolved_read_dependencies: u64,
}

#[derive(Debug, Default)]
struct CaptureState {
    started: u64,
    open: u64,
    dropped: u64,
    records: Vec<HttpCallRecord>,
    read_started: u64,
    read_open: u64,
    read_dropped: u64,
    read_spans: Vec<ReadSpanRecord>,
    dependency_started: u64,
    dependency_dropped: u64,
    dependency_unresolved: u64,
    read_dependencies: Vec<ReadDependencyRecord>,
}

/// Capacity separately bounds calls, read spans and observed read dependencies.
/// Calls and read spans include unfinished reservations.
/// Read spans are exported on final close. Root trace IDs are limited to 128 UTF-8 bytes.
/// Calls include credential requests and can contain client-internal retries or redirects.
/// Byte counts cover response data consumed through the body wrapper.
#[derive(Debug, uniffi::Object)]
pub struct HttpTraceCapture {
    epoch: Instant,
    capacity: u64,
    state: Mutex<CaptureState>,
}

#[uniffi::export]
impl HttpTraceCapture {
    #[uniffi::constructor]
    pub fn new(capacity: u32) -> Arc<Self> {
        Arc::new(Self {
            epoch: Instant::now(),
            capacity: u64::from(capacity),
            state: Mutex::new(CaptureState::default()),
        })
    }

    pub fn snapshot(&self) -> HttpTraceSnapshot {
        let state = self.state.lock();
        HttpTraceSnapshot {
            records: state.records.clone(),
            dropped_calls: state.dropped,
            open_calls: state.open,
            completed_call_count: (state.dropped == 0 && state.open == 0)
                .then_some(state.records.len() as u64),
            read_spans: state.read_spans.clone(),
            open_read_spans: state.read_open,
            dropped_read_spans: state.read_dropped,
            read_dependencies: state.read_dependencies.clone(),
            dropped_read_dependencies: state.dependency_dropped,
            unresolved_read_dependencies: state.dependency_unresolved,
        }
    }
}

impl HttpTraceCapture {
    pub(crate) fn now_us(&self) -> u64 {
        self.epoch
            .elapsed()
            .as_micros()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    pub(crate) fn start_read_span(
        &self,
        mut record: ReadSpanRecord,
        rejected: bool,
    ) -> Option<ReadSpanRecord> {
        let mut state = self.state.lock();
        state.read_started = state.read_started.saturating_add(1);
        state.read_open += 1;
        if rejected || state.read_started > self.capacity {
            state.read_dropped = state.read_dropped.saturating_add(1);
            return None;
        }
        record.span = state.read_started;
        Some(record)
    }

    pub(crate) fn finish_read_span(&self, record: Option<ReadSpanRecord>) {
        let mut state = self.state.lock();
        state.read_open -= 1;
        if let Some(record) = record {
            state.read_spans.push(record);
        }
    }

    pub(crate) fn record_read_dependency(&self, predecessor: Option<u64>, dependent: Option<u64>) {
        let mut state = self.state.lock();
        state.dependency_started = state.dependency_started.saturating_add(1);
        if state.dependency_started > self.capacity {
            state.dependency_dropped = state.dependency_dropped.saturating_add(1);
        } else if let (Some(predecessor), Some(dependent)) = (predecessor, dependent) {
            state.read_dependencies.push(ReadDependencyRecord {
                predecessor,
                dependent,
            });
        } else {
            state.dependency_unresolved = state.dependency_unresolved.saturating_add(1);
        }
    }

    fn start(self: &Arc<Self>, method: HttpMethod) -> CallGuard {
        let read_span = read_parent(self);
        let parent_span_id = Span::current().id().map(|id| id.into_u64());
        let span = tracing::info_span!(
            "slatedb.http",
            http_method = ?method,
            status = tracing::field::Empty,
            consumed_bytes = tracing::field::Empty,
            outcome = tracing::field::Empty,
        );
        let mut state = self.state.lock();
        state.started = state.started.saturating_add(1);
        state.open += 1;
        let record = if state.started <= self.capacity {
            Some(HttpCallRecord {
                call: state.started,
                method,
                status: None,
                start_us: self.now_us(),
                headers_us: None,
                finish_us: 0,
                consumed_bytes: 0,
                outcome: HttpCallOutcome::Cancelled,
                span_id: span.id().map(|id| id.into_u64()),
                parent_span_id,
                read_span,
            })
        } else {
            state.dropped = state.dropped.saturating_add(1);
            None
        };
        CallGuard {
            capture: self.clone(),
            span,
            record,
            status: None,
            bytes: 0,
            finished: false,
        }
    }
}

struct CallGuard {
    capture: Arc<HttpTraceCapture>,
    span: Span,
    record: Option<HttpCallRecord>,
    status: Option<u16>,
    bytes: u64,
    finished: bool,
}

impl CallGuard {
    fn headers(&mut self, status: u16) -> u64 {
        let headers_us = self.capture.now_us();
        self.status = Some(status);
        self.span.record("status", status);
        if let Some(record) = &mut self.record {
            record.status = Some(status);
            record.headers_us = Some(headers_us);
        }
        headers_us
    }

    fn finish(&mut self, outcome: HttpCallOutcome) {
        self.finish_at(outcome, self.capture.now_us());
    }

    fn finish_at(&mut self, outcome: HttpCallOutcome, finish_us: u64) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.span.record("outcome", tracing::field::debug(outcome));
        self.span.record("consumed_bytes", self.bytes);
        let mut state = self.capture.state.lock();
        if let Some(mut record) = self.record.take() {
            record.finish_us = finish_us;
            record.consumed_bytes = self.bytes;
            record.outcome = outcome;
            state.records.push(record);
        }
        state.open -= 1;
    }
}

impl Drop for CallGuard {
    fn drop(&mut self) {
        self.finish(if self.status.is_some() {
            HttpCallOutcome::BodyDropped
        } else {
            HttpCallOutcome::Cancelled
        });
        let span = std::mem::replace(&mut self.span, Span::none());
        if let Some(subscriber) = Span::with_subscriber(&span, |(_, subscriber)| subscriber.clone())
        {
            tracing::dispatcher::with_default(&subscriber, || drop(span));
        }
    }
}

#[derive(Debug)]
pub(crate) struct TraceConnector {
    capture: Arc<HttpTraceCapture>,
}

impl TraceConnector {
    pub(crate) fn new(capture: Arc<HttpTraceCapture>) -> Self {
        Self { capture }
    }
}

impl HttpConnector for TraceConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let inner = ReqwestConnector::default().connect(options)?;
        Ok(HttpClient::new(TraceService {
            inner,
            capture: self.capture.clone(),
        }))
    }
}

#[derive(Debug)]
struct TraceService {
    inner: HttpClient,
    capture: Arc<HttpTraceCapture>,
}

#[async_trait]
impl HttpService for TraceService {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        let mut guard = self
            .capture
            .start(HttpMethod::from_method(request.method().as_str()));
        let response = self
            .inner
            .execute(request)
            .instrument(guard.span.clone())
            .await;
        match response {
            Ok(response) => {
                let ended = response.body().is_end_stream();
                let headers_us = guard.headers(response.status().as_u16());
                Ok(response.map(|inner| {
                    if ended {
                        guard.finish_at(HttpCallOutcome::Complete, headers_us);
                    }
                    HttpResponseBody::new(TraceBody { inner, guard })
                }))
            }
            Err(error) => {
                guard.finish(HttpCallOutcome::TransportError);
                Err(error)
            }
        }
    }
}

struct TraceBody {
    inner: HttpResponseBody,
    guard: CallGuard,
}

impl Body for TraceBody {
    type Data = Bytes;
    type Error = HttpError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, HttpError>>> {
        let _entered = self.guard.span.clone().entered();
        let frame = Pin::new(&mut self.inner).poll_frame(cx);
        match &frame {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    self.guard.bytes = self.guard.bytes.saturating_add(data.len() as u64);
                }
                if self.inner.is_end_stream() {
                    self.guard.finish(HttpCallOutcome::Complete);
                }
            }
            Poll::Ready(Some(Err(_))) => self.guard.finish(HttpCallOutcome::BodyError),
            Poll::Ready(None) => self.guard.finish(HttpCallOutcome::Complete),
            Poll::Pending => {}
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{poll, TryStreamExt};
    use http_body_util::{BodyExt, Empty};
    use object_store::client::{HttpErrorKind, HttpRequestBody};
    use object_store::ObjectStoreExt;
    use std::future::Future;
    use std::io::{Read, Write};
    use tokio::sync::mpsc;
    use tracing::instrument::WithSubscriber;

    #[derive(Debug)]
    enum Reply {
        Response(Mutex<Option<HttpResponse>>),
        Error,
        ErrorAfterPending,
        Pending,
    }

    #[async_trait]
    impl HttpService for Reply {
        async fn call(&self, _: HttpRequest) -> Result<HttpResponse, HttpError> {
            match self {
                Self::Response(response) => Ok(response.lock().take().unwrap()),
                Self::Error => Err(body_error()),
                Self::ErrorAfterPending => {
                    let mut pending = true;
                    std::future::poll_fn(|cx| {
                        if std::mem::replace(&mut pending, false) {
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        } else {
                            Poll::Ready(Err(body_error()))
                        }
                    })
                    .await
                }
                Self::Pending => std::future::pending().await,
            }
        }
    }

    struct ChannelBody(mpsc::UnboundedReceiver<Result<Frame<Bytes>, HttpError>>);

    impl Body for ChannelBody {
        type Data = Bytes;
        type Error = HttpError;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, HttpError>>> {
            self.0.poll_recv(cx)
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::with_exact(3)
        }
    }

    fn body_error() -> HttpError {
        HttpError::new(
            HttpErrorKind::Decode,
            std::io::Error::other("private response text"),
        )
    }

    fn request() -> HttpRequest {
        http::Request::builder()
            .uri("https://private.example/document?secret=token")
            .header("authorization", "private credential")
            .body(HttpRequestBody::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn http_calls_keep_their_live_query_identity() {
        use crate::read_trace::{ReadSpanKind, ReadTraceLayer};
        use tracing_subscriber::layer::SubscriberExt;

        let capture = HttpTraceCapture::new(16);
        let dispatch =
            tracing::Dispatch::new(tracing_subscriber::registry().with(ReadTraceLayer {
                capture: capture.clone(),
            }));
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let run = |trace_id: &'static str| {
            let capture = capture.clone();
            let barrier = barrier.clone();
            async move {
                let root = tracing::info_span!("slatedb.read", trace_id);
                async move {
                    barrier.wait().await;
                    let response = http::Response::new(Bytes::new().into());
                    client(Reply::Response(Mutex::new(Some(response))), &capture)
                        .execute(request())
                        .await
                        .unwrap();
                }
                .instrument(root)
                .await;
            }
            .with_subscriber(dispatch.clone())
        };
        futures::join!(run("query-a"), run("query-b"));
        let snapshot = capture.snapshot();
        assert_eq!(snapshot.completed_call_count, Some(2));
        assert_eq!(snapshot.open_read_spans, 0);
        assert_eq!(snapshot.dropped_read_spans, 0);
        let roots: Vec<_> = snapshot
            .read_spans
            .iter()
            .filter(|span| span.kind == ReadSpanKind::Read)
            .collect();
        assert_eq!(roots.len(), 2);
        assert_ne!(snapshot.records[0].read_span, snapshot.records[1].read_span);
        for call in &snapshot.records {
            let root = roots
                .iter()
                .find(|span| Some(span.span) == call.read_span)
                .unwrap();
            assert!(root.start_us <= call.start_us);
            assert!(call.finish_us <= root.finish_us);
            assert!(matches!(
                root.trace_id.as_deref(),
                Some("query-a" | "query-b")
            ));
        }
    }

    fn client(reply: Reply, capture: &Arc<HttpTraceCapture>) -> HttpClient {
        HttpClient::new(TraceService {
            inner: HttpClient::new(reply),
            capture: capture.clone(),
        })
    }

    fn streamed(
        capture: &Arc<HttpTraceCapture>,
    ) -> (
        HttpClient,
        mpsc::UnboundedSender<Result<Frame<Bytes>, HttpError>>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        let response = http::Response::builder()
            .status(206)
            .body(HttpResponseBody::new(ChannelBody(rx)))
            .unwrap();
        (
            client(Reply::Response(Mutex::new(Some(response))), capture),
            tx,
        )
    }

    #[tokio::test]
    async fn http_calls_close_with_the_query_subscriber() {
        use crate::read_trace::ReadTraceLayer;
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::registry::LookupSpan;

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        for (case, outcome) in [
            ("dropped", HttpCallOutcome::BodyDropped),
            ("cancelled", HttpCallOutcome::Cancelled),
            ("ended", HttpCallOutcome::Complete),
            ("complete", HttpCallOutcome::Complete),
            ("body-error", HttpCallOutcome::BodyError),
            ("transport-error", HttpCallOutcome::TransportError),
        ] {
            let capture = HttpTraceCapture::new(8);
            let dispatch_a =
                tracing::Dispatch::new(tracing_subscriber::registry().with(ReadTraceLayer {
                    capture: capture.clone(),
                }));
            let root = tracing::dispatcher::with_default(&dispatch_a, || {
                tracing::info_span!("slatedb.read", trace_id = "query-owner")
            });
            let root_id = root.id().unwrap();
            let (client, mut tx) = match case {
                "cancelled" => (client(Reply::Pending, &capture), None),
                "transport-error" => (client(Reply::ErrorAfterPending, &capture), None),
                "ended" => {
                    let response = http::Response::builder()
                        .status(204)
                        .body(HttpResponseBody::new(
                            Empty::<Bytes>::new().map_err(|never| match never {}),
                        ))
                        .unwrap();
                    (
                        client(Reply::Response(Mutex::new(Some(response))), &capture),
                        None,
                    )
                }
                _ => {
                    let (client, tx) = streamed(&capture);
                    (client, Some(tx))
                }
            };
            let mut call = Box::pin(client.execute(request()));
            let first = tracing::dispatcher::with_default(&dispatch_a, || {
                root.in_scope(|| call.as_mut().poll(&mut cx))
            });
            let (mut pending, mut response) = match first {
                Poll::Pending => {
                    assert!(matches!(case, "cancelled" | "transport-error"));
                    (Some(call), None)
                }
                Poll::Ready(result) => {
                    tracing::dispatcher::with_default(&dispatch_a, || drop(call));
                    (None, Some(result.unwrap()))
                }
            };
            tracing::dispatcher::with_default(&dispatch_a, || drop(root));
            assert_eq!(capture.snapshot().open_read_spans, 1, "{case}");

            let foreign_capture = HttpTraceCapture::new(8);
            let dispatch_b =
                tracing::Dispatch::new(tracing_subscriber::registry().with(ReadTraceLayer {
                    capture: foreign_capture.clone(),
                }));
            let foreign = tracing::dispatcher::with_default(&dispatch_b, || {
                let root = tracing::info_span!("slatedb.read", trace_id = "foreign-query");
                let child = tracing::info_span!(parent: &root, "slatedb.read.read_index");
                (root, child)
            });
            assert_eq!(Some(root_id.clone()), foreign.0.id(), "{case}");
            let http_id = foreign.1.id().unwrap();
            let registry_a = dispatch_a
                .downcast_ref::<tracing_subscriber::Registry>()
                .unwrap();
            assert_eq!(
                registry_a.span(&http_id).map(|span| span.name()),
                Some("slatedb.http"),
                "{case}"
            );
            assert_eq!(foreign_capture.snapshot().open_read_spans, 2, "{case}");

            if case == "transport-error" {
                let result = tracing::dispatcher::with_default(&dispatch_b, || {
                    pending.as_mut().unwrap().as_mut().poll(&mut cx)
                });
                let Poll::Ready(Err(error)) = result else {
                    panic!("The transport error must finish on its second poll.");
                };
                assert_eq!(error.kind(), HttpErrorKind::Decode);
            } else if matches!(case, "complete" | "body-error") {
                tx.as_ref()
                    .unwrap()
                    .send(Ok(Frame::data(Bytes::from_static(b"abc"))))
                    .unwrap();
                let frame = tracing::dispatcher::with_default(&dispatch_b, || {
                    Pin::new(response.as_mut().unwrap().body_mut()).poll_frame(&mut cx)
                });
                let Poll::Ready(Some(Ok(frame))) = frame else {
                    panic!("The body must return its queued data frame.");
                };
                assert_eq!(frame.into_data().unwrap(), Bytes::from_static(b"abc"));
                if case == "body-error" {
                    tx.as_ref().unwrap().send(Err(body_error())).unwrap();
                } else {
                    drop(tx.take());
                }
                let frame = tracing::dispatcher::with_default(&dispatch_b, || {
                    Pin::new(response.as_mut().unwrap().body_mut()).poll_frame(&mut cx)
                });
                if case == "body-error" {
                    let Poll::Ready(Some(Err(error))) = frame else {
                        panic!("The body must return its queued error.");
                    };
                    assert_eq!(error.kind(), HttpErrorKind::Decode);
                } else {
                    assert!(matches!(frame, Poll::Ready(None)));
                }
            }
            tracing::dispatcher::with_default(&dispatch_b, || drop((pending, response)));
            let snapshot = capture.snapshot();
            assert_eq!(
                snapshot.open_read_spans, 0,
                "HTTP spans must close with the query subscriber: {case}"
            );
            assert!(registry_a.span(&http_id).is_none(), "{case}");
            assert!(registry_a.span(&root_id).is_none(), "{case}");
            assert_eq!(snapshot.read_spans.len(), 1, "{case}");
            assert_eq!(
                snapshot.read_spans[0].trace_id.as_deref(),
                Some("query-owner")
            );
            assert_eq!(snapshot.dropped_read_spans, 0);
            assert_eq!(snapshot.dropped_read_dependencies, 0);
            assert_eq!(snapshot.unresolved_read_dependencies, 0);
            assert_eq!(snapshot.open_calls, 0);
            assert_eq!(snapshot.dropped_calls, 0);
            assert_eq!(snapshot.completed_call_count, Some(1));
            let record = &snapshot.records[0];
            assert_eq!(record.outcome, outcome, "{case}");
            assert_eq!(record.span_id, Some(http_id.into_u64()));
            assert_eq!(record.parent_span_id, Some(root_id.into_u64()));
            assert_eq!(record.read_span, Some(snapshot.read_spans[0].span));
            assert_eq!(
                record.consumed_bytes,
                if matches!(case, "complete" | "body-error") {
                    3
                } else {
                    0
                }
            );
            assert_eq!(
                record.status,
                match case {
                    "cancelled" | "transport-error" => None,
                    "ended" => Some(204),
                    _ => Some(206),
                }
            );
            assert!(record.start_us <= record.finish_us);
            if let Some(headers) = record.headers_us {
                assert!(record.start_us <= headers && headers <= record.finish_us);
            }
            assert!(record.finish_us <= snapshot.read_spans[0].finish_us);
            let foreign_snapshot = foreign_capture.snapshot();
            assert_eq!(foreign_snapshot.open_read_spans, 2, "{case}");
            assert!(foreign_snapshot.read_spans.is_empty(), "{case}");
            tracing::dispatcher::with_default(&dispatch_b, || drop(foreign));
            assert_eq!(foreign_capture.snapshot().open_read_spans, 0, "{case}");
            assert_eq!(foreign_capture.snapshot().read_spans.len(), 2, "{case}");
        }
    }

    #[tokio::test]
    async fn trace_waits_for_streamed_body_and_preserves_frames() {
        let capture = HttpTraceCapture::new(8);
        let (client, tx) = streamed(&capture);
        let mut response = client.execute(request()).await.unwrap();
        assert_eq!(response.status(), 206);
        assert_eq!(response.body().size_hint().exact(), Some(3));
        assert!(!response.body().is_end_stream());
        assert_eq!(capture.snapshot().open_calls, 1);
        assert!(capture.snapshot().records.is_empty());
        assert!(capture.snapshot().completed_call_count.is_none());
        {
            let mut pending = Box::pin(response.body_mut().frame());
            assert!(poll!(pending.as_mut()).is_pending());
        }
        tx.send(Ok(Frame::data(Bytes::from_static(b"abc"))))
            .unwrap();
        let data = response.body_mut().frame().await.unwrap().unwrap();
        assert_eq!(data.into_data().unwrap(), Bytes::from_static(b"abc"));
        assert!(capture.snapshot().records.is_empty());
        let mut trailers = http::HeaderMap::new();
        trailers.insert("x-test", "value".parse().unwrap());
        tx.send(Ok(Frame::trailers(trailers))).unwrap();
        let frame = response.body_mut().frame().await.unwrap().unwrap();
        assert_eq!(frame.trailers_ref().unwrap()["x-test"], "value");
        drop(tx);
        assert!(response.body_mut().frame().await.is_none());
        drop(response);
        let snapshot = capture.snapshot();
        assert_eq!(snapshot.completed_call_count, Some(1));
        assert_eq!(snapshot.records.len(), 1);
        let record = &snapshot.records[0];
        assert_eq!(record.status, Some(206));
        assert_eq!(record.consumed_bytes, 3);
        assert_eq!(record.outcome, HttpCallOutcome::Complete);
        assert!(record.start_us <= record.headers_us.unwrap());
        assert!(record.headers_us.unwrap() <= record.finish_us);
        let rendered = format!("{snapshot:?}");
        for private in ["private", "token", "credential", "abc", "x-test", "value"] {
            assert!(!rendered.contains(private), "capture contains {private}");
        }
    }

    #[tokio::test]
    async fn trace_records_empty_body_at_headers() {
        let capture = HttpTraceCapture::new(1);
        let response = http::Response::builder()
            .status(204)
            .body(HttpResponseBody::new(
                Empty::<Bytes>::new().map_err(|never| match never {}),
            ))
            .unwrap();
        let client = client(Reply::Response(Mutex::new(Some(response))), &capture);
        let request = http::Request::builder()
            .method("HEAD")
            .body(HttpRequestBody::empty())
            .unwrap();
        let response = client.execute(request).await.unwrap();
        let snapshot = capture.snapshot();
        assert_eq!(snapshot.completed_call_count, Some(1));
        assert_eq!(snapshot.records[0].method, HttpMethod::Head);
        assert_eq!(snapshot.records[0].outcome, HttpCallOutcome::Complete);
        assert_eq!(snapshot.records[0].consumed_bytes, 0);
        assert_eq!(
            snapshot.records[0].headers_us,
            Some(snapshot.records[0].finish_us)
        );
        drop(response);
        assert_eq!(capture.snapshot().records.len(), 1);
    }

    #[tokio::test]
    async fn trace_preserves_transport_and_body_errors() {
        let capture = HttpTraceCapture::new(2);
        let error = client(Reply::Error, &capture)
            .execute(request())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), HttpErrorKind::Decode);
        assert_eq!(
            capture.snapshot().records[0].outcome,
            HttpCallOutcome::TransportError
        );
        let (client, tx) = streamed(&capture);
        let response = client.execute(request()).await.unwrap();
        tx.send(Ok(Frame::data(Bytes::from_static(b"abc"))))
            .unwrap();
        tx.send(Err(body_error())).unwrap();
        let error = response.into_body().bytes().await.unwrap_err();
        assert_eq!(error.kind(), HttpErrorKind::Decode);
        let snapshot = capture.snapshot();
        assert_eq!(snapshot.completed_call_count, Some(2));
        assert_eq!(snapshot.records[1].outcome, HttpCallOutcome::BodyError);
        assert_eq!(snapshot.records[1].consumed_bytes, 3);
    }

    #[tokio::test]
    async fn trace_cancellation_and_body_drop_finish_once() {
        let capture = HttpTraceCapture::new(3);
        let client = client(Reply::Pending, &capture);
        drop(client.execute(request()));
        assert_eq!(capture.snapshot().completed_call_count, Some(0));
        let mut pending = Box::pin(client.execute(request()));
        assert!(poll!(pending.as_mut()).is_pending());
        assert_eq!(capture.snapshot().open_calls, 1);
        drop(pending);
        let (client, tx) = streamed(&capture);
        let response = client.execute(request()).await.unwrap();
        drop(response);
        drop(tx);
        let snapshot = capture.snapshot();
        assert_eq!(snapshot.completed_call_count, Some(2));
        assert_eq!(snapshot.records[0].outcome, HttpCallOutcome::Cancelled);
        assert_eq!(snapshot.records[0].status, None);
        assert_eq!(snapshot.records[1].outcome, HttpCallOutcome::BodyDropped);
    }

    #[tokio::test]
    async fn trace_capacity_includes_open_calls_and_reports_loss() {
        let capture = HttpTraceCapture::new(1);
        let (first, _first_tx) = streamed(&capture);
        let (second, _second_tx) = streamed(&capture);
        let (a, b) = tokio::join!(first.execute(request()), second.execute(request()));
        let snapshot = capture.snapshot();
        assert_eq!(snapshot.open_calls, 2);
        assert_eq!(snapshot.dropped_calls, 1);
        assert!(snapshot.completed_call_count.is_none());
        drop(b);
        assert_eq!(capture.snapshot().open_calls, 1);
        assert!(capture.snapshot().records.is_empty());
        drop(a);
        let snapshot = capture.snapshot();
        assert_eq!(snapshot.open_calls, 0);
        assert_eq!(snapshot.records.len(), 1);
        assert_eq!(snapshot.dropped_calls, 1);
        assert!(snapshot.completed_call_count.is_none());
    }

    #[tokio::test]
    async fn trace_counts_do_not_depend_on_log_filter() {
        for subscriber in [
            tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()),
            tracing::Dispatch::new(
                tracing_subscriber::fmt()
                    .with_max_level(tracing::Level::WARN)
                    .finish(),
            ),
        ] {
            let capture = HttpTraceCapture::new(1);
            let client = client(Reply::Error, &capture);
            client
                .execute(request())
                .with_subscriber(subscriber)
                .await
                .unwrap_err();
            let snapshot = capture.snapshot();
            assert_eq!(snapshot.completed_call_count, Some(1));
            assert_eq!(snapshot.records[0].span_id, None);
            assert_eq!(snapshot.records[0].parent_span_id, None);
        }
    }

    #[tokio::test]
    async fn trace_span_has_initiating_parent_when_enabled() {
        let subscriber = tracing::Dispatch::new(tracing_subscriber::registry());
        async {
            let parent = tracing::info_span!("slatedb.read");
            let parent_id = parent.id().unwrap().into_u64();
            let capture = HttpTraceCapture::new(1);
            let client = client(Reply::Error, &capture);
            client
                .execute(request())
                .instrument(parent)
                .await
                .unwrap_err();
            let snapshot = capture.snapshot();
            assert!(snapshot.records[0].span_id.is_some());
            assert_eq!(snapshot.records[0].parent_span_id, Some(parent_id));
        }
        .with_subscriber(subscriber)
        .await;
    }

    #[tokio::test]
    async fn trace_endpoints_precede_subscriber_work() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use tracing_subscriber::layer::SubscriberExt;
        struct DelayedRecords {
            capture: Arc<HttpTraceCapture>,
            headers: Arc<AtomicU64>,
            finish: Arc<AtomicU64>,
        }
        impl tracing_subscriber::Layer<tracing_subscriber::Registry> for DelayedRecords {
            fn on_record(
                &self,
                _: &tracing::Id,
                values: &tracing::span::Record<'_>,
                _: tracing_subscriber::layer::Context<'_, tracing_subscriber::Registry>,
            ) {
                #[derive(Default)]
                struct Endpoint(Option<&'static str>);
                impl tracing::field::Visit for Endpoint {
                    fn record_debug(
                        &mut self,
                        field: &tracing::field::Field,
                        _: &dyn std::fmt::Debug,
                    ) {
                        self.0 = match field.name() {
                            "status" => Some("headers"),
                            "outcome" => Some("finish"),
                            _ => None,
                        };
                    }
                }
                let mut endpoint = Endpoint::default();
                values.record(&mut endpoint);
                if let Some(name) = endpoint.0 {
                    std::thread::sleep(std::time::Duration::from_millis(2));
                    let time = self.capture.now_us();
                    if name == "headers" {
                        self.headers.store(time, Ordering::Relaxed);
                    } else {
                        self.finish.store(time, Ordering::Relaxed);
                    }
                }
            }
        }
        let capture = HttpTraceCapture::new(1);
        let headers = Arc::new(AtomicU64::new(0));
        let finish = Arc::new(AtomicU64::new(0));
        let subscriber = tracing_subscriber::registry().with(DelayedRecords {
            capture: capture.clone(),
            headers: headers.clone(),
            finish: finish.clone(),
        });
        let response = http::Response::builder()
            .body(HttpResponseBody::new(
                Empty::<Bytes>::new().map_err(|never| match never {}),
            ))
            .unwrap();
        client(Reply::Response(Mutex::new(Some(response))), &capture)
            .execute(request())
            .with_subscriber(subscriber)
            .await
            .unwrap();
        let snapshot = capture.snapshot();
        let record = &snapshot.records[0];
        assert!(record.headers_us.unwrap() < headers.load(Ordering::Relaxed));
        assert!(record.finish_us < finish.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn trace_counts_provider_retries_and_list_pages() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let replies = [
                (503, "retry"),
                (200, "abc"),
                (200, "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>private-token</NextContinuationToken><Contents><Key>one</Key><LastModified>2026-10-15T00:00:00Z</LastModified><ETag>one</ETag><Size>3</Size></Contents></ListBucketResult>"),
                (200, "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>two</Key><LastModified>2026-10-15T00:00:00Z</LastModified><ETag>two</ETag><Size>3</Size></Contents></ListBucketResult>"),
            ];
            let mut requests = Vec::new();
            for (status, body) in replies {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                    .unwrap();
                let mut header = Vec::new();
                while !header.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    stream.read_exact(&mut byte).unwrap();
                    header.push(byte[0]);
                    assert!(header.len() < 16384);
                }
                requests.push(String::from_utf8(header).unwrap());
                write!(stream, "HTTP/1.1 {status} test\r\nContent-Length: {}\r\nConnection: close\r\nLast-Modified: Thu, 15 Oct 2026 00:00:00 GMT\r\nETag: test\r\n\r\n{body}", body.len()).unwrap();
            }
            requests
        });
        let capture = HttpTraceCapture::new(8);
        let builder = crate::ObjectStoreBuilder::new(crate::ObjectStoreType::S3);
        for (key, value) in [
            ("bucket", "test-bucket"),
            ("region", "test-region"),
            ("access_key_id", "test-key"),
            ("secret_access_key", "test-secret"),
            ("allow_http", "true"),
            ("endpoint", endpoint.as_str()),
        ] {
            builder
                .with_config(key.to_owned(), value.to_owned())
                .unwrap();
        }
        builder.with_http_trace(capture.clone()).unwrap();
        let store = builder.build().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let bytes = store
                .inner
                .get(&object_store::path::Path::from("private-key"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert_eq!(bytes, Bytes::from_static(b"abc"));
            let objects: Vec<_> = store.inner.list(None).try_collect().await.unwrap();
            assert_eq!(objects.len(), 2);
            assert_eq!(objects[0].location.as_ref(), "one");
            assert_eq!(objects[1].location.as_ref(), "two");
        })
        .await
        .unwrap();
        let requests = server.join().unwrap();
        assert!(requests[0].starts_with("GET /test-bucket/private-key "));
        assert!(requests[1].starts_with("GET /test-bucket/private-key "));
        assert!(!requests[2].contains("private-token"));
        assert!(requests[3].contains("private-token"));
        let snapshot = capture.snapshot();
        assert_eq!(snapshot.completed_call_count, Some(4));
        let statuses: Vec<_> = snapshot.records.iter().map(|r| r.status).collect();
        assert_eq!(statuses, vec![Some(503), Some(200), Some(200), Some(200)]);
        assert_eq!(snapshot.records[0].outcome, HttpCallOutcome::BodyDropped);
        assert!(snapshot.records[1..]
            .iter()
            .all(|r| r.outcome == HttpCallOutcome::Complete));
        assert!(!format!("{snapshot:?}").contains("private"));
    }

    #[tokio::test]
    async fn trace_discloses_hidden_redirect_attempts() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            loop {
                let (mut stream, _) = tokio::select! {
                    result = listener.accept() => result.unwrap(),
                    _ = &mut stopped => break,
                };
                let mut header = Vec::new();
                while !header.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    stream.read_exact(&mut byte).await.unwrap();
                    header.push(byte[0]);
                    assert!(header.len() < 16384);
                }
                requests.push(String::from_utf8(header).unwrap());
                let response = if requests.len() == 1 {
                    "HTTP/1.1 307 Temporary Redirect\r\nLocation: /redirected\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                } else {
                    "HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\nLast-Modified: Thu, 15 Oct 2026 00:00:00 GMT\r\nETag: test\r\n\r\nabc"
                };
                stream.write_all(response.as_bytes()).await.unwrap();
            }
            requests
        });
        let capture = HttpTraceCapture::new(8);
        let builder = crate::ObjectStoreBuilder::new(crate::ObjectStoreType::S3);
        for (key, value) in [
            ("bucket", "test-bucket"),
            ("region", "test-region"),
            ("access_key_id", "test-key"),
            ("secret_access_key", "test-secret"),
            ("allow_http", "true"),
            ("endpoint", endpoint.as_str()),
        ] {
            builder
                .with_config(key.to_owned(), value.to_owned())
                .unwrap();
        }
        builder.with_http_trace(capture.clone()).unwrap();
        let store = builder.build().unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            store
                .inner
                .get(&object_store::path::Path::from("private-key"))
                .await?
                .bytes()
                .await
        })
        .await;
        let _ = stop.send(());
        let requests = tokio::time::timeout(std::time::Duration::from_secs(10), server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.unwrap().unwrap(), Bytes::from_static(b"abc"));
        assert_eq!(requests.len(), 2);
        assert!(requests[0].starts_with("GET /test-bucket/private-key "));
        assert!(requests[1].starts_with("GET /redirected "));
        let snapshot = capture.snapshot();
        assert_eq!(snapshot.completed_call_count, Some(1));
        assert_eq!(snapshot.records.len(), 1);
        assert_eq!(snapshot.records[0].method, HttpMethod::Get);
        assert_eq!(snapshot.records[0].status, Some(200));
        assert_eq!(snapshot.records[0].consumed_bytes, 3);
        assert_eq!(snapshot.records[0].outcome, HttpCallOutcome::Complete);
        assert!(!format!("{snapshot:?}").contains("private"));
    }
}
