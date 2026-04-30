use async_trait::async_trait;
use slatedb::Error;

use crate::{Actor, ActorCtx};

/// Decorates an actor by suppressing selected errors from `run`.
pub struct SuppressErrorActor<A, F> {
    inner: A,
    should_suppress: F,
}

impl<A, F> SuppressErrorActor<A, F> {
    pub fn new(inner: A, should_suppress: F) -> Self {
        Self {
            inner,
            should_suppress,
        }
    }
}

#[async_trait]
impl<A, F> Actor for SuppressErrorActor<A, F>
where
    A: Actor,
    F: Fn(&Error) -> bool + Send + 'static,
{
    async fn run(&mut self, ctx: &ActorCtx) -> Result<(), Error> {
        match self.inner.run(ctx).await {
            Err(error) if (self.should_suppress)(&error) => Ok(()),
            result => result,
        }
    }

    async fn finish(&mut self, ctx: &ActorCtx) -> Result<(), Error> {
        self.inner.finish(ctx).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;
    use slatedb::config::Settings;
    use slatedb::{CloseReason, Db, Error, ErrorKind};

    use super::SuppressErrorActor;
    use crate::{Actor, ActorCtx, Harness};

    struct ErrorThenShutdownActor {
        runs: Arc<AtomicUsize>,
        error: fn() -> Error,
    }

    impl ErrorThenShutdownActor {
        fn new(runs: Arc<AtomicUsize>, error: fn() -> Error) -> Self {
            Self { runs, error }
        }
    }

    #[async_trait]
    impl Actor for ErrorThenShutdownActor {
        async fn run(&mut self, ctx: &ActorCtx) -> Result<(), Error> {
            if self.runs.fetch_add(1, Ordering::SeqCst) == 0 {
                Err((self.error)())
            } else {
                ctx.shutdown_token().cancel();
                Ok(())
            }
        }
    }

    fn test_harness<A: Actor>(name: &'static str, actor: A) -> Harness {
        Harness::new(name, 7, move |ctx| async move {
            let db = Db::builder(ctx.path().clone(), ctx.main_object_store())
                .with_system_clock(ctx.system_clock())
                .with_settings(Settings {
                    compactor_options: None,
                    garbage_collector_options: None,
                    ..Settings::default()
                })
                .build()
                .await?;

            Ok(Arc::new(db))
        })
        .actor("test-actor", actor)
    }

    #[test]
    fn should_suppress_matching_run_error() {
        let runs = Arc::new(AtomicUsize::new(0));
        let actor = SuppressErrorActor::new(
            ErrorThenShutdownActor::new(runs.clone(), || {
                Error::closed("fenced".to_string(), CloseReason::Fenced)
            }),
            |error: &Error| matches!(error.kind(), ErrorKind::Closed(CloseReason::Fenced)),
        );

        test_harness("suppress-matching-error", actor)
            .run()
            .expect("matching error should be suppressed");

        assert_eq!(runs.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn should_propagate_non_matching_run_error() {
        let runs = Arc::new(AtomicUsize::new(0));
        let actor = SuppressErrorActor::new(
            ErrorThenShutdownActor::new(runs.clone(), || Error::unavailable("retry".to_string())),
            |error: &Error| matches!(error.kind(), ErrorKind::Closed(CloseReason::Fenced)),
        );

        let error = test_harness("propagate-non-matching-error", actor)
            .run()
            .expect_err("non-matching error should be propagated");

        assert_eq!(error.kind(), ErrorKind::Unavailable);
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }
}
