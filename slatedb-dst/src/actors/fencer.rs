use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use log::info;
use slatedb::{CloseReason, Db, Error, ErrorKind};
use tracing::instrument;

use crate::{Actor, ActorCtx};

type DbFactoryFuture = Pin<Box<dyn Future<Output = Result<Arc<Db>, Error>> + Send + 'static>>;
type DbFactory = Box<dyn Fn(ActorCtx) -> DbFactoryFuture + Send + Sync + 'static>;

/// Wraps actors that may race with a DB replacement and observe the old handle
/// after it has been fenced.
pub struct SuppressFenced<A> {
    inner: A,
}

impl<A> SuppressFenced<A> {
    pub fn new(inner: A) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<A> Actor for SuppressFenced<A>
where
    A: Actor,
{
    async fn run(&mut self, ctx: &ActorCtx) -> Result<(), Error> {
        match self.inner.run(ctx).await {
            Err(error) if matches!(error.kind(), ErrorKind::Closed(CloseReason::Fenced)) => Ok(()),
            result => result,
        }
    }

    async fn finish(&mut self, ctx: &ActorCtx) -> Result<(), Error> {
        self.inner.finish(ctx).await
    }
}

/// Configuration for the database fencing DST actor.
#[derive(Clone, Debug)]
pub struct DbFencerActorOptions {
    /// How long the actor waits between DB replacement attempts.
    pub restart_interval: Duration,
}

/// Reopens the shared database in a loop and verifies each old handle is fenced.
pub struct DbFencerActor {
    actor_options: DbFencerActorOptions,
    db_factory: DbFactory,
}

impl DbFencerActor {
    pub fn new<F, Fut>(actor_options: DbFencerActorOptions, db_factory: F) -> Result<Self, Error>
    where
        F: Fn(ActorCtx) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Arc<Db>, Error>> + Send + 'static,
    {
        info!("db fencer actor created [options={:?}]", actor_options);
        Ok(Self {
            actor_options,
            db_factory: Box::new(move |ctx| Box::pin(db_factory(ctx))),
        })
    }
}

#[async_trait]
impl Actor for DbFencerActor {
    #[instrument(level = "debug", skip_all, fields(name = %ctx.name()))]
    async fn run(&mut self, ctx: &ActorCtx) -> Result<(), Error> {
        // Make a new DB, fencing the old one.
        let Some(next_db) = self.open_replacement_db(ctx).await? else {
            return Ok(());
        };
        let old_db = ctx.swap_db(next_db);

        // Verify the old DB is fenced.
        match old_db.put(b"foo", b"bar").await {
            Err(err) if matches!(err.kind(), ErrorKind::Closed(CloseReason::Fenced)) => {}
            result => panic!("old db was not fenced as expected [result={result:?}]"),
        }
        old_db.close().await?;

        info!("db fencing complete [name={}]", ctx.name());

        // Wait for the restart interval before allowing the next generation to start.
        let shutdown_token = ctx.shutdown_token();
        let system_clock = ctx.system_clock();
        tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => {}
            _ = system_clock.sleep(self.actor_options.restart_interval) => {}
        }

        Ok(())
    }
}

impl DbFencerActor {
    async fn open_replacement_db(&self, ctx: &ActorCtx) -> Result<Option<Arc<Db>>, Error> {
        loop {
            let shutdown_token = ctx.shutdown_token();
            let open_db = (self.db_factory)(ctx.clone());
            let result = tokio::select! {
                biased;
                _ = shutdown_token.cancelled() => return Ok(None),
                result = open_db => result,
            };

            match result {
                Err(err) if matches!(err.kind(), ErrorKind::Unavailable) => {
                    info!(
                        "db replacement open failed with unavailable, retrying [name={}]",
                        ctx.name()
                    );
                }
                result => return result.map(Some),
            }
        }
    }
}
