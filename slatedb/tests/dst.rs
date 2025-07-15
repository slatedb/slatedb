use rand::distr::weighted::WeightedIndex;
use rand::distr::Distribution;
use rand::seq::IteratorRandom;
use rand::Rng;
use slatedb::config::CompressionCodec;
use slatedb::config::PutOptions;
use slatedb::config::WriteOptions;
use slatedb::object_store::memory::InMemory;
use slatedb::Db;
use slatedb::DbBuilder;
use slatedb::DbRand;
use slatedb::Settings;
use slatedb::SlateDBError;
use slatedb::WriteBatch;
use std::collections::BTreeMap;
use std::future::Future;
use std::ops::RangeFull;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;
use tracing::error;
use tracing::info;

const MIB_1: usize = 1024 * 1024;
const MIB_500: usize = 500 * MIB_1;
const GIB_5: usize = 5 * MIB_500;

const MAX_KEY_LEN: usize = u16::MAX as usize; // keys are limited to 65_535 bytes
const MAX_VAL_LEN: usize = MIB_1;
const MAX_WRITE_BATCH_SIZE: usize = 1024;

const COMPRESSION_CODECS: [Option<&str>; 5] = [
    Some("snappy"),
    Some("zlib"),
    Some("lz4"),
    Some("zstd"),
    None,
];

// TODO make test_deterministic_simulation have a seed parameter and use proptest
#[tokio::test(start_paused = true, flavor = "current_thread")]
async fn test_deterministic_simulation() -> Result<(), SlateDBError> {
    configure_logger();
    let seed = std::env::var("SLATEDB_DST_SEED")
        .map(|s| s.parse::<u64>().unwrap())
        .unwrap_or_else(|_| rand::random::<u64>());
    let rand = DbRand::new(seed);
    let db = build_db(&rand).await;
    let iterations = rand.rng().random_range(1..5_000_000);
    info!(seed, iterations, "test_deterministic_simulation");
    match Dst::new(db, rand).run_simulation(iterations).await {
        Ok(_) => Ok(()),
        Err(e) => {
            error!(seed, ?e, "test_deterministic_simulation failed");
            Err(e)
        }
    }
}

struct Dst {
    db: Db,
    rand: DbRand,
    state: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Dst {
    fn new(db: Db, rand: DbRand) -> Self {
        Self {
            db,
            rand,
            state: BTreeMap::new(),
        }
    }

    // TODO: should we be using rng_seed (tokio_unstable) for the tokio runtime?
    async fn run_simulation(&mut self, iterations: u32) -> Result<(), SlateDBError> {
        let mut op_count = 0;
        let action_index = WeightedIndex::new([
            1, // write
            1, // get
            1, // scan
            1, // flush
            1, // advance time
        ])
        .unwrap();

        for _ in 0..iterations {
            let action_type = action_index.sample(&mut self.rand.rng());
            info!(op_count, action_type, "run_simulation");
            match action_type {
                0 => self.run_write().await?,
                1 => self.run_get().await?,
                2 => self.run_scan().await?,
                3 => self.run_flush().await?,
                4 => self.advance_time().await,
                // TODO: add DbReader open, close, get, and scan
                // TODO: add DbWriter close and open
                // TODO: add seek
                // TODO: add checkpointing?
                // TODO: add fencing?
                _ => unreachable!(),
            }
            op_count += 1;
        }
        Ok(())
    }

    async fn run_write(&mut self) -> Result<(), SlateDBError> {
        let mut write_batch = WriteBatch::new();
        let write_batch_size = self.rand.rng().random_range(1..MAX_WRITE_BATCH_SIZE);
        let write_option = self.get_write_options().await;
        let put_probability = self.rand.rng().random_range(0.0..1.0);
        debug!(write_batch_size, put_probability, "run_write");
        for _ in 0..write_batch_size {
            let is_put = self.rand.rng().random_bool(put_probability);
            if is_put {
                let key = self.gen_key();
                let val = self.gen_val();
                write_batch.put_with_options(&key, &val, &self.gen_put_options().await);
                self.state.insert(key, val);
            } else {
                let key = self.gen_key();
                write_batch.delete(&key);
                self.state.remove(&key);
            }
        }
        let future = self.db.write_with_options(write_batch, &write_option);
        self.poll_await(future).await?;
        Ok(())
    }

    async fn run_get(&mut self) -> Result<(), SlateDBError> {
        let hit_probability: f64 = self.rand.rng().random_range(0.0..1.0);
        let is_db_hit = self.state.len() > 0 && self.rand.rng().random_bool(hit_probability);
        let key = if is_db_hit {
            self.state.keys().choose(&mut self.rand.rng()).unwrap()
        } else {
            // Still might be in keyspace, but unlikely
            &self.gen_key()
        };
        debug!(hit_probability, is_db_hit, "run_get");
        let future = self.db.get(key);
        let result = self.poll_await(future).await?;
        let expected_val = self.state.get(key);
        let actual_val = result.map(|b| b.to_vec());
        assert_eq!(expected_val, actual_val.as_ref());
        Ok(())
    }

    // TODO: add ScanOption variation
    async fn run_scan(&self) -> Result<(), SlateDBError> {
        if self.state.is_empty() {
            debug!("run_scan (empty)");
            let mut db_iter = self.db.scan::<Vec<u8>, RangeFull>(..).await?;
            assert!(db_iter.next().await?.is_none());
            return Ok(());
        }
        // Only scan non-empty ranges since SlateDB panics otherwise (by design, see #680)
        let start_key_prefix_idx = self.rand.rng().random_range(0..self.state.len());
        let end_key_prefix_idx = self
            .rand
            .rng()
            .random_range(start_key_prefix_idx..self.state.len())
            - start_key_prefix_idx;
        let mut keys = self.state.keys();
        let mut start_key_prefix = keys.nth(start_key_prefix_idx).unwrap().clone();
        let mut end_key_prefix = keys.nth(end_key_prefix_idx).unwrap().clone();
        start_key_prefix.truncate(8);
        end_key_prefix.truncate(8);
        debug!(?start_key_prefix, ?end_key_prefix, "run_scan");
        if start_key_prefix > end_key_prefix {
            let mut db_iter = self.db.scan(start_key_prefix..end_key_prefix).await?;
            assert!(db_iter.next().await?.is_none());
            Ok(())
        } else {
            let future = self
                .db
                .scan(start_key_prefix.clone()..end_key_prefix.clone());
            let mut actual_itr = self.poll_await(future).await?;
            let expected_itr = self.state.range(start_key_prefix..end_key_prefix);
            for (expected_key, expected_val) in expected_itr {
                let actual_key_val = actual_itr
                    .next()
                    .await?
                    .expect("should have more items in scan iterator");
                assert_eq!(expected_key, actual_key_val.key.as_ref());
                assert_eq!(expected_val, actual_key_val.value.as_ref());
            }
            assert!(actual_itr.next().await?.is_none());
            Ok(())
        }
    }

    async fn run_flush(&self) -> Result<(), SlateDBError> {
        debug!("run_flush");
        self.db.flush().await
    }

    async fn advance_time(&self) {
        let sleep_micros = self.rand.rng().random_range(0..10_000_000);
        debug!(sleep_micros, "advance_time");
        // TODO: should use system_clock.advance();
        tokio::time::advance(Duration::from_micros(sleep_micros)).await;
    }

    #[inline]
    fn gen_key(&self) -> Vec<u8> {
        let mut rng = self.rand.rng();
        let key_len = rng.random_range(1..MAX_KEY_LEN);
        let mut bytes = Vec::with_capacity(key_len);
        for _ in 0..key_len {
            bytes.push(rng.random_range(0..255));
        }
        bytes
    }

    #[inline]
    fn gen_val(&self) -> Vec<u8> {
        let mut rng = self.rand.rng();
        let val_len = rng.random_range(1..MAX_VAL_LEN);
        let mut bytes = Vec::with_capacity(val_len);
        for _ in 0..val_len {
            bytes.push(rng.random_range(0..255));
        }
        bytes
    }

    async fn gen_put_options(&self) -> PutOptions {
        // TODO: implement ttl support. we'll need to track the time of the write to
        //       determine the expiration time. This will let us know when the write
        //       should no longer exist.
        // let mut rng = self.rand.rng();
        // let ttl_type = rng.random_range(0..2);
        // PutOptions {
        //     ttl: if ttl_type == 0 {
        //         Ttl::Default
        //     } else if ttl_type == 1 {
        //         Ttl::NoExpiry
        //     } else {
        //         Ttl::ExpireAfter(rng.random_range(0..HOUR_1))
        //     },
        // }
        PutOptions::default()
    }

    async fn get_write_options(&self) -> WriteOptions {
        let mut rng = self.rand.rng();
        WriteOptions {
            await_durable: rng.random_bool(0.5),
        }
    }

    /// Polls a future until it is ready, advancing time if it is not ready.
    async fn poll_await<T>(
        &self,
        future: impl Future<Output = Result<T, SlateDBError>>,
    ) -> Result<T, SlateDBError> {
        use futures::task::noop_waker_ref;
        use std::task::Context;
        use std::task::Poll;

        let mut fut = Box::pin(future);
        let mut cx = Context::from_waker(noop_waker_ref());

        loop {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(res) => {
                    return res;
                }
                Poll::Pending => {
                    self.advance_time().await;
                }
            }
        }
    }
}

/// Builds a DB instance with components that are selected at random.
async fn build_db(rand: &DbRand) -> Db {
    let mut builder = DbBuilder::new("test_db", Arc::new(InMemory::new()));
    builder = builder.with_settings(build_settings(rand).await);
    builder.build().await.unwrap()
}

/// Builds a Settings instance with random values.
async fn build_settings(rand: &DbRand) -> Settings {
    let mut rng = rand.rng();
    let flush_interval = rng.random_range(Duration::from_secs(1)..Duration::from_secs(60));
    let manifest_poll_interval = rng.random_range(Duration::from_secs(1)..Duration::from_secs(60));
    let manifest_update_timeout = rng.random_range(Duration::from_secs(1)..Duration::from_secs(60));
    let min_filter_keys = rng.random_range(100..1000);
    let filter_bits_per_key = rng.random_range(1..20);
    let l0_sst_size_bytes = rng.random_range(MIB_1..MIB_500);
    let l0_max_ssts = rng.random_range(1..100);
    let max_unflushed_bytes = rng.random_range(MIB_1..GIB_5);
    // TODO: implement ttl support
    // let default_ttl = if rng.random_bool(0.5) {
    //     None
    // } else {
    //     Some(rng.random_range(0..HOUR_1))
    // };
    let compression_codec_idx = rng.random_range(0..COMPRESSION_CODECS.len());
    let compression_codec =
        if let Some(compression_codec) = COMPRESSION_CODECS[compression_codec_idx] {
            match CompressionCodec::from_str(compression_codec) {
                Ok(codec) => Some(codec),
                Err(_) => None,
            }
        } else {
            None
        };

    // TODO: Build object store cache options
    // TODO: Build garbage collector options
    // TODO: Build compactor options

    Settings {
        flush_interval: Some(flush_interval),
        manifest_poll_interval,
        manifest_update_timeout,
        min_filter_keys,
        filter_bits_per_key,
        l0_sst_size_bytes,
        l0_max_ssts,
        max_unflushed_bytes,
        // default_ttl,
        compression_codec,
        #[cfg(feature = "wal_disable")]
        wal_enabled: rng.random_bool(0.5),
        ..Default::default()
    }
}

fn configure_logger() {
    use tracing_subscriber::filter::EnvFilter;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::Registry;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let subscriber = Registry::default()
        .with(filter)
        .with(tracing_subscriber::fmt::layer());
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

/*
interesting seeds:

- 4828976414946781144: seems to hang on first write (polling for write to finish)
- 6561056955098952705: range end out of bounds: 3603312325 <= 833739 (now seems to hang, too)

*/
