use rand::Rng;
use rstest::rstest;
use slatedb::DbRand;
use slatedb::SlateDBError;
use slatedb_dst::utils::build_dst;
use std::rc::Rc;
use tracing::error;
use tracing::info;

#[rstest]
#[case(Rc::new(DbRand::new(1)), 10)]
#[case(Rc::new(DbRand::new(2)), 10)]
#[case(Rc::new(DbRand::new(3)), 10)]
#[case(Rc::new(DbRand::new(4)), 10)]
#[case(Rc::new(DbRand::new(5)), 10)]
#[tokio::test(start_paused = true, flavor = "current_thread")]
async fn test_dst_fast(
    #[case] rand: Rc<DbRand>,
    #[case] iterations: u32,
) -> Result<(), SlateDBError> {
    let seed = rand.seed();
    info!("running simulation with seed {}", seed);
    let mut dst = build_dst(rand.clone()).await;
    match dst.run_simulation(iterations).await {
        Ok(_) => Ok(()),
        Err(e) => {
            error!("simulation failed with seed {}: {}", seed, e);
            Err(e)
        }
    }
}

#[tokio::test(start_paused = true, flavor = "current_thread")]
#[rstest]
#[case(1, 10, 100)]
async fn test_slatedb_is_deterministic(
    #[case] seed: u64,
    #[case] simulations: u32,
    #[case] iterations: u32,
) -> Result<(), SlateDBError> {
    let mut expected_u64: Option<u64> = None;
    for _i in 0..simulations {
        let rand = Rc::new(DbRand::new(seed));
        let mut dst = build_dst(rand.clone()).await;
        match dst.run_simulation(iterations).await {
            Ok(()) => {
                let next_u64 = rand.rng().random::<u64>();
                if let Some(expected_u64) = expected_u64 {
                    eprintln!("expected_u64: {}, next_u64: {}", expected_u64, next_u64);
                    assert_eq!(next_u64, expected_u64);
                }
                expected_u64 = Some(next_u64);
            }
            Err(e) => {
                error!("simulation failed with seed {}: {}", seed, e);
                return Err(e);
            }
        }
    }
    Ok(())
}

// TODO make test_deterministic_simulation have a seed parameter and use proptest
// #[tokio::test(start_paused = true, flavor = "current_thread")]
// async fn test_deterministic_simulation() -> Result<(), SlateDBError> {
//     let seed = std::env::var("SLATEDB_DST_SEED")
//         .map(|s| s.parse::<u64>().unwrap())
//         .unwrap_or_else(|_| rand::random::<u64>());
//     let rand = Rc::new(DbRand::new(seed));
//     let db = build_db(&rand).await;
//     let iterations = rand.rng().random_range(1..5_000_000);
//     info!(seed, iterations, "test_deterministic_simulation");
//     let dst_opts = DstOptions::default();
//     match Dst::new(
//         db,
//         rand.clone(),
//         Box::new(DefaultDstDistribution::new(rand, dst_opts.clone())),
//         dst_opts,
//     )
//     .run_simulation(iterations)
//     .await
//     {
//         Ok(_) => Ok(()),
//         Err(e) => {
//             error!(seed, ?e, "test_deterministic_simulation failed");
//             Err(e)
//         }
//     }
// }
