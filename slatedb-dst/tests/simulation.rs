use rstest::rstest;
use slatedb::DbRand;
use slatedb::SlateDBError;
use slatedb_dst::utils::build_db;
use slatedb_dst::{DefaultDstDistribution, Dst, DstOptions};
use std::rc::Rc;
use tracing::error;
use tracing::info;

#[rstest]
#[case(1, 10)]
#[case(2, 10)]
#[case(3, 10)]
#[case(4, 10)]
#[case(5, 10)]
#[tokio::test(start_paused = true, flavor = "current_thread")]
async fn test_dst_smoke_test(
    #[case] seed: u64,
    #[case] iterations: u32,
) -> Result<(), SlateDBError> {
    let rand = Rc::new(DbRand::new(seed));
    let db = build_db(&rand).await;
    info!(seed, iterations, "test_dst_smoke_test");
    let dst_opts = DstOptions::default();
    match Dst::new(
        db,
        rand.clone(),
        Box::new(DefaultDstDistribution::new(rand, dst_opts.clone())),
        dst_opts,
    )
    .run_simulation(iterations)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => {
            error!(seed, ?e, "test_dst_smoke_test failed");
            Err(e)
        }
    }
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
