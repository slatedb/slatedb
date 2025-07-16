use rand::Rng;
use rstest::rstest;
use slatedb::DbRand;
use slatedb::SlateDBError;
use slatedb_dst::utils::build_dst;
use std::rc::Rc;
use tracing::error;
use tracing::info;

#[rstest]
#[case(Rc::new(DbRand::new(1)), 100)]
#[case(Rc::new(DbRand::new(2)), 100)]
#[tokio::test(start_paused = true, flavor = "current_thread")]
async fn test_dst_quickly(
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
#[case(101, 10, 100)]
#[case(102, 10, 100)]
#[case(103, 10, 100)]
#[case(104, 10, 100)]
#[case(105, 10, 100)]
async fn test_dst_is_deterministic(
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
                    assert_eq!(
                        next_u64, expected_u64,
                        "non-determinism detected: next_u64={}, expected_u64={}",
                        next_u64, expected_u64
                    );
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
