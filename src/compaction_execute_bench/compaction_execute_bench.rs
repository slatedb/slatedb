use slatedb::compaction_execute_bench::run_compaction_execute_bench;

use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

fn main() {
    env_logger::init();
    run_compaction_execute_bench().unwrap();
}
