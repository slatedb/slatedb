use slatedb::compaction_execute_bench::run_compaction_execute_bench;

fn main() {
    tracing_subscriber::fmt::init();
    run_compaction_execute_bench().unwrap();
}
