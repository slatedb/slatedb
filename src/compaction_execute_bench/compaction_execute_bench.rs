use slatedb::compaction_execute_bench::run_compaction_execute_bench;

fn main() {
    env_logger::init();
    run_compaction_execute_bench().unwrap();
}
