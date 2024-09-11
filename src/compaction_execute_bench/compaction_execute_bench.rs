use slatedb::compaction_execute_bench::run_compaction_execute_bench;

fn main() {
    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt().with_writer(non_blocking).init();
    run_compaction_execute_bench().unwrap();
}
