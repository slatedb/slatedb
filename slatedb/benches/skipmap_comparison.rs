#![cfg(not(windows))]

use criterion::criterion_main;

criterion_main!(slatedb::skipmap_bench::benches);
