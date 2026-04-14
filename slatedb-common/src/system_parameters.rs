use std::collections::HashMap;

#[derive(Clone, Default)]
pub struct SystemParameters {
    // CPU
    pub global_cpu_usage: f32,
    pub cpu_core_usage: Vec<f32>,
    // Memory
    pub total_memory_mb: u64,
    pub used_memory_mb: u64,
    pub free_memory_mb: u64,
    pub available_memory_mb: u64,
    pub total_swap_mb: u64,
    pub free_swap_mb: u64,
    pub used_swap_mb: u64,
    pub bencher_memory_mb: u64,
    // Disk
    pub read_throughput_mb_per_disk: HashMap<String, f64>,
    pub write_throughput_mb_per_disk: HashMap<String, f64>,
    // Network
    pub recv_mb_per_interface: HashMap<String, f64>,
    pub write_mb_per_interface: HashMap<String, f64>,
}
