use std::time::Duration;
use sysinfo::{Disks, Networks, System};
use tokio::{task::JoinHandle, time::Instant};
use tracing::info;

/// Starts a background task that monitors system resources (CPU, disk, memory, network)
/// and logs their usage every 10 seconds.
///
/// Returns a JoinHandle that can be used to await or abort the monitoring task.
pub(crate) fn start_monitoring() -> JoinHandle<()> {
    info!("Starting system resource monitoring");
    let mut system = System::new();
    let mut disks = Disks::new_with_refreshed_list();
    let mut networks = Networks::new_with_refreshed_list();
    let mut last_disk_refresh = Instant::now();
    let mut last_network_refresh = Instant::now();

    loop {
        system.refresh_cpu_usage();

        let global_cpu_usage = system.global_cpu_usage();
        let mut cpu_info = format!("CPU Usage: {:.1}% (global)", global_cpu_usage);

        for (idx, cpu) in system.cpus().iter().enumerate() {
            if idx < 4 {
                // Only show first few cores to avoid excessive logging
                cpu_info.push_str(&format!(" | Core {}: {:.1}%", idx, cpu.cpu_usage()));
            }
        }
        info!("{}", cpu_info);

        system.refresh_memory();

        let total_memory = system.total_memory() / 1024 / 1024; // Convert to MB
        let used_memory = system.used_memory() / 1024 / 1024; // Convert to MB
        let free_memory = system.free_memory() / 1024 / 1024; // Convert to MB
        let available_memory = system.available_memory() / 1024 / 1024; // Convert to MB
        let total_swap = system.total_swap() / 1024 / 1024; // Convert to MB
        let free_swap = system.free_swap() / 1024 / 1024; // Convert to MB
        let used_swap = system.used_swap() / 1024 / 1024; // Convert to MB
        let memory_usage = (used_memory as f64 / total_memory as f64) * 100.0;
        info!(
            total_memory,
            used_memory,
            free_memory,
            available_memory,
            total_swap,
            free_swap,
            used_swap,
            memory_usage,
            "memory usage (MiB)"
        );

        disks.refresh(true);
        let now = Instant::now();
        let elapsed = now.duration_since(last_disk_refresh);
        last_disk_refresh = now;

        for disk in disks.list() {
            let usage = disk.usage();
            let read_mebibytes_since_last_refresh = usage.read_bytes / 1024 / 1024;
            let write_mebibytes_since_last_refresh = usage.written_bytes / 1024 / 1024;
            let read_mebibytes_per_second =
                read_mebibytes_since_last_refresh as f64 / elapsed.as_secs_f64();
            let write_mebibytes_per_second =
                write_mebibytes_since_last_refresh as f64 / elapsed.as_secs_f64();
            let disk_name = disk.name().to_str().unwrap();
            info!(
                disk_name,
                read_mebibytes_per_second, write_mebibytes_per_second, "disk usage (MiB/s)",
            );
        }

        networks.refresh(true);
        let now = Instant::now();
        let elapsed = now.duration_since(last_network_refresh);
        last_network_refresh = now;

        for (interface_name, data) in networks.list() {
            let received_mebibytes_since_last_refresh = data.received() / 1024 / 1024;
            let transmitted_mebibytes_since_last_refresh = data.transmitted() / 1024 / 1024;
            let received_mebibytes_per_second =
                received_mebibytes_since_last_refresh as f64 / elapsed.as_secs_f64();
            let transmitted_mebibytes_per_second =
                transmitted_mebibytes_since_last_refresh as f64 / elapsed.as_secs_f64();

            info!(
                interface_name,
                received_mebibytes_per_second,
                transmitted_mebibytes_per_second,
                "network usage (MiB/s)",
            );
        }

        std::thread::sleep(Duration::from_secs(10));
    }
}
