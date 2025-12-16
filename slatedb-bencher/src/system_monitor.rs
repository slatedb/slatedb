use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use sysinfo::{Disks, Networks, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::time::Instant;
use tracing::info;

/// A system monitor that tracks CPU, memory, disk, and network usage.
pub struct SystemMonitor {
    /// Handle to the monitoring thread
    handle: Option<JoinHandle<()>>,
    /// Optional Tokio runtime handle for collecting runtime metrics
    runtime_handle: Option<tokio::runtime::Handle>,
    /// Flag to signal the monitoring thread to stop
    running: Arc<AtomicBool>,
}

impl SystemMonitor {
    /// Creates a new `SystemMonitor`.
    pub fn new(runtime_handle: Option<tokio::runtime::Handle>) -> Self {
        SystemMonitor {
            handle: None,
            running: Arc::new(AtomicBool::new(false)),
            runtime_handle,
        }
    }

    /// Starts the system monitoring in a background thread.
    pub fn start(&mut self) {
        if self.handle.is_some() {
            info!("system monitoring is already running");
            return;
        }

        info!("starting system resource monitoring");
        self.running.store(true, Ordering::SeqCst);
        let running = Arc::clone(&self.running);

        let runtime_handle = self.runtime_handle.clone();
        self.handle = Some(thread::spawn(move || {
            let mut system = System::new();
            let bencher_pid = sysinfo::get_current_pid().unwrap();
            let mut disks = Disks::new_with_refreshed_list();
            let mut networks = Networks::new_with_refreshed_list();
            let mut last_disk_refresh = Instant::now();
            let mut last_network_refresh = Instant::now();

            while running.load(Ordering::SeqCst) {
                for _ in 0..10 {
                    if !running.load(Ordering::SeqCst) {
                        break;
                    }
                    thread::sleep(Duration::from_secs(1));
                }

                system.refresh_cpu_usage();

                let global_cpu_usage = f32::trunc(system.global_cpu_usage() * 10.0) / 10.0;
                let cpu_core_usage = system
                    .cpus()
                    .iter()
                    .map(|cpu| f32::trunc(cpu.cpu_usage() * 10.0) / 10.0)
                    .collect::<Vec<_>>();
                info!(global_cpu_usage, ?cpu_core_usage, "cpu usage");

                system.refresh_memory();
                system.refresh_processes_specifics(
                    ProcessesToUpdate::Some(&[bencher_pid]),
                    true,
                    ProcessRefreshKind::nothing().with_memory(),
                );

                let total_memory = slatedb::format_bytes_si(system.total_memory());
                let used_memory = slatedb::format_bytes_si(system.used_memory());
                let free_memory = slatedb::format_bytes_si(system.free_memory());
                let available_memory = slatedb::format_bytes_si(system.available_memory());
                let total_swap = slatedb::format_bytes_si(system.total_swap());
                let free_swap = slatedb::format_bytes_si(system.free_swap());
                let used_swap = slatedb::format_bytes_si(system.used_swap());
                let bencher_memory = system
                    .process(bencher_pid)
                    .map(|p| slatedb::format_bytes_si(p.memory()))
                    .unwrap_or_else(|| "0 B".to_string());
                info!(
                    bencher_memory,
                    total_memory,
                    used_memory,
                    free_memory,
                    available_memory,
                    total_swap,
                    free_swap,
                    used_swap,
                    "memory usage"
                );

                disks.refresh(true);
                let now = Instant::now();
                let elapsed = now.duration_since(last_disk_refresh);
                last_disk_refresh = now;

                for disk in disks.list() {
                    let usage = disk.usage();
                    let read_bytes_per_second =
                        (usage.read_bytes as f64 / elapsed.as_secs_f64()) as u64;
                    let write_bytes_per_second =
                        (usage.written_bytes as f64 / elapsed.as_secs_f64()) as u64;
                    let disk_name = disk.name().to_str().unwrap();
                    let read_per_second =
                        format!("{}/s", slatedb::format_bytes_si(read_bytes_per_second));
                    let write_per_second =
                        format!("{}/s", slatedb::format_bytes_si(write_bytes_per_second));
                    info!(
                        disk_name,
                        read_per_second, write_per_second, "disk usage",
                    );
                }

                networks.refresh(true);
                let now = Instant::now();
                let elapsed = now.duration_since(last_network_refresh);
                last_network_refresh = now;

                for (interface_name, data) in networks.list() {
                    let received_bytes = data.received();
                    let transmitted_bytes = data.transmitted();
                    if received_bytes == 0 && transmitted_bytes == 0 {
                        continue;
                    }
                    let received_bytes_per_second =
                        (received_bytes as f64 / elapsed.as_secs_f64()) as u64;
                    let transmitted_bytes_per_second =
                        (transmitted_bytes as f64 / elapsed.as_secs_f64()) as u64;
                    let received_per_second =
                        format!("{}/s", slatedb::format_bytes_si(received_bytes_per_second));
                    let transmitted_per_second =
                        format!("{}/s", slatedb::format_bytes_si(transmitted_bytes_per_second));

                    info!(
                        interface_name,
                        received_per_second, transmitted_per_second, "network usage",
                    );
                }

                if let Some(handle) = &runtime_handle {
                    let metrics = handle.metrics();
                    let num_workers = metrics.num_workers();
                    let num_alive_tasks = metrics.num_alive_tasks();
                    let global_queue_depth = metrics.global_queue_depth();
                    info!(
                        num_workers,
                        num_alive_tasks, global_queue_depth, "tokio runtime metrics",
                    );
                }
            }

            info!("system resource monitoring stopped");
        }));
    }

    /// Stops the system monitoring and waits for the thread to finish.
    pub fn stop(&mut self) {
        if let Some(handle) = self.handle.take() {
            info!("stopping system resource monitoring");
            self.running.store(false, Ordering::SeqCst);
            if let Err(e) = handle.join() {
                info!("error joining system monitor thread [error={:?}]", e);
            }
        } else {
            info!("system monitoring is not running");
        }
    }
}

impl Drop for SystemMonitor {
    fn drop(&mut self) {
        self.stop();
    }
}
