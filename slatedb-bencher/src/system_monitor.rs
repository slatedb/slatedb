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
    /// Flag to signal the monitoring thread to stop
    running: Arc<AtomicBool>,
}

impl SystemMonitor {
    /// Creates a new `SystemMonitor`.
    pub fn new() -> Self {
        SystemMonitor {
            handle: None,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Starts the system monitoring in a background thread.
    pub fn start(&mut self) {
        if self.handle.is_some() {
            info!("System monitoring is already running");
            return;
        }

        info!("Starting system resource monitoring");
        self.running.store(true, Ordering::SeqCst);
        let running = Arc::clone(&self.running);

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

                let global_cpu_usage = f32::trunc(system.global_cpu_usage() * 100.0) / 100.0;
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

                let total_memory_mb = system.total_memory() / 1024 / 1024;
                let used_memory_mb = system.used_memory() / 1024 / 1024;
                let free_memory_mb = system.free_memory() / 1024 / 1024;
                let available_memory_mb = system.available_memory() / 1024 / 1024;
                let total_swap_mb = system.total_swap() / 1024 / 1024;
                let free_swap_mb = system.free_swap() / 1024 / 1024;
                let used_swap_mb = system.used_swap() / 1024 / 1024;
                let bencher_memory_mb = system
                    .process(bencher_pid)
                    .map(|p| p.memory() / 1024 / 1024)
                    .unwrap_or(0);
                info!(
                    bencher_memory_mb,
                    total_memory_mb,
                    used_memory_mb,
                    free_memory_mb,
                    available_memory_mb,
                    total_swap_mb,
                    free_swap_mb,
                    used_swap_mb,
                    "memory usage (MiB)"
                );

                disks.refresh(true);
                let now = Instant::now();
                let elapsed = now.duration_since(last_disk_refresh);
                last_disk_refresh = now;

                for disk in disks.list() {
                    let usage = disk.usage();
                    let read_mb_since_last_refresh = usage.read_bytes / 1024 / 1024;
                    let write_mb_since_last_refresh = usage.written_bytes / 1024 / 1024;
                    let read_mb_per_second =
                        read_mb_since_last_refresh as f64 / elapsed.as_secs_f64();
                    let write_mb_per_second =
                        write_mb_since_last_refresh as f64 / elapsed.as_secs_f64();
                    let disk_name = disk.name().to_str().unwrap();
                    info!(
                        disk_name,
                        read_mb_per_second, write_mb_per_second, "disk usage (MiB/s)",
                    );
                }

                networks.refresh(true);
                let now = Instant::now();
                let elapsed = now.duration_since(last_network_refresh);
                last_network_refresh = now;

                for (interface_name, data) in networks.list() {
                    let received_mb_since_last_refresh = data.received() / 1024 / 1024;
                    let transmitted_mb_since_last_refresh = data.transmitted() / 1024 / 1024;
                    if received_mb_since_last_refresh == 0 && transmitted_mb_since_last_refresh == 0
                    {
                        continue;
                    }
                    let received_mb_per_second =
                        received_mb_since_last_refresh as f64 / elapsed.as_secs_f64();
                    let transmitted_mb_per_second =
                        transmitted_mb_since_last_refresh as f64 / elapsed.as_secs_f64();

                    info!(
                        interface_name,
                        received_mb_per_second, transmitted_mb_per_second, "network usage (MiB/s)",
                    );
                }
            }

            info!("System resource monitoring stopped");
        }));
    }

    /// Stops the system monitoring and waits for the thread to finish.
    pub fn stop(&mut self) {
        if let Some(handle) = self.handle.take() {
            info!("Stopping system resource monitoring");
            self.running.store(false, Ordering::SeqCst);
            if let Err(e) = handle.join() {
                info!("Error joining system monitor thread: {:?}", e);
            }
        } else {
            info!("System monitoring is not running");
        }
    }
}

impl Drop for SystemMonitor {
    fn drop(&mut self) {
        self.stop();
    }
}
