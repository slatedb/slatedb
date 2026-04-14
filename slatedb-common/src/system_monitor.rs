use chrono::{DateTime, Utc};
use log::info;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};
use std::time::Duration;
use sysinfo::{Disks, Networks, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::task::JoinHandle;

use crate::clock::{DefaultSystemClock, SystemClock};
use crate::SystemParameters;

/// A system monitor that tracks CPU, memory, disk, and network usage.
pub struct SystemMonitor {
    /// Handle to the monitoring thread
    handle: Option<JoinHandle<()>>,
    /// Optional Tokio runtime handle for collecting runtime metrics
    runtime_handle: Option<tokio::runtime::Handle>,
    /// Flag to signal the monitoring thread to stop
    running: Arc<AtomicBool>,
    /// Shared system parameters updated by the monitoring thread
    parameters: Arc<RwLock<SystemParameters>>,
}

impl SystemMonitor {
    /// Creates a new `SystemMonitor`.
    pub fn new(runtime_handle: Option<tokio::runtime::Handle>) -> Self {
        SystemMonitor {
            handle: None,
            running: Arc::new(AtomicBool::new(false)),
            runtime_handle,
            parameters: Arc::new(RwLock::new(SystemParameters::default())),
        }
    }

    /// Returns a snapshot of the current system parameters.
    pub fn parameters(&self) -> SystemParameters {
        self.parameters
            .read()
            .expect("parameters lock poisoned")
            .clone()
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
        let parameters = Arc::clone(&self.parameters);
        self.handle = Some(tokio::task::spawn(async move {
            let mut system = System::new();
            let bencher_pid = sysinfo::get_current_pid().expect("failed to get current pid");
            let mut disks = Disks::new_with_refreshed_list();
            let mut networks = Networks::new_with_refreshed_list();
            let clock = DefaultSystemClock::new();
            let mut last_disk_refresh: DateTime<Utc> = clock.now();
            let mut last_network_refresh: DateTime<Utc> = clock.now();

            while running.load(Ordering::SeqCst) {
                for _ in 0..10 {
                    if !running.load(Ordering::SeqCst) {
                        break;
                    }
                    clock.sleep(Duration::from_secs(1)).await;
                }

                let mut system_parameters = parameters.write().expect("parameters lock poisoned");

                system.refresh_cpu_usage();

                // CPU
                system_parameters.global_cpu_usage =
                    f32::trunc(system.global_cpu_usage() * 10.0) / 10.0;
                system_parameters.cpu_core_usage = system
                    .cpus()
                    .iter()
                    .map(|cpu| f32::trunc(cpu.cpu_usage() * 10.0) / 10.0)
                    .collect::<Vec<_>>();
                info!(
                    "cpu usage: global={}, cores={:?}",
                    system_parameters.global_cpu_usage, system_parameters.cpu_core_usage,
                );

                // Memory
                system.refresh_memory();
                system.refresh_processes_specifics(
                    ProcessesToUpdate::Some(&[bencher_pid]),
                    true,
                    ProcessRefreshKind::nothing().with_memory(),
                );

                system_parameters.total_memory_mb = system.total_memory() / 1024 / 1024;
                system_parameters.used_memory_mb = system.used_memory() / 1024 / 1024;
                system_parameters.free_memory_mb = system.free_memory() / 1024 / 1024;
                system_parameters.available_memory_mb = system.available_memory() / 1024 / 1024;
                system_parameters.total_swap_mb = system.total_swap() / 1024 / 1024;
                system_parameters.free_swap_mb = system.free_swap() / 1024 / 1024;
                system_parameters.used_swap_mb = system.used_swap() / 1024 / 1024;
                system_parameters.bencher_memory_mb = system
                    .process(bencher_pid)
                    .map(|p| p.memory() / 1024 / 1024)
                    .unwrap_or(0);
                info!(
                    "memory usage (MiB): bencher={}, total={}, used={}, free={}, available={}, swap_total={}, swap_free={}, swap_used={}",
                    system_parameters.bencher_memory_mb,
                    system_parameters.total_memory_mb,
                    system_parameters.used_memory_mb,
                    system_parameters.free_memory_mb,
                    system_parameters.available_memory_mb,
                    system_parameters.total_swap_mb,
                    system_parameters.free_swap_mb,
                    system_parameters.used_swap_mb,
                );

                // Disk
                disks.refresh(true);
                let now = clock.now();
                let elapsed = now
                    .signed_duration_since(last_disk_refresh)
                    .to_std()
                    .expect("elapsed time is negative");
                last_disk_refresh = now;

                system_parameters.read_throughput_mb_per_disk.clear();
                system_parameters.write_throughput_mb_per_disk.clear();
                for disk in disks.list() {
                    let usage = disk.usage();
                    let read_mb_since_last_refresh = usage.read_bytes / 1024 / 1024;
                    let write_mb_since_last_refresh = usage.written_bytes / 1024 / 1024;
                    let read_mb_per_second =
                        read_mb_since_last_refresh as f64 / elapsed.as_secs_f64();
                    let write_mb_per_second =
                        write_mb_since_last_refresh as f64 / elapsed.as_secs_f64();
                    let disk_name = disk.name().to_str().expect("").to_string();
                    info!(
                        "disk usage (MiB/s): disk={}, read={}, write={}",
                        disk_name, read_mb_per_second, write_mb_per_second,
                    );
                    system_parameters
                        .read_throughput_mb_per_disk
                        .insert(disk_name.clone(), read_mb_per_second);
                    system_parameters
                        .write_throughput_mb_per_disk
                        .insert(disk_name, write_mb_per_second);
                }

                // Network
                networks.refresh(true);
                let now = clock.now();
                let elapsed = now
                    .signed_duration_since(last_network_refresh)
                    .to_std()
                    .expect("elapsed time is negative");
                last_network_refresh = now;

                system_parameters.recv_mb_per_interface.clear();
                system_parameters.write_mb_per_interface.clear();
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
                        "network usage (MiB/s): interface={}, recv={}, transmit={}",
                        interface_name, received_mb_per_second, transmitted_mb_per_second,
                    );
                    system_parameters
                        .recv_mb_per_interface
                        .insert(interface_name.clone(), received_mb_per_second);
                    system_parameters
                        .write_mb_per_interface
                        .insert(interface_name.clone(), transmitted_mb_per_second);
                }

                // drop the write lock before logging tokio metrics
                drop(system_parameters);

                if let Some(handle) = &runtime_handle {
                    let metrics = handle.metrics();
                    let num_workers = metrics.num_workers();
                    let num_alive_tasks = metrics.num_alive_tasks();
                    let global_queue_depth = metrics.global_queue_depth();
                    info!(
                        "tokio runtime metrics: workers={}, alive_tasks={}, global_queue_depth={}",
                        num_workers, num_alive_tasks, global_queue_depth,
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
            handle.abort();
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
