// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use std::fs;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

const PROC_STATUS_PATH: &str = "/proc/self/status";
const CGROUP_MEMORY_CURRENT_PATH: &str = "/sys/fs/cgroup/memory.current";
const CGROUP_MEMORY_PEAK_PATH: &str = "/sys/fs/cgroup/memory.peak";
const CGROUP_MEMORY_STAT_PATH: &str = "/sys/fs/cgroup/memory.stat";
const CGROUP_MEMORY_EVENTS_PATH: &str = "/sys/fs/cgroup/memory.events.local";
const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);

static ACTIVE_JOBS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Default, PartialEq, Eq)]
struct MemoryStat {
    anon: u64,
    file: u64,
    kernel: u64,
    sock: u64,
    slab: u64,
}

#[derive(Debug)]
struct MemorySnapshot {
    rss: u64,
    current: u64,
    peak: Option<u64>,
    stat: MemoryStat,
    events_max: u64,
    events_oom: u64,
    events_oom_kill: u64,
}

impl MemorySnapshot {
    fn read() -> io::Result<Self> {
        let status = fs::read_to_string(PROC_STATUS_PATH)?;
        let rss = parse_rss_bytes(&status)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "VmRSS missing"))?;
        let current = read_number(CGROUP_MEMORY_CURRENT_PATH)?;
        let peak = read_number(CGROUP_MEMORY_PEAK_PATH).ok();
        let stat = parse_memory_stat(&fs::read_to_string(CGROUP_MEMORY_STAT_PATH)?);
        let events = fs::read_to_string(CGROUP_MEMORY_EVENTS_PATH).unwrap_or_default();

        Ok(Self {
            rss,
            current,
            peak,
            stat,
            events_max: parse_named_number(&events, "max").unwrap_or(0),
            events_oom: parse_named_number(&events, "oom").unwrap_or(0),
            events_oom_kill: parse_named_number(&events, "oom_kill").unwrap_or(0),
        })
    }
}

fn parse_rss_bytes(status: &str) -> Option<u64> {
    let kib = status.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        (fields.next()? == "VmRSS:")
            .then(|| fields.next()?.parse::<u64>().ok())
            .flatten()
    })?;
    kib.checked_mul(1024)
}

fn parse_memory_stat(stat: &str) -> MemoryStat {
    MemoryStat {
        anon: parse_named_number(stat, "anon").unwrap_or(0),
        file: parse_named_number(stat, "file").unwrap_or(0),
        kernel: parse_named_number(stat, "kernel").unwrap_or(0),
        sock: parse_named_number(stat, "sock").unwrap_or(0),
        slab: parse_named_number(stat, "slab").unwrap_or(0),
    }
}

fn parse_named_number(input: &str, name: &str) -> Option<u64> {
    input.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        (fields.next()? == name)
            .then(|| fields.next()?.parse::<u64>().ok())
            .flatten()
    })
}

fn read_number(path: impl AsRef<Path>) -> io::Result<u64> {
    fs::read_to_string(path)?.trim().parse().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid number: {error}"),
        )
    })
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

pub struct ActiveJobGuard;

pub fn track_job() -> ActiveJobGuard {
    ACTIVE_JOBS.fetch_add(1, Ordering::Relaxed);
    ActiveJobGuard
}

impl Drop for ActiveJobGuard {
    fn drop(&mut self) {
        ACTIVE_JOBS.fetch_sub(1, Ordering::Relaxed);
    }
}

pub fn active_jobs() -> usize {
    ACTIVE_JOBS.load(Ordering::Relaxed)
}

pub fn start() {
    if !Path::new(PROC_STATUS_PATH).exists() || !Path::new(CGROUP_MEMORY_CURRENT_PATH).exists() {
        log::warn!("memory profiler requires Linux cgroup v2; profiler disabled");
        return;
    }

    if let Err(error) = thread::Builder::new()
        .name("memory-profiler".to_string())
        .spawn(|| {
            loop {
                match MemorySnapshot::read() {
                    Ok(snapshot) => log::info!(
                        target: "memory_profiler",
                        "memory rss_mib={:.1} cgroup_mib={:.1} peak_mib={} anon_mib={:.1} file_mib={:.1} kernel_mib={:.1} sock_mib={:.1} slab_mib={:.1} active_jobs={} events_max={} events_oom={} events_oom_kill={}",
                        mib(snapshot.rss),
                        mib(snapshot.current),
                        snapshot
                            .peak
                            .map(|bytes| format!("{:.1}", mib(bytes)))
                            .unwrap_or_else(|| "unavailable".to_string()),
                        mib(snapshot.stat.anon),
                        mib(snapshot.stat.file),
                        mib(snapshot.stat.kernel),
                        mib(snapshot.stat.sock),
                        mib(snapshot.stat.slab),
                        active_jobs(),
                        snapshot.events_max,
                        snapshot.events_oom,
                        snapshot.events_oom_kill,
                    ),
                    Err(error) => {
                        log::warn!("memory profiler stopped: {error}");
                        return;
                    }
                }
                thread::sleep(SAMPLE_INTERVAL);
            }
        })
    {
        log::warn!("failed to start memory profiler: {error}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_rss_from_proc_status() {
        let status = "Name:\tworker\nVmSize:\t100000 kB\nVmRSS:\t6144 kB\nThreads:\t12\n";

        assert_eq!(parse_rss_bytes(status), Some(6 * 1024 * 1024));
    }

    #[test]
    fn parses_relevant_cgroup_memory_categories() {
        let stat =
            "anon 1048576\nfile 2097152\nkernel 3145728\nsock 4096\nslab 8192\ninactive_file 512\n";

        assert_eq!(
            parse_memory_stat(stat),
            MemoryStat {
                anon: 1_048_576,
                file: 2_097_152,
                kernel: 3_145_728,
                sock: 4_096,
                slab: 8_192,
            }
        );
    }

    #[test]
    fn guard_tracks_active_jobs_until_drop() {
        let before = active_jobs();
        let guard = track_job();

        assert_eq!(active_jobs(), before + 1);
        drop(guard);
        assert_eq!(active_jobs(), before);
    }
}
