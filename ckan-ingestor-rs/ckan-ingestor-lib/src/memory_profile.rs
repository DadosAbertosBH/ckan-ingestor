// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// ckan-ingestor-rs is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with ckan-ingestor-rs.  If not, see <https://www.gnu.org/licenses/>.

use duckdb::arrow::array::RecordBatch;
use duckdb::Connection;
use log::debug;

#[cfg(test)]
static SNAPSHOT_COLLECTION_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[derive(Debug)]
struct DuckDbMemory {
    tag: String,
    memory_usage_bytes: i64,
    temporary_storage_bytes: i64,
}

#[derive(Debug, PartialEq)]
struct ArrowBatchStats {
    batch_count: usize,
    row_count: usize,
    memory_bytes: usize,
}

pub(crate) fn log_snapshot(
    conn: &Connection,
    stage: &str,
    resource_id: &str,
    batches: Option<&[RecordBatch]>,
) {
    if !log::log_enabled!(target: "memory_profile", log::Level::Debug) {
        return;
    }

    #[cfg(test)]
    SNAPSHOT_COLLECTION_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    let process_rss_bytes = read_process_rss_bytes();
    let cgroup_memory_bytes = read_u64_file("/sys/fs/cgroup/memory.current");
    let cgroup_memory_limit_bytes = std::fs::read_to_string("/sys/fs/cgroup/memory.max")
        .ok()
        .and_then(|value| parse_cgroup_limit(value.trim()));
    let arrow = batches.map(arrow_batch_stats);

    let duckdb_memory = match read_duckdb_memory(conn) {
        Ok(memory) => memory,
        Err(error) => {
            debug!(
                target: "memory_profile",
                "duckdb_memory() unavailable stage={} error={}",
                stage,
                error
            );
            Vec::new()
        }
    };
    let duckdb_memory_summary = duckdb_memory
        .iter()
        .map(|memory| {
            format!(
                "{}={}B,temp={}B",
                memory.tag, memory.memory_usage_bytes, memory.temporary_storage_bytes
            )
        })
        .collect::<Vec<_>>()
        .join(",");

    debug!(
        target: "memory_profile",
        "memory snapshot stage={} resource_id={} process_rss_bytes={:?} cgroup_memory_bytes={:?} cgroup_memory_limit_bytes={:?} arrow={:?} duckdb_memory={}",
        stage,
        resource_id,
        process_rss_bytes,
        cgroup_memory_bytes,
        cgroup_memory_limit_bytes,
        arrow,
        duckdb_memory_summary
    );
}

fn read_duckdb_memory(conn: &Connection) -> anyhow::Result<Vec<DuckDbMemory>> {
    let mut statement = conn
        .prepare("SELECT tag, memory_usage_bytes, temporary_storage_bytes FROM duckdb_memory()")?;
    let rows = statement
        .query_map([], |row| {
            Ok(DuckDbMemory {
                tag: row.get(0)?,
                memory_usage_bytes: row.get(1)?,
                temporary_storage_bytes: row.get(2)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn arrow_batch_stats(batches: &[RecordBatch]) -> ArrowBatchStats {
    ArrowBatchStats {
        batch_count: batches.len(),
        row_count: batches.iter().map(RecordBatch::num_rows).sum(),
        memory_bytes: batches.iter().map(RecordBatch::get_array_memory_size).sum(),
    }
}

fn read_process_rss_bytes() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find(|line| line.starts_with("VmRSS:"))
                .and_then(parse_memory_value)
        })
}

fn read_u64_file(path: &str) -> Option<u64> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|value| value.trim().parse().ok())
}

fn parse_memory_value(value: &str) -> Option<u64> {
    let mut parts = value.split_whitespace();
    parts.next()?;
    let amount: u64 = parts.next()?.parse().ok()?;
    match parts.next() {
        Some("kB") => amount.checked_mul(1024),
        Some("MB") => amount.checked_mul(1024 * 1024),
        Some("bytes") | None => Some(amount),
        _ => None,
    }
}

fn parse_cgroup_limit(value: &str) -> Option<u64> {
    (value != "max").then(|| value.parse().ok()).flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use duckdb::arrow::datatypes::Schema;

    struct DisabledLogger;

    impl log::Log for DisabledLogger {
        fn enabled(&self, _: &log::Metadata<'_>) -> bool {
            false
        }

        fn log(&self, _: &log::Record<'_>) {}

        fn flush(&self) {}
    }

    static DISABLED_LOGGER: DisabledLogger = DisabledLogger;

    #[test]
    fn parses_linux_memory_values_in_kibibytes() {
        assert_eq!(parse_memory_value("VmRSS:       2048 kB"), Some(2_097_152));
        assert_eq!(parse_memory_value("memory.current\n"), None);
    }

    #[test]
    fn parses_cgroup_memory_limit_and_unlimited_value() {
        assert_eq!(parse_cgroup_limit("4294967296"), Some(4_294_967_296));
        assert_eq!(parse_cgroup_limit("max"), None);
    }

    #[test]
    fn reads_duckdb_memory_metadata_without_affecting_ingestion() {
        let conn = Connection::open_in_memory().expect("in-memory DuckDB");
        let memory = read_duckdb_memory(&conn).expect("duckdb_memory() should be available");

        assert!(memory.iter().all(|entry| entry.memory_usage_bytes >= 0));
    }

    #[test]
    fn summarizes_arrow_batches_for_resource_correlation() {
        let schema = Arc::new(Schema::empty());
        let batches = vec![
            RecordBatch::new_empty(Arc::clone(&schema)),
            RecordBatch::new_empty(schema),
        ];

        assert_eq!(
            arrow_batch_stats(&batches),
            ArrowBatchStats {
                batch_count: 2,
                row_count: 0,
                memory_bytes: 0,
            }
        );
    }

    #[test]
    fn skips_snapshot_collection_when_profiling_is_disabled() {
        log::set_logger(&DISABLED_LOGGER).expect("test logger should be installed once");
        log::set_max_level(log::LevelFilter::Trace);
        SNAPSHOT_COLLECTION_COUNT.store(0, std::sync::atomic::Ordering::SeqCst);
        let conn = Connection::open_in_memory().expect("in-memory DuckDB");

        log_snapshot(&conn, "test", "resource", None);

        assert_eq!(
            SNAPSHOT_COLLECTION_COUNT.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }
}
