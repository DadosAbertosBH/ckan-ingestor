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

use anyhow::{anyhow, bail, Result};
use ckan_ingestor_lib::config::S3Settings;
use ckan_ingestor_lib::ingestion_service::IngestionService;
use ckan_ingestor_lib::s3_document_ingestor::S3DocumentIngestor;
use futures::FutureExt;
use futures::StreamExt;
use log::{error, info, warn};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::timeout as tokio_timeout;

use crate::messages::{JobMessage, JobResultMessage};

const RESULT_TOPIC: &str = "ckan.ingest.jobs_result";

type PartitionKey = (String, i32);

pub struct Worker {
    partition_locks: Arc<Mutex<HashMap<PartitionKey, Arc<Mutex<()>>>>>,
}

impl Worker {
    pub fn new() -> Result<Self> {
        Ok(Self {
            partition_locks: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        let bootstrap =
            env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_string());
        let topic = env::var("KAFKA_TOPIC").unwrap_or_else(|_| "ckan.ingest.jobs".to_string());
        let retry_topic =
            env::var("KAFKA_TOPIC_RETRY").unwrap_or_else(|_| "ckan.ingest.jobs.retry".to_string());
        let group_id = env::var("KAFKA_GROUP_ID").unwrap_or_else(|_| "ckan-worker-rs".to_string());

        info!(
            "Worker started, bootstrap={}, topics: {}, {}",
            bootstrap, topic, retry_topic
        );

        ensure_topics(&bootstrap, &topic, &retry_topic).await;

        // Set up shutdown signal ONCE.
        // Uses Notify instead of watch::channel — the latter resolves immediately
        // if the sender is dropped (e.g. when ctrl_c fails without a TTY in Docker).
        let shutdown = Arc::new(tokio::sync::Notify::new());
        {
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                // SIGTERM is what Docker sends on `docker stop`.
                // SIGINT is what Ctrl+C sends (useful for local dev).
                let mut sigterm =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("failed to register SIGTERM handler");
                let mut sigint =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                        .expect("failed to register SIGINT handler");

                tokio::select! {
                    _ = sigterm.recv() => {},
                    _ = sigint.recv() => {},
                }
                shutdown.notify_waiters();
            });
        }

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("acks", "all")
            .set("retries", "5")
            .set("message.timeout.ms", "10000")
            .create()?;
        let producer = Arc::new(producer);

        // Create S3 ingestor from env vars
        let s3 = Arc::new(create_s3_ingestor().await?);

        // Restart loop: if the consumer stream ends (disconnect, rebalance),
        // we recreate the consumer and re-subscribe.
        loop {
            let consumer: StreamConsumer = ClientConfig::new()
                .set("bootstrap.servers", &bootstrap)
                .set("group.id", &group_id)
                .set("auto.offset.reset", "earliest")
                .set("enable.auto.commit", "false")
                .set("enable.auto.offset.store", "false")
                .set("max.poll.interval.ms", "1800000")
                .set("session.timeout.ms", "45000")
                .set("heartbeat.interval.ms", "15000")
                .set("partition.assignment.strategy", "cooperative-sticky")
                .create()?;
            consumer.subscribe(&[&topic, &retry_topic])?;

            let consumer = Arc::new(consumer);
            let mut stream = consumer.stream();

            info!("Consumer stream started");

            let poll_timeout_secs: u64 = env::var("KAFKA_POLL_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300);
            let poll_timeout = Duration::from_secs(poll_timeout_secs);

            let stream_ended = loop {
                tokio::select! {
                    _ = shutdown.notified() => {
                        info!("Shutdown signal received");
                        return Ok(());
                    }
                    result = next_with_timeout(&mut stream, poll_timeout) => {
                        match result {
                            Ok(Some(Ok(msg))) => {
                                let topic = msg.topic().to_string();
                                let partition = msg.partition();
                                let owned = msg.detach();

                                let producer = producer.clone();
                                let consumer = consumer.clone();
                                let s3 = s3.clone();
                                let partition_locks = self.partition_locks.clone();

                                tokio::spawn(async move {
                                    let lock = {
                                        let mut locks = partition_locks.lock().await;
                                        locks
                                            .entry((topic.clone(), partition))
                                            .or_insert_with(|| Arc::new(Mutex::new(())))
                                            .clone()
                                    };
                                    let _guard = lock.lock().await;

                                    match process_message(&owned, &producer, s3.clone()).await {
                                        Ok(()) => {
                                            if let Err(e) = commit_offset_with_retry(
                                                &topic,
                                                partition,
                                                owned.offset(),
                                                &|tpl| consumer.commit(tpl, rdkafka::consumer::CommitMode::Sync),
                                                10,
                                            )
                                            .await
                                            {
                                                error!(
                                                    "Commit failed for {}/{}@{}: {}",
                                                    topic, partition, owned.offset(), e
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            error!(
                                                "Not committing offset {}/{}@{} — processing failed: {}",
                                                topic, partition, owned.offset(), e
                                            );
                                        }
                                    }
                                });
                            }
                            Ok(Some(Err(e))) => {
                                error!("Consumer error: {}", e);
                            }
                            Ok(None) => {
                                break true;
                            }
                            Err(_elapsed) => {
                                warn!(
                                    "Consumer poll timed out after {}s — recreating consumer",
                                    poll_timeout_secs
                                );
                                break true;
                            }
                        }
                    }
                }
            };

            if stream_ended {
                info!("Consumer stream ended, restarting in 5s...");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

async fn process_message(
    msg: &rdkafka::message::OwnedMessage,
    producer: &Arc<FutureProducer>,
    s3: Arc<S3DocumentIngestor>,
) -> Result<()> {
    let result = std::panic::AssertUnwindSafe(process_message_inner(msg, producer, s3))
        .catch_unwind()
        .await;
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => {
            error!(
                "Job processing failed [topic={} partition={} offset={}]: {}",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                e
            );
            Err(e)
        }
        Err(panic_err) => {
            let panic_msg = if let Some(s) = panic_err.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = panic_err.downcast_ref::<String>() {
                s.clone()
            } else {
                "unknown panic".to_string()
            };
            error!(
                "Job panicked [topic={} partition={} offset={}]: {}",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                panic_msg
            );
            Err(anyhow!("Panic: {}", panic_msg))
        }
    }
}

async fn process_message_inner(
    msg: &rdkafka::message::OwnedMessage,
    producer: &Arc<FutureProducer>,
    s3: Arc<S3DocumentIngestor>,
) -> Result<()> {
    let payload: &[u8] = match msg.payload() {
        Some(p) => p,
        None => {
            bail!(
                "Empty message payload [topic={} partition={} offset={}]",
                msg.topic(),
                msg.partition(),
                msg.offset()
            );
        }
    };

    let job_msg: JobMessage = match serde_json::from_slice(payload) {
        Ok(m) => m,
        Err(e) => {
            bail!(
                "Failed to parse job message [topic={} partition={} offset={}]: {}",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                e
            );
        }
    };

    let job_id = job_msg.job_id.clone();
    let resource_id = job_msg.resource_id.clone();

    info!(
        "Processing job {} (resource {}) [topic={} partition={} offset={}]",
        job_id,
        resource_id,
        msg.topic(),
        msg.partition(),
        msg.offset()
    );

    publish_result(
        producer,
        &JobResultMessage {
            job_id: job_id.clone(),
            status: "PROCESSING".to_string(),
            rows_processed: None,
            expected_rows: None,
            resource_size: None,
            encoding: None,
            expected_columns: None,
            datastore_active: false,
            labels: vec![],
            error_message: None,
            preview: None,
        },
    )
    .await?;

    let datastore_url = format!("{}/datastore/dump", job_msg.ckan_url.trim_end_matches('/'));

    let db_path = env::var("DUCKLAKE_DATABASE").expect("DUCKLAKE_DATABASE must be set");
    let catalog_uri = env::var("DUCKLAKE_CATALOG_URI").expect("DUCKLAKE_CATALOG_URI must be set");
    let s3_endpoint = env::var("S3_ENDPOINT").expect("S3_ENDPOINT must be set");
    let s3_bucket = env::var("S3_BUCKET").expect("S3_BUCKET must be set");
    let s3_access_key = env::var("S3_ACCESS_KEY_ID").expect("S3_ACCESS_KEY_ID must be set");
    let s3_secret_key = env::var("S3_SECRET_ACCESS_KEY").expect("S3_SECRET_ACCESS_KEY must be set");
    let s3_use_ssl = env::var("S3_USE_SSL").map(|v| v == "true").unwrap_or(false);
    let resource_url = job_msg.resource_url.clone();
    let resource_format = job_msg.resource_format.clone();

    let result = tokio::task::spawn_blocking(move || {
        let conn = duckdb::Connection::open(&db_path)?;
        // Configure S3 access for HTTP file downloads (httpfs extension)
        conn.execute_batch(&format!(
            "SET s3_endpoint='{}';", s3_endpoint
        ))?;
        conn.execute_batch(&format!(
            "SET s3_use_ssl={};", s3_use_ssl
        ))?;
        conn.execute_batch(&format!(
            "SET s3_access_key_id='{}';", s3_access_key
        ))?;
        conn.execute_batch(&format!(
            "SET s3_secret_access_key='{}';", s3_secret_key
        ))?;
        conn.execute_batch("SET s3_url_style='path';")?;
        conn.execute_batch("SET pg_debug_show_queries=false;")?;
        // Attach DuckLake catalog
        conn.execute_batch(&format!(
            "ATTACH IF NOT EXISTS 'ducklake:{}' AS lake (DATA_PATH 's3://{}', DATA_INLINING_ROW_LIMIT 10000, AUTOMATIC_MIGRATION TRUE);",
            catalog_uri, s3_bucket
        ))?;
        conn.execute_batch("USE lake;")?;
        conn.execute_batch("SET ducklake_max_retry_count = 100;")?;
        IngestionService::run(
            &conn,
            &resource_id,
            &resource_url,
            &resource_format,
            &datastore_url,
            &s3,
        )
    })
    .await
    .unwrap_or_else(|e| Err(anyhow::anyhow!("spawn_blocking panicked: {}", e)));

    match result {
        Ok(outcome) => {
            publish_result(
                producer,
                &JobResultMessage {
                    job_id: job_id.clone(),
                    status: outcome.status.clone(),
                    rows_processed: Some(outcome.rows_processed),
                    expected_rows: outcome.expected_rows,
                    resource_size: outcome.resource_size,
                    encoding: outcome.encoding,
                    expected_columns: outcome.expected_columns,
                    datastore_active: outcome.datastore_active,
                    labels: outcome.labels,
                    error_message: None,
                    preview: Some(outcome.preview),
                },
            )
            .await?;
            info!(
                "Job {} completed: {} [topic={} partition={} offset={}]",
                job_id,
                outcome.status,
                msg.topic(),
                msg.partition(),
                msg.offset()
            );
            Ok(())
        }
        Err(e) => {
            let error_str = format!("{}", e);
            let truncated = &error_str[..error_str.len().min(16_000)];
            publish_result(
                producer,
                &JobResultMessage {
                    job_id: job_id.clone(),
                    status: "FAILED".to_string(),
                    rows_processed: None,
                    expected_rows: None,
                    resource_size: None,
                    encoding: None,
                    expected_columns: None,
                    datastore_active: false,
                    labels: vec![],
                    error_message: Some(truncated.to_string()),
                    preview: None,
                },
            )
            .await?;
            error!(
                "Job {} failed [topic={} partition={} offset={}]: {}",
                job_id,
                msg.topic(),
                msg.partition(),
                msg.offset(),
                e
            );
            Ok(())
        }
    }
}

async fn ensure_topics(bootstrap: &str, topic: &str, retry_topic: &str) {
    let admin: AdminClient<_> = match ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
    {
        Ok(a) => a,
        Err(e) => {
            warn!("Failed to create admin client: {}", e);
            return;
        }
    };
    let topics: Vec<NewTopic> = [topic, retry_topic, RESULT_TOPIC]
        .iter()
        .map(|t| NewTopic {
            name: t,
            num_partitions: 10,
            replication: TopicReplication::Fixed(1),
            config: vec![],
        })
        .collect();
    match admin.create_topics(&topics, &AdminOptions::new()).await {
        Ok(results) => {
            for res in results {
                match res {
                    Ok(topic_name) => info!("Topic {} ready", topic_name),
                    Err((topic_name, err)) => {
                        if err.to_string().contains("ALREADY_EXISTS") {
                            info!("Topic {} already exists", topic_name);
                        } else {
                            warn!("Failed to create topic {}: {}", topic_name, err);
                        }
                    }
                }
            }
        }
        Err(e) => warn!("Failed to create topics: {}", e),
    }
}

async fn create_s3_ingestor() -> Result<S3DocumentIngestor> {
    let settings = S3Settings {
        endpoint: env::var("S3_ENDPOINT").unwrap_or_else(|_| "minio:9000".into()),
        bucket: env::var("S3_BUCKET").unwrap_or_else(|_| "warehouse".into()),
        access_key_id: env::var("S3_ACCESS_KEY_ID").unwrap_or_else(|_| "admin".into()),
        secret_access_key: env::var("S3_SECRET_ACCESS_KEY").unwrap_or_else(|_| "password".into()),
        use_ssl: env::var("S3_USE_SSL").map(|v| v == "true").unwrap_or(false),
        ..S3Settings::default()
    };
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(format!(
            "{}://{}",
            if settings.use_ssl { "https" } else { "http" },
            settings.endpoint
        ))
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            &settings.access_key_id,
            &settings.secret_access_key,
            None,
            None,
            "static",
        ))
        .load()
        .await;
    let s3_client = aws_sdk_s3::Client::new(&config);
    S3DocumentIngestor::new(settings, s3_client)
}

async fn publish_result(producer: &Arc<FutureProducer>, msg: &JobResultMessage) -> Result<()> {
    let payload = serde_json::to_vec(msg).map_err(|e| {
        anyhow::anyhow!(
            "serialize result job={} status={}: {}",
            msg.job_id,
            msg.status,
            e
        )
    })?;

    let record = FutureRecord::to(RESULT_TOPIC)
        .key(&msg.job_id)
        .payload(&payload);

    let (partition, offset) = producer
        .send(record, Timeout::After(Duration::from_secs(10)))
        .await
        .map_err(|(e, _)| {
            anyhow::anyhow!(
                "producer send job={} status={}: {}",
                msg.job_id,
                msg.status,
                e
            )
        })?;

    info!(
        "Published result for job {} [{}] to {} [{}:{}]",
        msg.job_id, msg.status, RESULT_TOPIC, partition, offset
    );
    Ok(())
}

// ── Stream stall detection ────────────────────────────────────────────

/// Wraps `futures::StreamExt::next()` with a timeout.
///
/// Returns:
/// - `Ok(Some(item))` — item received before timeout
/// - `Ok(None)` — stream ended normally
/// - `Err(Elapsed)` — timeout fired; the stream is stalled
async fn next_with_timeout<S, I>(
    stream: &mut S,
    timeout_dur: Duration,
) -> Result<Option<I>, tokio::time::error::Elapsed>
where
    S: futures::Stream<Item = I> + Unpin,
{
    tokio_timeout(timeout_dur, stream.next()).await
}

/// Commits a single offset with retry logic.
///
/// Returns `Ok(())` once the commit succeeds, or `Err` if all retries are
/// exhausted.  Unlike the old inline code, this does **not** call
/// `std::process::exit` — the caller decides what to do (typically log and
/// let the message be re-delivered).
async fn commit_offset_with_retry<F>(
    topic: &str,
    partition: i32,
    offset: i64,
    commit_fn: &F,
    max_retries: u64,
) -> Result<()>
where
    F: Fn(&rdkafka::TopicPartitionList) -> rdkafka::error::KafkaResult<()>,
{
    for attempt in 1..=max_retries {
        let mut tpl = rdkafka::TopicPartitionList::new();
        {
            let mut tp = tpl.add_partition(topic, partition);
            let _ = tp.set_offset(rdkafka::Offset::Offset(offset + 1));
        }
        match commit_fn(&tpl) {
            Ok(_) => {
                info!(
                    "Committed offset {}/{}@{} (attempt {})",
                    topic, partition, offset, attempt
                );
                return Ok(());
            }
            Err(e) => {
                error!(
                    "Failed to commit offset {}/{}@{} (attempt {}/{}): {}",
                    topic, partition, offset, attempt, max_retries, e
                );
                if attempt < max_retries {
                    tokio::time::sleep(Duration::from_millis(100 * attempt)).await;
                }
            }
        }
    }
    error!(
        "Failed to commit offset {}/{}@{} after {} attempts — message will be re-delivered",
        topic, partition, offset, max_retries
    );
    Err(anyhow!(
        "Commit failed for {}/{}@{} after {} retries",
        topic,
        partition,
        offset,
        max_retries
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::{self};
    use std::time::Duration;

    /// A stream that is permanently stalled — simulates a consumer whose
    /// underlying TCP connection is broken and librdkafka cannot recover.
    #[tokio::test]
    async fn stalled_stream_triggers_timeout() {
        let mut stalled = stream::pending::<u32>();
        let result = next_with_timeout(&mut stalled, Duration::from_millis(50)).await;
        assert!(
            result.is_err(),
            "Stalled stream must trigger Elapsed error, got {:?}",
            result
        );
    }

    /// An active stream that yields immediately must NOT trigger the timeout.
    #[tokio::test]
    async fn active_stream_does_not_trigger_timeout() {
        let mut stream = stream::iter(vec![1, 2, 3]);
        let result = next_with_timeout(&mut stream, Duration::from_millis(200)).await;
        assert!(
            matches!(result, Ok(Some(1))),
            "Active stream should yield item, got {:?}",
            result
        );
    }

    /// A stream that ends normally returns Ok(None), not an error.
    #[tokio::test]
    async fn ended_stream_returns_none() {
        let mut stream = stream::iter(Vec::<u32>::new());
        let result = next_with_timeout(&mut stream, Duration::from_millis(200)).await;
        assert!(
            matches!(result, Ok(None)),
            "Ended stream should return Ok(None), got {:?}",
            result
        );
    }

    /// Commit failure after all retries must NOT crash the process.
    /// When the consumer connection is dead (e.g. after a reconnection),
    /// the commit will fail. The message will be re-delivered to the new
    /// consumer — at-least-once semantics are acceptable.
    #[tokio::test]
    async fn commit_failure_after_retries_does_not_panic() {
        // Simulate a commit function that always fails.
        let commit_fn = |_tpl: &rdkafka::TopicPartitionList| -> rdkafka::error::KafkaResult<()> {
            Err(rdkafka::error::KafkaError::Canceled)
        };

        let result = commit_offset_with_retry(
            "test-topic",
            0,
            42,
            &commit_fn,
            3, // max_retries
        )
        .await;

        // Must not panic/exit — just return the error.
        assert!(
            result.is_err(),
            "Commit failure should return Err, got {:?}",
            result
        );
    }
}
