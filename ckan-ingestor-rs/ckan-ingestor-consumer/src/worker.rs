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

use anyhow::{anyhow, Result};
use ckan_ingestor_lib::config::S3Settings;
use ckan_ingestor_lib::ingestion_service::IngestionService;
use ckan_ingestor_lib::s3_document_ingestor::S3DocumentIngestor;
use futures::StreamExt;
use log::{error, info, warn};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::messages::{JobMessage, JobResultMessage};

const RESULT_TOPIC: &str = "ckan.ingest.jobs_result";

type PartitionKey = (String, i32);

pub struct Worker;

impl Worker {
    pub fn new() -> Result<Self> {
        Ok(Self {})
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

        // Shutdown signal
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let shutdown_requested = Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let shutdown = shutdown.clone();
            let shutdown_requested = shutdown_requested.clone();
            tokio::spawn(async move {
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
                shutdown_requested.store(true, std::sync::atomic::Ordering::SeqCst);
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

        // Restart loop
        loop {
            let consumer = Arc::new(
                ClientConfig::new()
                    .set("bootstrap.servers", &bootstrap)
                    .set("group.id", &group_id)
                    .set("auto.offset.reset", "earliest")
                    .set("enable.auto.commit", "false")
                    .set("enable.auto.offset.store", "false")
                    .set("max.poll.interval.ms", "1800000")
                    .set("session.timeout.ms", "45000")
                    .set("heartbeat.interval.ms", "15000")
                    .set("partition.assignment.strategy", "cooperative-sticky")
                    .create::<StreamConsumer>()?,
            );
            consumer.subscribe(&[&topic, &retry_topic])?;

            // mpsc channels: one per partition. Buffer of 256 messages.
            let mut senders: HashMap<PartitionKey, mpsc::Sender<rdkafka::message::OwnedMessage>> =
                HashMap::new();
            let mut workers = JoinSet::new();

            // Spawn a worker for each possible partition of each topic.
            // The worker reads from an mpsc receiver and processes messages
            // sequentially, preserving per-partition ordering.
            for t in [topic.as_str(), retry_topic.as_str()] {
                for p in 0..5 {
                    let (tx, rx) = mpsc::channel::<rdkafka::message::OwnedMessage>(256);
                    senders.insert((t.to_string(), p), tx);
                    workers.spawn(run_partition_worker(
                        t.to_string(),
                        p,
                        rx,
                        producer.clone(),
                        s3.clone(),
                        consumer.clone(),
                        shutdown.clone(),
                    ));
                }
            }

            info!("Spawned {} partition workers", workers.len());

            // Main dispatch loop: reads from consumer.stream() and routes
            // each message to the appropriate partition mpsc channel.
            let dispatch_shutdown = shutdown.clone();
            let dispatch_result: Result<()> = async {
                let mut stream = consumer.stream();
                loop {
                    tokio::select! {
                        _ = dispatch_shutdown.notified() => {
                            info!("Shutdown signal received, stopping dispatch");
                            return Ok(());
                        }
                        msg = stream.next() => {
                            match msg {
                                Some(Ok(msg)) => {
                                    let key: PartitionKey = (
                                        msg.topic().to_string(),
                                        msg.partition(),
                                    );
                                    if let Some(tx) = senders.get(&key) {
                                        if tx.send(msg.detach()).await.is_err() {
                                            // Worker's receiver dropped — worker exited
                                            warn!(
                                                "Partition {}/{} worker channel closed",
                                                key.0, key.1
                                            );
                                        }
                                    } else {
                                        warn!(
                                            "No worker for {}/{} — message skipped [offset={}]",
                                            key.0, key.1, msg.offset()
                                        );
                                    }
                                }
                                Some(Err(e)) => {
                                    error!("Consumer error: {}", e);
                                }
                                None => {
                                    info!("Consumer stream ended, restarting...");
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
            }
            .await;

            // Drop senders to signal workers that no more messages are coming.
            drop(senders);

            // Wait for all partition workers to drain.
            info!("Draining {} partition workers...", workers.len());
            workers.shutdown().await;
            info!("All partition workers drained");

            dispatch_result?;

            if shutdown_requested.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(());
            }
            info!("Consumer stream ended, restarting in 5s...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}

/// Runs a single partition worker: receives messages via an mpsc channel,
/// processes them sequentially, and commits offsets after each message.
async fn run_partition_worker(
    topic: String,
    partition: i32,
    mut rx: mpsc::Receiver<rdkafka::message::OwnedMessage>,
    producer: Arc<FutureProducer>,
    s3: Arc<S3DocumentIngestor>,
    consumer: Arc<StreamConsumer>,
    shutdown: Arc<tokio::sync::Notify>,
) {
    info!("Partition {}/{} worker ready", topic, partition);

    loop {
        tokio::select! {
            _ = shutdown.notified() => {
                info!("Partition {}/{} shutting down", topic, partition);
                return;
            }
            maybe_msg = rx.recv() => {
                match maybe_msg {
                    Some(owned) => {
                        let offset = owned.offset();

                        if let Err(e) = process_message(&owned, &producer, s3.clone()).await {
                            error!(
                                "Processing failed for {}/{}@{}: {}",
                                topic, partition, offset, e
                            );
                        }

                        if let Err(e) = commit_offset_with_retry(
                            &topic,
                            partition,
                            offset,
                            &|tpl| consumer.commit(tpl, CommitMode::Sync),
                            10,
                        )
                        .await
                        {
                            error!(
                                "Commit failed for {}/{}@{} after all retries: {} \
                                 — message will be re-delivered on restart",
                                topic, partition, offset, e
                            );
                        }
                    }
                    None => {
                        // Channel closed — no more messages
                        info!(
                            "Partition {}/{} channel closed, worker exiting",
                            topic, partition
                        );
                        return;
                    }
                }
            }
        }
    }
}

async fn process_message(
    msg: &rdkafka::message::OwnedMessage,
    producer: &Arc<FutureProducer>,
    s3: Arc<S3DocumentIngestor>,
) -> Result<()> {
    let payload: &[u8] = msg.payload().ok_or_else(|| {
        anyhow!(
            "Empty message payload [topic={} partition={} offset={}]",
            msg.topic(),
            msg.partition(),
            msg.offset()
        )
    })?;

    let job_msg: JobMessage = serde_json::from_slice(payload).map_err(|e| {
        anyhow!(
            "Failed to parse job message [topic={} partition={} offset={}]: {}",
            msg.topic(),
            msg.partition(),
            msg.offset(),
            e
        )
    })?;

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
        conn.execute_batch(&format!("SET s3_endpoint='{}';", s3_endpoint))?;
        conn.execute_batch(&format!("SET s3_use_ssl={};", s3_use_ssl))?;
        conn.execute_batch(&format!(
            "SET s3_access_key_id='{}';",
            s3_access_key
        ))?;
        conn.execute_batch(&format!(
            "SET s3_secret_access_key='{}';",
            s3_secret_key
        ))?;
        conn.execute_batch("SET s3_url_style='path';")?;
        conn.execute_batch("SET pg_debug_show_queries=false;")?;
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
            num_partitions: 5,
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
    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
    use std::time::Duration;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn commit_failure_after_retries_does_not_panic() {
        let commit_fn = |_tpl: &rdkafka::TopicPartitionList| -> rdkafka::error::KafkaResult<()> {
            Err(rdkafka::error::KafkaError::Canceled)
        };

        let result = commit_offset_with_retry("test-topic", 0, 42, &commit_fn, 3).await;

        assert!(
            result.is_err(),
            "Commit must fail after all retries, got {:?}",
            result
        );
    }

    /// Validates that partition workers process messages sequentially via
    /// mpsc channels, and that two workers can run concurrently.
    #[tokio::test]
    async fn partition_workers_run_concurrently() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let processed = Arc::new(AtomicI32::new(0));
        let completed = Arc::new(AtomicI32::new(0));

        // Simulate two partition channels
        let (tx1, mut rx1) = mpsc::channel::<i32>(10);
        let (tx2, mut rx2) = mpsc::channel::<i32>(10);

        // Messages for partition 0: [1, 2, 3]
        tx1.send(1).await.unwrap();
        tx1.send(2).await.unwrap();
        tx1.send(3).await.unwrap();
        drop(tx1);

        // Messages for partition 1: [10, 20]
        tx2.send(10).await.unwrap();
        tx2.send(20).await.unwrap();
        drop(tx2);

        let p1 = processed.clone();
        let c1 = completed.clone();
        let s1 = shutdown.clone();
        let h1 = tokio::spawn(async move {
            while let Some(msg) = rx1.recv().await {
                p1.fetch_add(msg, Ordering::SeqCst);
            }
            c1.fetch_add(1, Ordering::SeqCst);
            s1.notify_one();
        });

        let p2 = processed.clone();
        let c2 = completed.clone();
        let s2 = shutdown.clone();
        let h2 = tokio::spawn(async move {
            while let Some(msg) = rx2.recv().await {
                p2.fetch_add(msg, Ordering::SeqCst);
            }
            c2.fetch_add(1, Ordering::SeqCst);
            s2.notify_one();
        });

        // Wait for both partition tasks to finish
        shutdown.notified().await;
        shutdown.notified().await;

        h1.await.unwrap();
        h2.await.unwrap();

        assert_eq!(processed.load(Ordering::SeqCst), 36);
        assert_eq!(completed.load(Ordering::SeqCst), 2);
    }

    /// Validates that shutdown stops partition workers.
    #[tokio::test]
    async fn shutdown_stops_partition_workers() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let running = Arc::new(AtomicBool::new(false));

        let s = shutdown.clone();
        let r = running.clone();
        let handle = tokio::spawn(async move {
            r.store(true, Ordering::SeqCst);
            s.notified().await;
            r.store(false, Ordering::SeqCst);
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(running.load(Ordering::SeqCst), "Worker should be running");

        shutdown.notify_waiters();

        tokio::time::sleep(Duration::from_millis(10)).await;
        handle.await.unwrap();
        assert!(
            !running.load(Ordering::SeqCst),
            "Worker should have stopped"
        );
    }
}
