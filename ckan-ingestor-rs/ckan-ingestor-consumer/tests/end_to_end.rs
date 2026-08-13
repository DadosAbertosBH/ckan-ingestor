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

use std::sync::Arc;
use std::time::Duration;

use ckan_ingestor_consumer::coordinator_consumer_context::CoordinatorConsumerContext;
use ckan_ingestor_consumer::job_processor::JobProcessor;
use ckan_ingestor_consumer::messages::{JobMessage, JobResultMessage};
use ckan_ingestor_consumer::worker_coordinator::WorkerCoordinator;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::sync::{Notify, mpsc};

#[derive(Clone)]
struct StubProcessor;

impl JobProcessor for StubProcessor {
    fn process(&self, job: JobMessage) -> JobResultMessage {
        JobResultMessage {
            job_id: job.job_id,
            status: "done".to_string(),
            rows_processed: Some(1),
            expected_rows: None,
            resource_size: None,
            encoding: None,
            expected_columns: None,
            datastore_active: false,
            labels: vec![],
            error_message: None,
            preview: None,
        }
    }
}

#[tokio::test]
#[ignore = "requires a Docker daemon to run Kafka via testcontainers"]
async fn end_to_end_message_flow() -> anyhow::Result<()> {
    // 1. Start a single-node Kafka broker (KRaft mode).
    let _kafka = GenericImage::new("apache/kafka-native", "4.2.1")
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var(
            "KAFKA_LISTENERS",
            "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
        )
        .with_env_var("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://localhost:9092")
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        )
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env_var("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
        .with_mapped_port(9092, 9092.tcp())
        .with_ready_conditions(vec![WaitFor::seconds(15)])
        .start()
        .await?;

    let bootstrap = "localhost:9092";

    // 2. Create topics explicitly (mirrors worker.rs ensure_topics).
    ckan_ingestor_consumer::ensure_topics(bootstrap, "ckan.ingest.jobs", "ckan.ingest.jobs.retry")
        .await;

    // 3. A separate consumer verifies the result topic.
    let result_consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", "test-verifier")
        .set("auto.offset.reset", "earliest")
        .create()?;
    result_consumer.subscribe(&["ckan.ingest.jobs_result"])?;

    // 3. Producer for the input job message.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()?;
    let producer = Arc::new(producer);

    // 4. Coordinator wired with the rebalance context and a stub processor.
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let context = CoordinatorConsumerContext::new(cmd_tx);

    let consumer: StreamConsumer<CoordinatorConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", "test-worker")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .create_with_context(context)?;
    let consumer = Arc::new(consumer);
    consumer.subscribe(&["ckan.ingest.jobs", "ckan.ingest.jobs.retry"])?;

    let mut coordinator = WorkerCoordinator::new(consumer, StubProcessor);
    let shutdown = Arc::new(Notify::new());

    let producer_for_coordinator = producer.clone();
    let shutdown_for_coordinator = shutdown.clone();
    let coordinator_handle = tokio::spawn(async move {
        coordinator
            .run(cmd_rx, producer_for_coordinator, shutdown_for_coordinator)
            .await;
    });

    // Give the consumer group time to rebalance and assign partitions.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // 5. Produce a job message.
    let job = JobMessage {
        job_id: "job-1".to_string(),
        resource_id: "res-1".to_string(),
        ckan_url: "http://ckan".to_string(),
        resource_url: "".to_string(),
        resource_format: "".to_string(),
    };
    let payload = serde_json::to_vec(&job)?;
    producer
        .send(
            FutureRecord::to("ckan.ingest.jobs")
                .key(&job.job_id)
                .payload(&payload),
            Timeout::After(Duration::from_secs(10)),
        )
        .await
        .map_err(|(e, _)| anyhow::anyhow!("produce failed: {}", e))?;

    // 6. Verify PROCESSING + final result are published.
    let first = tokio::time::timeout(Duration::from_secs(20), result_consumer.recv())
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for first result"))??
        .detach();
    let first: JobResultMessage = serde_json::from_slice(first.payload().unwrap_or(&[]))?;
    assert_eq!(first.status, "PROCESSING");

    let second = tokio::time::timeout(Duration::from_secs(20), result_consumer.recv())
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for final result"))??
        .detach();
    let second: JobResultMessage = serde_json::from_slice(second.payload().unwrap_or(&[]))?;
    assert_eq!(second.job_id, "job-1");
    assert_eq!(second.status, "done");

    // 7. Shutdown the coordinator.
    shutdown.notify_waiters();
    coordinator_handle.await?;

    Ok(())
}
