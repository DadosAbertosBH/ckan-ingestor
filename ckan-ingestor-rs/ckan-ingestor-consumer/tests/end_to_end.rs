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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use ckan_ingestor_consumer::coordinator_consumer_context::CoordinatorConsumerContext;
use ckan_ingestor_consumer::job_processor::JobProcessor;
use ckan_ingestor_consumer::messages::{JobMessage, JobResultMessage, JobStatus};
use ckan_ingestor_consumer::result_publisher::ResultPublisher;
use ckan_ingestor_consumer::worker_coordinator::{Command, WorkerCoordinator};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::{Notify, mpsc};

const JOB_TOPIC: &str = "ckan.ingest.jobs";
const RETRY_TOPIC: &str = "ckan.ingest.jobs.retry";

async fn start_kafka() -> anyhow::Result<ContainerAsync<GenericImage>> {
    Ok(GenericImage::new("apache/kafka-native", "4.2.1")
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
        .await?)
}

#[derive(Clone)]
struct StubProcessor;

impl JobProcessor for StubProcessor {
    fn process(&self, job: JobMessage) -> JobResultMessage {
        JobResultMessage {
            reader: String::new(),
            job_id: job.job_id,
            status: JobStatus::Success,
            rows_processed: Some(1),
            expected_rows: None,
            encoding: None,
            expected_columns: None,
            datastore_active: false,
            error_message: None,
            preview: None,
        }
    }
}

#[derive(Clone)]
struct CountingProcessor {
    processed: Arc<AtomicUsize>,
}

impl JobProcessor for CountingProcessor {
    fn process(&self, job: JobMessage) -> JobResultMessage {
        self.processed.fetch_add(1, Ordering::SeqCst);
        JobResultMessage {
            reader: String::new(),
            job_id: job.job_id,
            status: JobStatus::Success,
            rows_processed: Some(1),
            expected_rows: None,
            encoding: None,
            expected_columns: None,
            datastore_active: false,
            error_message: None,
            preview: None,
        }
    }
}

#[derive(Clone)]
struct NoopPublisher;

impl ResultPublisher for NoopPublisher {
    fn publish(
        &self,
        _result: JobResultMessage,
    ) -> impl std::future::Future<Output = Result<(), anyhow::Error>> + Send {
        std::future::ready(Ok(()))
    }
}

#[tokio::test]
#[ignore = "requires a Docker daemon to run Kafka via testcontainers"]
async fn end_to_end_message_flow() -> anyhow::Result<()> {
    // 1. Start a single-node Kafka broker (KRaft mode).
    let _kafka = start_kafka().await?;

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

    let mut coordinator = WorkerCoordinator::new(consumer, StubProcessor, cmd_rx);
    let shutdown = Arc::new(Notify::new());

    let producer_for_coordinator = producer.clone();
    let shutdown_for_coordinator = shutdown.clone();
    let coordinator_handle = tokio::spawn(async move {
        coordinator
            .run(producer_for_coordinator, shutdown_for_coordinator)
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
    assert_eq!(first.status, JobStatus::Processing);

    let second = tokio::time::timeout(Duration::from_secs(20), result_consumer.recv())
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for final result"))??
        .detach();
    let second: JobResultMessage = serde_json::from_slice(second.payload().unwrap_or(&[]))?;
    assert_eq!(second.job_id, "job-1");
    assert_eq!(second.status, JobStatus::Success);

    // 7. Shutdown the coordinator.
    shutdown.notify_waiters();
    coordinator_handle.await?;

    Ok(())
}

#[tokio::test]
#[ignore = "requires a Docker daemon to run Kafka via testcontainers"]
async fn restart_processes_messages_waiting_before_partition_queues_are_split() -> anyhow::Result<()>
{
    let _kafka = start_kafka().await?;

    let bootstrap = "localhost:9092";
    let group_id = "restart-before-split-worker";
    ckan_ingestor_consumer::ensure_topics(bootstrap, JOB_TOPIC, RETRY_TOPIC).await;

    // Let the first incarnation join the group and then stop it. Messages
    // published next will already be waiting when the replacement rejoins.
    let (first_cmd_tx, first_cmd_rx) = mpsc::unbounded_channel();
    let first_context = CoordinatorConsumerContext::new(first_cmd_tx);
    let first_consumer: StreamConsumer<CoordinatorConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .create_with_context(first_context)?;
    let first_consumer = Arc::new(first_consumer);
    first_consumer.subscribe(&[JOB_TOPIC, RETRY_TOPIC])?;

    let mut first_coordinator = WorkerCoordinator::new(first_consumer, StubProcessor, first_cmd_rx);
    let first_shutdown = Arc::new(Notify::new());
    let first_coordinator_shutdown = first_shutdown.clone();
    let first_handle = tokio::spawn(async move {
        first_coordinator
            .run(NoopPublisher, first_coordinator_shutdown)
            .await;
    });
    tokio::time::sleep(Duration::from_secs(2)).await;
    first_shutdown.notify_waiters();
    tokio::time::timeout(Duration::from_secs(20), first_handle)
        .await
        .map_err(|_| anyhow::anyhow!("initial consumer did not stop cleanly"))??;
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()?;
    let producer = Arc::new(producer);

    const JOB_COUNT: usize = 100;
    for index in 0..JOB_COUNT {
        let job = JobMessage {
            job_id: format!("restart-job-{index}"),
            resource_id: format!("resource-{index}"),
            ckan_url: "http://ckan".to_string(),
            resource_url: String::new(),
            resource_format: String::new(),
        };
        let payload = serde_json::to_vec(&job)?;
        producer
            .send(
                FutureRecord::to(JOB_TOPIC)
                    .key(&job.job_id)
                    .payload(&payload),
                Timeout::After(Duration::from_secs(10)),
            )
            .await
            .map_err(|(error, _)| anyhow::anyhow!("produce failed: {error}"))?;
    }
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let context = CoordinatorConsumerContext::new(cmd_tx);
    let restarted_consumer: StreamConsumer<CoordinatorConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .create_with_context(context)?;
    let restarted_consumer = Arc::new(restarted_consumer);
    restarted_consumer.subscribe(&[JOB_TOPIC, RETRY_TOPIC])?;

    let processed = Arc::new(AtomicUsize::new(0));
    let processor = CountingProcessor {
        processed: processed.clone(),
    };
    let mut coordinator = WorkerCoordinator::new(restarted_consumer, processor, cmd_rx);
    let shutdown = Arc::new(Notify::new());
    let coordinator_shutdown = shutdown.clone();
    let coordinator_handle = tokio::spawn(async move {
        coordinator.run(NoopPublisher, coordinator_shutdown).await;
    });
    let all_jobs_processed = tokio::time::timeout(Duration::from_secs(5), async {
        while processed.load(Ordering::SeqCst) < JOB_COUNT {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    shutdown.notify_waiters();
    coordinator_handle.await?;

    all_jobs_processed.map_err(|_| {
        anyhow::anyhow!(
            "restart processed {} of {JOB_COUNT} waiting jobs",
            processed.load(Ordering::SeqCst)
        )
    })?;
    assert_eq!(processed.load(Ordering::SeqCst), JOB_COUNT);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a Docker daemon to run Kafka via testcontainers"]
async fn subscribe_delivers_assignment_only_after_consumer_polling() -> anyhow::Result<()> {
    let _kafka = start_kafka().await?;

    let bootstrap = "localhost:9092";
    ckan_ingestor_consumer::ensure_topics(bootstrap, JOB_TOPIC, RETRY_TOPIC).await;

    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
    let context = CoordinatorConsumerContext::new(cmd_tx);
    let consumer = Arc::new(
        ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("group.id", "subscribe-without-polling")
            .set("auto.offset.reset", "earliest")
            .set("partition.assignment.strategy", "cooperative-sticky")
            .create_with_context::<_, StreamConsumer<CoordinatorConsumerContext>>(context)?,
    );

    consumer.subscribe(&[JOB_TOPIC, RETRY_TOPIC])?;

    // subscribe() starts group coordination, while the rebalance callback is
    // served through the consumer poll queue.
    assert!(
        tokio::time::timeout(Duration::from_secs(2), cmd_rx.recv())
            .await
            .is_err(),
        "subscribe unexpectedly delivered an assignment without polling"
    );

    let poll_consumer = consumer.clone();
    let poll = tokio::spawn(async move {
        let _ = poll_consumer.recv().await;
    });
    let command = tokio::time::timeout(Duration::from_secs(20), cmd_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("polling did not deliver an assignment"))?
        .ok_or_else(|| anyhow::anyhow!("rebalance command channel closed"))?;

    match command {
        Command::Assign(partitions, done) => {
            assert!(!partitions.is_empty());
            drop(done);
        }
        Command::Revoke(_, done) => {
            drop(done);
            anyhow::bail!("received revoke before the initial assignment");
        }
    }

    cmd_rx.close();
    poll.abort();
    let _ = poll.await;

    Ok(())
}
