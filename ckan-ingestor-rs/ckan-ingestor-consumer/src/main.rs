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

mod coordinator_consumer_context;
mod job_processor;
mod message_source;
mod messages;
mod result_publisher;
mod worker;
mod worker_coordinator;
mod worker_thread;

use anyhow::Result;
use ckan_ingestor_lib::config::S3Settings;
use ckan_ingestor_lib::s3_document_ingestor::S3DocumentIngestor;
use log::{info, warn};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::FutureProducer;
use std::env;
use std::sync::Arc;
use tokio::sync::{Notify, mpsc};

use crate::coordinator_consumer_context::CoordinatorConsumerContext;
use crate::job_processor::RealJobProcessor;
use crate::worker_coordinator::WorkerCoordinator;

const RESULT_TOPIC: &str = "ckan.ingest.jobs_result";

#[tokio::main]
async fn main() -> Result<()> {
    std::panic::set_hook(Box::new(|info| {
        eprintln!("{}", info);
        std::process::abort();
    }));

    eprintln!("ckan-ingestor-consumer starting...");
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

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
    let shutdown = Arc::new(Notify::new());
    {
        let shutdown = shutdown.clone();
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

    let s3 = create_s3_ingestor().await?;

    // Rebalance channel: context sends commands, coordinator receives them.
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let context = CoordinatorConsumerContext::new(cmd_tx);

    let consumer: StreamConsumer<CoordinatorConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", &group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("max.poll.interval.ms", "1800000")
        .set("session.timeout.ms", "45000")
        .set("heartbeat.interval.ms", "15000")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .create_with_context(context)?;
    let consumer = Arc::new(consumer);
    consumer.subscribe(&[&topic, &retry_topic])?;

    let processor = RealJobProcessor::new(s3);
    let mut coordinator = WorkerCoordinator::new(consumer, processor);

    info!("CKAN Ingestor Consumer started");
    coordinator.run(cmd_rx, producer, shutdown.clone()).await;

    info!("CKAN Ingestor Consumer stopped");
    Ok(())
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
