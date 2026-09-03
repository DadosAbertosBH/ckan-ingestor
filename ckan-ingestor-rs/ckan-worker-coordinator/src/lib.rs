// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

pub mod job_planner;
pub mod job_publisher;
pub mod job_repository;
pub mod metadata_processor;
pub mod metadata_publisher;
pub mod metadata_worker_thread;
pub mod mysql_job_repository;

use anyhow::{Context, Result};
use ckan_ingestor_lib::duckdb_factory::DuckdbFactory;
use iggy::prelude::{
    AutoCommit, Client, CompressionAlgorithm, DirectConfig, IggyClient, IggyDuration, IggyExpiry,
    MaxTopicSize, PollingStrategy, StreamClient, TopicClient,
};
use log::{info, warn};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

use crate::job_planner::JobPlanner;
use crate::job_publisher::JobPublisher;
use crate::metadata_processor::RealMetadataProcessor;
use crate::metadata_publisher::MetadataPublisher;
use crate::metadata_worker_thread::MetadataHandler;
use crate::mysql_job_repository::MySqlJobRepository;
use ckan_ingestor_worker_lib::{ConsumerWorker, IggySource};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IggySettings {
    pub address: String,
    pub username: String,
    pub password: String,
    pub stream: String,
    pub job_topic: String,
    pub retry_topic: String,
    pub result_topic: String,
    pub metadata_sync_topic: String,
    pub metadata_sync_result_topic: String,
    pub metadata_consumer_group: String,
    pub partitions: u32,
}

impl Default for IggySettings {
    fn default() -> Self {
        Self {
            address: "localhost:8090".into(),
            username: "iggy".into(),
            password: "iggy".into(),
            stream: "ckan-ingestor".into(),
            job_topic: "jobs".into(),
            retry_topic: "jobs-retry".into(),
            result_topic: "job-results".into(),
            metadata_sync_topic: "ckan_metadata_sync".into(),
            metadata_sync_result_topic: "ckan_metadata_sync_result".into(),
            metadata_consumer_group: "ckan-metadata-sync-worker".into(),
            partitions: 10,
        }
    }
}

impl IggySettings {
    pub fn connection_string(&self) -> String {
        format!(
            "iggy://{}:{}@{}",
            self.username, self.password, self.address
        )
    }

    pub fn from_env() -> Result<Self> {
        let defaults = Self::default();
        let partitions = env::var("IGGY_PARTITIONS")
            .ok()
            .map(|value| value.parse::<u32>())
            .transpose()
            .context("IGGY_PARTITIONS must be a positive integer")?
            .unwrap_or(defaults.partitions);
        anyhow::ensure!(partitions > 0, "IGGY_PARTITIONS must be greater than zero");
        Ok(Self {
            address: env::var("IGGY_ADDRESS").unwrap_or(defaults.address),
            username: env::var("IGGY_USERNAME").unwrap_or(defaults.username),
            password: env::var("IGGY_PASSWORD").unwrap_or(defaults.password),
            stream: env::var("IGGY_STREAM").unwrap_or(defaults.stream),
            job_topic: env::var("IGGY_TOPIC").unwrap_or(defaults.job_topic),
            retry_topic: env::var("IGGY_TOPIC_RETRY").unwrap_or(defaults.retry_topic),
            result_topic: env::var("IGGY_TOPIC_RESULTS").unwrap_or(defaults.result_topic),
            metadata_sync_topic: env::var("IGGY_METADATA_SYNC_TOPIC")
                .unwrap_or(defaults.metadata_sync_topic),
            metadata_sync_result_topic: env::var("IGGY_METADATA_SYNC_RESULT_TOPIC")
                .unwrap_or(defaults.metadata_sync_result_topic),
            metadata_consumer_group: env::var("IGGY_METADATA_SYNC_GROUP_ID")
                .unwrap_or(defaults.metadata_consumer_group),
            partitions,
        })
    }
}

pub async fn run() -> Result<()> {
    let settings = IggySettings::from_env()?;
    let connection_string = settings.connection_string();
    let admin = connected_client(&connection_string).await?;
    ensure_topology(&admin, &settings).await?;
    let producer = admin
        .producer(&settings.stream, &settings.metadata_sync_result_topic)?
        .direct(DirectConfig::builder().batch_length(1).build())
        .build();
    producer.init().await?;
    let result_producer = admin
        .producer(&settings.stream, &settings.result_topic)?
        .direct(DirectConfig::builder().batch_length(1).build())
        .build();
    result_producer.init().await?;
    let job_producer = admin
        .producer(&settings.stream, &settings.job_topic)?
        .direct(DirectConfig::builder().batch_length(1).build())
        .build();
    job_producer.init().await?;
    let retry_producer = admin
        .producer(&settings.stream, &settings.retry_topic)?
        .direct(DirectConfig::builder().batch_length(1).build())
        .build();
    retry_producer.init().await?;
    let client = connected_client(&connection_string).await?;
    let mut consumer = client
        .consumer_group(
            &settings.metadata_consumer_group,
            &settings.stream,
            &settings.metadata_sync_topic,
        )?
        .auto_commit(AutoCommit::Disabled)
        .create_consumer_group_if_not_exists()
        .auto_join_consumer_group()
        .polling_strategy(PollingStrategy::next())
        .poll_interval(IggyDuration::from(Duration::from_millis(10)))
        .batch_length(1)
        .build();
    consumer.init().await?;
    let processor = RealMetadataProcessor::new(DuckdbFactory::from_env())?;
    let mut worker = ConsumerWorker::new(
        settings.metadata_sync_topic.clone(),
        0,
        IggySource::new(consumer),
        MetadataHandler::new(
            MetadataPublisher::new(producer),
            JobPublisher::new(result_producer, job_producer, retry_producer),
            JobPlanner::new(Box::new(MySqlJobRepository::from_env()?)),
            processor,
        ),
    );
    worker.run();
    let shutdown = Arc::new(Notify::new());
    install_shutdown_handler(shutdown.clone());
    info!("CKAN worker coordinator started");
    shutdown.notified().await;
    tokio::task::spawn_blocking(move || worker.shutdown()).await?;
    info!("CKAN worker coordinator stopped");
    Ok(())
}

async fn connected_client(connection_string: &str) -> Result<IggyClient> {
    let client = IggyClient::from_connection_string(connection_string)?;
    client.connect().await?;
    Ok(client)
}

pub async fn ensure_topology(client: &IggyClient, settings: &IggySettings) -> Result<()> {
    let stream = settings.stream.as_str().try_into()?;
    if client.get_stream(&stream).await?.is_none()
        && let Err(error) = client.create_stream(&settings.stream).await
    {
        if client.get_stream(&stream).await?.is_none() {
            return Err(error.into());
        }
        warn!("Iggy stream was created concurrently: {error}");
    }
    for (topic_name, partitions) in [
        (&settings.job_topic, settings.partitions),
        (&settings.retry_topic, settings.partitions),
        (&settings.result_topic, settings.partitions),
        (&settings.metadata_sync_topic, 1),
        (&settings.metadata_sync_result_topic, 1),
    ] {
        let topic = topic_name.as_str().try_into()?;
        if client.get_topic(&stream, &topic).await?.is_none()
            && let Err(error) = client
                .create_topic(
                    &stream,
                    topic_name,
                    partitions,
                    CompressionAlgorithm::None,
                    Some(1),
                    IggyExpiry::NeverExpire,
                    MaxTopicSize::ServerDefault,
                )
                .await
        {
            if client.get_topic(&stream, &topic).await?.is_none() {
                return Err(error.into());
            }
            warn!("Iggy topic was created concurrently: {error}");
        }
    }
    Ok(())
}

fn install_shutdown_handler(shutdown: Arc<Notify>) {
    tokio::spawn(async move {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM handler");
        let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            .expect("failed to register SIGINT handler");
        tokio::select! { _ = sigterm.recv() => {}, _ = sigint.recv() => {} }
        shutdown.notify_waiters();
    });
}
