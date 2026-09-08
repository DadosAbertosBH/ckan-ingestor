// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

pub mod job_processor;
pub mod messages;
pub mod parquet_uploader;

use anyhow::{Context, Result};
use ckan_ingestor_lib::config::S3Settings;
use ckan_ingestor_lib::s3_document_ingestor::S3DocumentIngestor;
use iggy_processor::iggy::prelude::{
    AutoCommit, Client, CompressionAlgorithm, DirectConfig, IggyClient, IggyDuration, IggyExpiry,
    MaxTopicSize, PollingStrategy, StreamClient, TopicClient,
};
use log::{info, warn};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

use crate::job_processor::JobProcessor;
use crate::parquet_uploader::ParquetUploader;
use iggy_processor::{IggyResultPublisher, IggySource};
use message_processor::ConsumerWorker;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IggySettings {
    pub address: String,
    pub username: String,
    pub password: String,
    pub stream: String,
    pub source_topic: String,
    pub retry_topic: String,
    pub result_topic: String,
    pub consumer_group: String,
    pub partitions: u32,
}

impl Default for IggySettings {
    fn default() -> Self {
        Self {
            address: "localhost:8090".into(),
            username: "iggy".into(),
            password: "iggy".into(),
            stream: "ckan-ingestor".into(),
            source_topic: "jobs".into(),
            retry_topic: "jobs-retry".into(),
            result_topic: "parquet-results".into(),
            consumer_group: "ckan-worker".into(),
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
            source_topic: env::var("IGGY_TOPIC").unwrap_or(defaults.source_topic),
            retry_topic: env::var("IGGY_TOPIC_RETRY").unwrap_or(defaults.retry_topic),
            result_topic: env::var("IGGY_PARQUET_RESULT_TOPIC").unwrap_or(defaults.result_topic),
            consumer_group: env::var("IGGY_GROUP_ID").unwrap_or(defaults.consumer_group),
            partitions,
        })
    }
}

pub async fn run() -> Result<()> {
    let settings = IggySettings::from_env()?;
    info!(
        "Worker starting with Iggy stream={}, partitions={}",
        settings.stream, settings.partitions
    );

    let connection_string = settings.connection_string();
    let admin = connected_client(&connection_string).await?;
    ensure_topology(&admin, &settings).await?;

    let result_producer = admin
        .producer(&settings.stream, &settings.result_topic)?
        .direct(DirectConfig::builder().batch_length(1).build())
        .build();
    result_producer.init().await?;
    let publisher = IggyResultPublisher::new(result_producer);

    let s3 = create_s3_ingestor().await?;
    let processor = JobProcessor::new(s3, ParquetUploader::new(S3Settings::from_env()))?;
    let mut workers = Vec::with_capacity((settings.partitions * 2) as usize);

    for topic in [&settings.source_topic, &settings.retry_topic] {
        for slot in 0..settings.partitions {
            let client = connected_client(&connection_string).await?;
            let mut consumer = client
                .consumer_group(&settings.consumer_group, &settings.stream, topic)?
                .auto_commit(AutoCommit::Disabled)
                .create_consumer_group_if_not_exists()
                .auto_join_consumer_group()
                .polling_strategy(PollingStrategy::next())
                .poll_interval(IggyDuration::from(Duration::from_millis(10)))
                .batch_length(1)
                .build();
            consumer.init().await?;
            let source = IggySource::new(consumer);
            let mut worker = ConsumerWorker::new(
                topic.clone(),
                slot as usize,
                source,
                publisher.clone(),
                processor.clone(),
            );
            worker.run();
            workers.push(worker);
        }
    }

    let shutdown = Arc::new(Notify::new());
    install_shutdown_handler(shutdown.clone());
    info!("CKAN Iggy consumer started with {} workers", workers.len());
    shutdown.notified().await;

    for worker in workers {
        tokio::task::spawn_blocking(move || worker.shutdown()).await?;
    }
    info!("CKAN Iggy consumer stopped");
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
        (&settings.source_topic, settings.partitions),
        (&settings.retry_topic, settings.partitions),
        (&settings.result_topic, settings.partitions),
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
        tokio::select! {
            _ = sigterm.recv() => {},
            _ = sigint.recv() => {},
        }
        shutdown.notify_waiters();
    });
}

async fn create_s3_ingestor() -> Result<S3DocumentIngestor> {
    S3DocumentIngestor::new(S3Settings::from_env())
}
