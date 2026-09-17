// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

pub mod data_writer;
pub mod ducklake_data_writer;
pub mod metadata_message_processor;
pub mod metadata_processor;
pub mod parquet_message_processor;
pub mod parquet_registrar;

use anyhow::{Context, Result};
use iggy_processor::iggy::prelude::{
    AutoCommit, Client, CompressionAlgorithm, DirectConfig, IggyClient, IggyDuration, IggyExpiry,
    MaxTopicSize, PollingStrategy, StreamClient, TopicClient,
};
use log::{info, warn};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

use crate::ducklake_data_writer::DucklakeDataWriter;
use crate::metadata_message_processor::MetadataProcessor;
use crate::metadata_processor::RealMetadataProcessor;
use crate::parquet_message_processor::ParquetProcessor;
use crate::parquet_registrar::ParquetRegistrar;
use ckan_ingestor_lib::ducklake_factory::DucklakeFactory;
use iggy_processor::{IggyPublisher, IggySource};
use message_processor::WorkerHandler;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IggySettings {
    pub address: String,
    pub username: String,
    pub password: String,
    pub stream: String,
    pub job_topic: String,
    pub retry_topic: String,
    pub result_topic: String,
    pub parquet_result_topic: String,
    pub parquet_consumer_group: String,
    pub metadata_sync_topic: String,
    pub metadata_sync_result_topic: String,
    pub metadata_consumer_group: String,
    pub job_partitions: u32,
    pub retry_partitions: u32,
    pub result_partitions: u32,
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
            parquet_result_topic: "parquet-results".into(),
            parquet_consumer_group: "ducklake-writer".into(),
            metadata_sync_topic: "ckan_metadata_sync".into(),
            metadata_sync_result_topic: "ckan_metadata_sync_result".into(),
            metadata_consumer_group: "ckan-metadata-sync-worker".into(),
            job_partitions: 10,
            retry_partitions: 10,
            result_partitions: 1,
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
        let job_partitions = env::var("IGGY_JOB_PARTITIONS")
            .ok()
            .map(|value| value.parse::<u32>())
            .transpose()
            .context("IGGY_JOB_PARTITIONS must be a positive integer")?
            .unwrap_or(defaults.job_partitions);
        anyhow::ensure!(
            job_partitions > 0,
            "IGGY_JOB_PARTITIONS must be greater than zero"
        );
        let retry_partitions = env::var("IGGY_RETRY_PARTITIONS")
            .ok()
            .map(|value| value.parse::<u32>())
            .transpose()
            .context("IGGY_RETRY_PARTITIONS must be a positive integer")?
            .unwrap_or(defaults.retry_partitions);
        anyhow::ensure!(
            retry_partitions > 0,
            "IGGY_RETRY_PARTITIONS must be greater than zero"
        );
        let result_partitions = env::var("IGGY_RESULT_PARTITIONS")
            .ok()
            .map(|value| value.parse::<u32>())
            .transpose()
            .context("IGGY_RESULT_PARTITIONS must be a positive integer")?
            .unwrap_or(defaults.result_partitions);
        anyhow::ensure!(
            result_partitions > 0,
            "IGGY_RESULT_PARTITIONS must be greater than zero"
        );
        Ok(Self {
            address: env::var("IGGY_ADDRESS").unwrap_or(defaults.address),
            username: env::var("IGGY_USERNAME").unwrap_or(defaults.username),
            password: env::var("IGGY_PASSWORD").unwrap_or(defaults.password),
            stream: env::var("IGGY_STREAM").unwrap_or(defaults.stream),
            job_topic: env::var("IGGY_TOPIC").unwrap_or(defaults.job_topic),
            retry_topic: env::var("IGGY_TOPIC_RETRY").unwrap_or(defaults.retry_topic),
            result_topic: env::var("IGGY_TOPIC_RESULTS").unwrap_or(defaults.result_topic),
            parquet_result_topic: env::var("IGGY_PARQUET_RESULT_TOPIC")
                .unwrap_or(defaults.parquet_result_topic),
            parquet_consumer_group: env::var("IGGY_PARQUET_GROUP_ID")
                .unwrap_or(defaults.parquet_consumer_group),
            metadata_sync_topic: env::var("IGGY_METADATA_SYNC_TOPIC")
                .unwrap_or(defaults.metadata_sync_topic),
            metadata_sync_result_topic: env::var("IGGY_METADATA_SYNC_RESULT_TOPIC")
                .unwrap_or(defaults.metadata_sync_result_topic),
            metadata_consumer_group: env::var("IGGY_METADATA_SYNC_GROUP_ID")
                .unwrap_or(defaults.metadata_consumer_group),
            job_partitions,
            retry_partitions,
            result_partitions,
        })
    }
}

pub async fn run() -> Result<()> {
    let settings = IggySettings::from_env()?;
    let connection_string = settings.connection_string();
    let admin = connected_client(&connection_string).await?;
    ensure_topology(&admin, &settings).await?;

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
    let parquet_client = connected_client(&connection_string).await?;
    let mut parquet_consumer = parquet_client
        .consumer_group(
            &settings.parquet_consumer_group,
            &settings.stream,
            &settings.parquet_result_topic,
        )?
        .auto_commit(AutoCommit::Disabled)
        .create_consumer_group_if_not_exists()
        .auto_join_consumer_group()
        .polling_strategy(PollingStrategy::next())
        .poll_interval(IggyDuration::from(Duration::from_millis(10)))
        .batch_length(1)
        .build();
    parquet_consumer.init().await?;
    let factory = DucklakeFactory::from_env()?;
    factory.initialize().await?;
    let registrar = ParquetRegistrar::new(factory.clone());
    registrar.initialize().await?;
    let metadata_writer =
        DucklakeDataWriter::new(factory.client().await?, factory.storage_options().to_vec());
    let processor = RealMetadataProcessor::new(factory.clone(), metadata_writer);
    let publisher = create_publisher(admin, &settings).await?;
    let mut metadata_consumer = WorkerHandler::new(
        IggySource::new(consumer),
        publisher.clone(),
        MetadataProcessor::new(
            settings.job_topic.clone(),
            settings.retry_topic.clone(),
            settings.result_topic.clone(),
            settings.metadata_sync_result_topic.clone(),
            processor,
        ),
    );
    metadata_consumer.run();
    let mut parquet_consumer = WorkerHandler::new(
        IggySource::new(parquet_consumer),
        publisher.clone(),
        parquet_processor(&settings, registrar),
    );
    parquet_consumer.run();
    let shutdown = Arc::new(Notify::new());
    install_shutdown_handler(shutdown.clone());
    info!("CKAN worker coordinator started");
    shutdown.notified().await;
    tokio::task::spawn_blocking(move || metadata_consumer.shutdown()).await?;
    tokio::task::spawn_blocking(move || parquet_consumer.shutdown()).await?;
    info!("CKAN worker coordinator stopped");
    Ok(())
}

fn parquet_processor(settings: &IggySettings, registrar: ParquetRegistrar) -> ParquetProcessor {
    ParquetProcessor::new(settings.result_topic.clone(), registrar)
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
        (&settings.job_topic, settings.job_partitions),
        (&settings.retry_topic, settings.retry_partitions),
        (&settings.result_topic, settings.result_partitions),
        (&settings.parquet_result_topic, 1),
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

async fn create_publisher(admin: IggyClient, settings: &IggySettings) -> Result<IggyPublisher> {
    let sync_producer = admin
        .producer(&settings.stream, &settings.metadata_sync_result_topic)?
        .direct(DirectConfig::builder().batch_length(100).build())
        .build();
    sync_producer.init().await?;
    let result_producer = admin
        .producer(&settings.stream, &settings.result_topic)?
        .direct(DirectConfig::builder().batch_length(100).build())
        .build();
    result_producer.init().await?;
    let job_producer = admin
        .producer(&settings.stream, &settings.job_topic)?
        .direct(DirectConfig::builder().batch_length(100).build())
        .build();
    job_producer.init().await?;
    let retry_producer = admin
        .producer(&settings.stream, &settings.retry_topic)?
        .direct(DirectConfig::builder().batch_length(100).build())
        .build();
    retry_producer.init().await?;

    Ok(IggyPublisher::new(
        vec![sync_producer, result_producer, job_producer, retry_producer].into_iter(),
    ))
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

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use tempfile::tempdir;

    use super::*;
    use ckan_ingestor_worker_lib::{JobResultMessage, JobStatus};
    use message_processor::MessageProcessor;

    #[tokio::test]
    async fn parquet_results_are_forwarded_to_the_terminal_result_topic() {
        let temp = tempdir().unwrap();
        let settings = IggySettings {
            result_topic: "terminal-results".into(),
            parquet_result_topic: "internal-parquet-results".into(),
            ..IggySettings::default()
        };
        let registrar = ParquetRegistrar::new(DucklakeFactory::for_sqlite(
            &temp.path().join("catalog.sqlite"),
            &temp.path().join("data"),
        ));
        let processor = parquet_processor(&settings, registrar);
        let mut result = JobResultMessage::pending("job", "resource", "dataset", "instance");
        result.status = JobStatus::Failed;

        let messages = processor.process(result).collect::<Vec<_>>().await;

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].topic, "terminal-results");
    }
}
