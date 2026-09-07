// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use anyhow::Context;
use ckan_ingestor_consumer::{IggySettings, ensure_topology};
use iggy::prelude::{Client, IggyClient, StreamClient, TopicClient};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::{Duration, timeout};

async fn start_iggy() -> anyhow::Result<ContainerAsync<GenericImage>> {
    Ok(GenericImage::new("apache/iggy", "0.8.0")
        .with_wait_for(WaitFor::message_on_stdout(
            "Iggy TCP server has started on: 0.0.0.0:8090",
        ))
        .with_mapped_port(0, 8090.tcp())
        .with_security_opt("seccomp=unconfined")
        .with_cap_add("IPC_LOCK")
        .with_cap_add("SYS_NICE")
        .with_ulimit("memlock", -1, Some(-1))
        .with_env_var("IGGY_ROOT_USERNAME", "iggy")
        .with_env_var("IGGY_ROOT_PASSWORD", "iggy")
        .with_env_var("IGGY_TCP_ADDRESS", "0.0.0.0:8090")
        .start()
        .await?)
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn creates_the_approved_stream_and_topics() -> anyhow::Result<()> {
    let container = timeout(Duration::from_secs(30), start_iggy())
        .await
        .context("timed out while starting the Iggy container")??;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(8090.tcp()).await?;
    let connection_string = format!("iggy://iggy:iggy@{host}:{port}");
    let client = IggyClient::from_connection_string(&connection_string)?;
    timeout(Duration::from_secs(10), client.connect())
        .await
        .context("timed out while connecting to Iggy")??;
    let settings = IggySettings {
        address: format!("{host}:{port}"),
        ..IggySettings::default()
    };

    timeout(Duration::from_secs(10), ensure_topology(&client, &settings))
        .await
        .context("timed out while creating the Iggy test topology")??;

    let stream_id = settings.stream.as_str().try_into()?;
    assert!(client.get_stream(&stream_id).await?.is_some());
    for topic in [
        settings.job_topic,
        settings.retry_topic,
        settings.parquet_result_topic,
    ] {
        let topic_id = topic.as_str().try_into()?;
        let details = client
            .get_topic(&stream_id, &topic_id)
            .await?
            .expect("topic should exist");
        assert_eq!(details.partitions_count, settings.partitions);
    }
    Ok(())
}
