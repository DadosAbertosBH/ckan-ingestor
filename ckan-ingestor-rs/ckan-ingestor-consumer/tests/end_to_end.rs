// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use ckan_ingestor_consumer::{IggySettings, ensure_topology};
use iggy::prelude::{Client, IggyClient, StreamClient, TopicClient};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

async fn start_iggy() -> anyhow::Result<ContainerAsync<GenericImage>> {
    Ok(GenericImage::new("apache/iggy", "0.8.0")
        .with_mapped_port(18090, 8090.tcp())
        .with_security_opt("seccomp=unconfined")
        .with_env_var("IGGY_ROOT_USERNAME", "iggy")
        .with_env_var("IGGY_ROOT_PASSWORD", "iggy")
        .with_env_var("IGGY_TCP_ADDRESS", "0.0.0.0:8090")
        .with_ready_conditions(vec![WaitFor::seconds(5)])
        .start()
        .await?)
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn creates_the_approved_stream_and_topics() -> anyhow::Result<()> {
    let container = start_iggy().await?;
    let host = container.get_host().await?;
    let port = 18090;
    let connection_string = format!("iggy://iggy:iggy@{host}:{port}");
    let client = IggyClient::from_connection_string(&connection_string)?;
    client.connect().await?;
    let settings = IggySettings {
        address: format!("{host}:{port}"),
        ..IggySettings::default()
    };

    ensure_topology(&client, &settings).await?;

    let stream_id = settings.stream.as_str().try_into()?;
    assert!(client.get_stream(&stream_id).await?.is_some());
    for topic in [
        settings.job_topic,
        settings.retry_topic,
        settings.result_topic,
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
