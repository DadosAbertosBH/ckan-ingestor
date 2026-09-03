// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use anyhow::Context;
use ckan_worker_coordinator::job_planner::MysqlJobPlanner;
use ckan_worker_coordinator::metadata_processor::ResourceCandidate;
use ckan_worker_coordinator::{IggySettings, ensure_topology};
use iggy::prelude::{Client, IggyClient, TopicClient};
use mysql::prelude::Queryable;
use mysql::{OptsBuilder, Pool};
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

async fn start_mysql() -> anyhow::Result<ContainerAsync<GenericImage>> {
    Ok(GenericImage::new("mysql", "8.0")
        .with_wait_for(WaitFor::message_on_either_std("ready for connections"))
        .with_cmd(["--default-authentication-plugin=mysql_native_password"])
        .with_mapped_port(0, 3306.tcp())
        .with_env_var("MYSQL_ROOT_PASSWORD", "coordinator")
        .with_env_var("MYSQL_ROOT_HOST", "%")
        .with_env_var("MYSQL_DATABASE", "coordinator_test")
        .start()
        .await?)
}

fn candidate(id: &str) -> ResourceCandidate {
    ResourceCandidate {
        resource_id: id.into(),
        resource_name: None,
        resource_url: None,
        resource_format: None,
        dataset_name: "dataset".into(),
        datastore_active: false,
    }
}

fn mysql_options(host: &str, port: u16) -> OptsBuilder {
    OptsBuilder::new()
        .ip_or_hostname(Some(host))
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some("coordinator"))
        .db_name(Some("coordinator_test"))
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn coordinator_plans_jobs_from_mysql_state() -> anyhow::Result<()> {
    let container = timeout(Duration::from_secs(60), start_mysql())
        .await
        .context("timed out while starting the MySQL container")??;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(3306.tcp()).await?;
    let host = host.to_string();
    let mut initial_connection = None;
    for _ in 0..30 {
        if let Ok(pool) = Pool::new(mysql_options(&host, port))
            && let Ok(connection) = pool.get_conn()
        {
            initial_connection = Some((pool, connection));
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let (pool, mut connection) =
        initial_connection.context("MySQL did not accept connections within 30 seconds")?;
    connection.query_drop(
        "CREATE TABLE ckan_data_job (id VARCHAR(36) PRIMARY KEY, idempotency_key VARCHAR(255), resource_id VARCHAR(255), status VARCHAR(32));\
         CREATE TABLE latest_resource_job (resource_id VARCHAR(255), latest_job_id VARCHAR(36));\
         INSERT INTO ckan_data_job VALUES \
             ('in-flight-job', 'in-flight', 'in-flight', 'pending'),\
             ('failed-job', 'failed', 'failed', 'failed');\
         INSERT INTO latest_resource_job VALUES ('failed', 'failed-job')",
    )?;

    let planned = MysqlJobPlanner::from_pool(pool).classify(vec![
        candidate("new"),
        candidate("in-flight"),
        candidate("failed"),
    ])?;

    assert_eq!(
        planned
            .iter()
            .map(|(resource, enqueue, retry)| (resource.resource_id.as_str(), *enqueue, *retry))
            .collect::<Vec<_>>(),
        vec![
            ("new", true, false),
            ("in-flight", false, false),
            ("failed", true, true),
        ]
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn coordinator_creates_the_complete_approved_topology() -> anyhow::Result<()> {
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

    let stream = settings.stream.as_str().try_into()?;
    for topic_name in [
        settings.job_topic,
        settings.retry_topic,
        settings.result_topic,
        settings.metadata_sync_topic,
        settings.metadata_sync_result_topic,
    ] {
        let topic = topic_name.as_str().try_into()?;
        assert!(client.get_topic(&stream, &topic).await?.is_some());
    }
    Ok(())
}
