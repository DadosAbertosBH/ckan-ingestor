// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.

//! Real SDK probe used by tests/test_iggy_session_recovery.py.
use std::{env, str::FromStr};

use anyhow::{Result, anyhow};
use iggy_processor::{IggySource, iggy::prelude::*};
use message_processor::{
    BrokerMessage, MessageProcessor, MessagePublisher, MessageSource, OutgoingMessage,
    WorkerHandler,
};
use serde::Serialize;

struct ReportingSource {
    source: IggySource,
    payload: String,
}

impl MessageSource for ReportingSource {
    async fn recv(&mut self) -> Result<BrokerMessage> {
        let message = self.source.recv().await.inspect_err(|error| {
            eprintln!("consumer poll error, retrying through WorkerHandler: {error}");
        })?;
        self.payload = serde_json::from_slice(&message.payload)?;
        Ok(message)
    }

    async fn commit(&mut self, partition: u32, offset: u64) -> Result<()> {
        self.source.commit(partition, offset).await?;
        println!(
            "{}",
            serde_json::json!({"event":"committed", "offset":offset, "payload":self.payload})
        );
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.source.shutdown().await?;
        println!("{}", serde_json::json!({"event":"left"}));
        Ok(())
    }
}

struct Processor;

impl MessageProcessor for Processor {
    type IncomingMessage = String;
    fn process(&self, _: String) -> impl futures::Stream<Item = OutgoingMessage<impl Serialize>> {
        futures::stream::empty::<OutgoingMessage<String>>()
    }
}

struct Publisher;

impl MessagePublisher for Publisher {
    async fn publish(&self, _: OutgoingMessage<impl Serialize>) -> Result<()> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let mode = args
        .get(1)
        .ok_or_else(|| anyhow!("expected setup, send or consume"))?;
    let address = env::var("IGGY_PROBE_ADDRESS")?;
    let client =
        iggy_processor::session_client(&format!("iggy://iggy:session-test-password@{address}"))?;
    client.connect().await?;
    let stream = "session-recovery".try_into()?;
    if mode == "setup" {
        client.create_stream("session-recovery").await?;
        client
            .create_topic(
                &stream,
                "results",
                &TopicCreateOptions {
                    partitions_count: Some(1),
                    ..TopicCreateOptions::default()
                },
            )
            .await?;
        println!("{}", serde_json::json!({"event":"setup"}));
    } else if mode == "state" {
        let topic = "results".try_into()?;
        let group = "coordinator".try_into()?;
        let state = client.get_consumer_group(&stream, &topic, &group).await?;
        println!("{}", serde_json::json!({"event":"state", "group": state}));
    } else if mode == "send" {
        let producer = client
            .producer("session-recovery", "results")?
            .direct(DirectConfig::builder().batch_length(1).build())
            .build();
        producer.init().await?;
        producer
            .send(vec![IggyMessage::from_str(&serde_json::to_string(
                &args[2],
            )?)?])
            .await?;
        producer.shutdown().await;
        println!("{}", serde_json::json!({"event":"sent"}));
    } else {
        let mut consumer = client
            .consumer_group("coordinator", "session-recovery", "results")?
            .auto_commit(AutoCommit::Disabled)
            .create_consumer_group_if_not_exists()
            .auto_join_consumer_group()
            .polling_strategy(PollingStrategy::next())
            .batch_length(1)
            .build();
        consumer.init().await?;
        let source = ReportingSource {
            source: IggySource::new(client, consumer),
            payload: String::new(),
        };
        let mut worker = WorkerHandler::new(source, Publisher, Processor);
        worker.run();
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        println!("{}", serde_json::json!({"event":"joined"}));
        terminate.recv().await;
        tokio::task::spawn_blocking(move || worker.shutdown()).await?;
    }
    Ok(())
}
