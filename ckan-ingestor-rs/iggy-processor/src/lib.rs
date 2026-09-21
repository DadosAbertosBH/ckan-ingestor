// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use std::{collections::HashMap, str::FromStr, sync::Arc, vec::IntoIter};

use anyhow::{Result, anyhow};
use futures::StreamExt;
use iggy::{
    clients::producer::IggyProducer,
    prelude::{IggyClient, IggyConsumer, IggyMessage, Partitioning},
};
use message_processor::{BrokerMessage, MessagePublisher, MessageSource, OutgoingMessage};

pub use iggy;
use serde::Serialize;

struct OwnedConsumer<Client, Consumer> {
    _client: Client,
    consumer: Consumer,
}

impl<Client, Consumer> OwnedConsumer<Client, Consumer> {
    fn new(client: Client, consumer: Consumer) -> Self {
        Self {
            _client: client,
            consumer,
        }
    }
}

pub struct IggySource {
    connection: OwnedConsumer<IggyClient, IggyConsumer>,
}

impl IggySource {
    pub fn new(client: IggyClient, consumer: IggyConsumer) -> Self {
        Self {
            connection: OwnedConsumer::new(client, consumer),
        }
    }
}

impl MessageSource for IggySource {
    async fn recv(&mut self) -> Result<BrokerMessage> {
        let received = self
            .connection
            .consumer
            .next()
            .await
            .ok_or_else(|| anyhow!("Iggy consumer stopped"))??;
        Ok(BrokerMessage {
            payload: received.message.payload.to_vec(),
            partition: received.partition_id,
            offset: received.message.header.offset,
        })
    }

    async fn commit(&mut self, partition: u32, offset: u64) -> Result<()> {
        self.connection
            .consumer
            .store_offset(offset, Some(partition))
            .await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct IggyPublisher {
    producers: Arc<HashMap<String, IggyProducer>>,
}

impl IggyPublisher {
    pub fn single(producer: IggyProducer) -> Self {
        let mut producers = HashMap::new();
        producers.insert(producer.topic().as_string(), producer);
        Self {
            producers: Arc::new(producers),
        }
    }

    pub fn new(producers: IntoIter<IggyProducer>) -> Self {
        let producers = producers.fold(HashMap::new(), |mut hashmap, producer| {
            hashmap.insert(producer.topic().as_string(), producer);
            hashmap
        });
        Self {
            producers: Arc::new(producers),
        }
    }
}

impl MessagePublisher for IggyPublisher {
    async fn publish(&self, message: OutgoingMessage<impl Serialize>) -> Result<(), anyhow::Error> {
        let payload =
            serde_json::to_string(&message.data).map_err(|error| anyhow::anyhow!(error))?;
        let iggy_message = IggyMessage::from_str(&payload)?;
        let partitioning = Arc::new(Partitioning::messages_key_str(&message.partition_key)?);
        self.producers
            .as_ref()
            .get(&message.topic)
            .ok_or_else(|| {
                anyhow::anyhow!("No Iggy producer configured for topic '{}'", message.topic)
            })?
            .send_with_partitioning(vec![iggy_message], Some(partitioning))
            .await
            .map(|_| ())
            .map_err(|error| {
                anyhow::anyhow!(
                    "Failed Iggy publish message {} with error {}",
                    payload,
                    error
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use message_processor::{MessagePublisher, OutgoingMessage};

    use super::{IggyPublisher, OwnedConsumer};

    struct DropProbe(Arc<AtomicBool>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn owned_consumer_keeps_the_client_alive() {
        let dropped = Arc::new(AtomicBool::new(false));
        let connection = OwnedConsumer::new(DropProbe(Arc::clone(&dropped)), ());

        assert!(!dropped.load(Ordering::SeqCst));
        drop(connection);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn publishing_to_an_unconfigured_topic_returns_an_error() {
        futures::executor::block_on(async {
            let publisher = IggyPublisher::new(Vec::new().into_iter());

            let error = publisher
                .publish(OutgoingMessage::new(
                    "missing-topic".into(),
                    "partition-key".into(),
                    "payload",
                ))
                .await
                .unwrap_err();

            assert_eq!(
                error.to_string(),
                "No Iggy producer configured for topic 'missing-topic'"
            );
        });
    }
}
