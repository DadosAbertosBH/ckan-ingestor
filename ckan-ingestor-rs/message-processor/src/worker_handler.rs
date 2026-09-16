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
use std::{sync::Arc, thread::JoinHandle};

use futures::StreamExt;
use serde::de::DeserializeOwned;
use tokio::sync::Notify;

use crate::{BrokerMessage, MessageProcessor, MessagePublisher, MessageSource};

pub struct WorkerHandler<M, Publisher, Proc>
where
    M: MessageSource,
    Proc: MessageProcessor,
    Publisher: MessagePublisher,
{
    topic: String,
    slot: usize,
    source: Option<M>,
    processor: Option<Proc>,
    publisher: Option<Publisher>,
    thread: Option<JoinHandle<()>>,
    shutdown: Arc<Notify>,
}

impl<M, Publisher, Proc> WorkerHandler<M, Publisher, Proc>
where
    M: MessageSource + Send + 'static,
    Publisher: MessagePublisher + Send + 'static,
    Proc: MessageProcessor + Send + 'static,
    Proc::IncomingMessage: DeserializeOwned,
{
    pub fn new(
        topic: String,
        slot: usize,
        source: M,
        publisher: Publisher,
        processor: Proc,
    ) -> Self {
        Self {
            topic,
            slot,
            source: Some(source),
            processor: Some(processor),
            publisher: Some(publisher),
            thread: None,
            shutdown: Arc::new(Notify::new()),
        }
    }

    async fn handle(
        publisher: &Publisher,
        processor: &Proc,
        message: &BrokerMessage,
    ) -> anyhow::Result<()> {
        let incoming: Proc::IncomingMessage = serde_json::from_slice(&message.payload).unwrap_or_else(|error| {
            let payload_prefix = message
                .payload
                .iter()
                .take(32)
                .map(|byte| format!("{byte:02x}"))
                .collect::<Vec<_>>()
                .join(" ");
            panic!(
                "failed to deserialize Iggy job message: partition={} offset={} payload_length={} payload_prefix_hex=[{payload_prefix}]: {error}",
                message.partition,
                message.offset,
                message.payload.len(),
            );
        });
        let stream = processor.process(incoming);
        tokio::pin!(stream);
        while let Some(msg) = stream.next().await {
            publisher
                .publish(msg)
                .await
                .expect("failed to publish message"); // Agora sim haverá um intervalo real!
        }
        Ok(())
    }

    pub fn run(&mut self) {
        let mut source = self.source.take().expect("worker already running");
        let publisher = self.publisher.take().expect("worker already running");
        let processor = self.processor.take().expect("worker already running");
        let shutdown = self.shutdown.clone();
        let topic = self.topic.clone();
        let slot = self.slot;
        self.thread = Some(std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("worker runtime");
            loop {
                let message = runtime.block_on(async {
                    tokio::select! {
                        _ = shutdown.notified() => None,
                        message = source.recv() => Some(message)
                    }
                });
                let Some(message) = message else {
                    return;
                };
                match message {
                    Ok(message) => {
                        let result =
                            runtime.block_on(Self::handle(&publisher, &processor, &message));
                        match result {
                            Ok(()) => {
                                if let Err(error) = runtime
                                    .block_on(source.commit(message.partition, message.offset))
                                {
                                    log::error!(
                                        "offset commit failed: topic={topic} slot={slot}: {error}"
                                    );
                                }
                            }
                            Err(error) => log::error!(
                                "message handling failed: topic={topic} slot={slot}: {error}"
                            ),
                        }
                    }
                    Err(error) => log::error!("consumer error: topic={topic} slot={slot}: {error}"),
                }
            }
        }));
    }

    pub fn shutdown(self) {
        self.shutdown.notify_one();
        if let Some(thread) = self.thread {
            thread.join().expect("worker thread panicked");
        }
    }
}
