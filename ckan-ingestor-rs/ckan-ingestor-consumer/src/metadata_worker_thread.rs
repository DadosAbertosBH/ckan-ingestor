// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use crate::message_source::MessageSource;
use crate::metadata_processor::RealMetadataProcessor;
use crate::result_publisher::IggyResultPublisher;
use ckan_metadata_ingestor::MetadataSyncCommand;
use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::sync::Notify;

pub struct MetadataWorkerThread<M: MessageSource> {
    topic: String,
    slot: usize,
    source: Option<M>,
    publisher: IggyResultPublisher,
    processor: Option<RealMetadataProcessor>,
    handle: Option<JoinHandle<()>>,
    shutdown: Arc<Notify>,
}
impl<M: MessageSource + Send + 'static> MetadataWorkerThread<M> {
    pub fn new(
        topic: String,
        slot: usize,
        source: M,
        publisher: IggyResultPublisher,
        processor: RealMetadataProcessor,
    ) -> Self {
        Self {
            topic,
            slot,
            source: Some(source),
            publisher,
            processor: Some(processor),
            handle: None,
            shutdown: Arc::new(Notify::new()),
        }
    }
    pub fn run(&mut self) {
        let mut source = self.source.take().expect("worker started twice");
        let publisher = self.publisher.clone();
        let processor = self.processor.take().expect("worker started twice");
        let shutdown = self.shutdown.clone();
        let topic = self.topic.clone();
        let slot = self.slot;
        self.handle = Some(std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("worker runtime");
            loop {
                let message = runtime.block_on(async { tokio::select! { _ = shutdown.notified() => None, message = source.recv() => Some(message) } });
                let Some(message) = message else {
                    return;
                };
                match message {
                    Ok(message) => {
                        match serde_json::from_slice::<MetadataSyncCommand>(&message.payload) {
                            Ok(command) => {
                                let result = processor.process(command);
                                if let Err(error) =
                                    runtime.block_on(publisher.publish_metadata(result))
                                {
                                    log::error!("metadata result publish failed: {error}");
                                    continue;
                                }
                                if let Err(error) = runtime
                                    .block_on(source.commit(message.partition, message.offset))
                                {
                                    log::error!("metadata offset commit failed: {error}");
                                }
                            }
                            Err(error) => log::error!(
                                "invalid metadata command topic={topic} slot={slot}: {error}"
                            ),
                        }
                    }
                    Err(error) => {
                        log::error!("metadata consumer error topic={topic} slot={slot}: {error}")
                    }
                }
            }
        }));
    }
    pub fn shutdown(self) {
        self.shutdown.notify_one();
        if let Some(handle) = self.handle {
            handle.join().expect("metadata worker thread panicked");
        }
    }
}
