// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use log::info;
use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::sync::Notify;

use crate::job_processor::JobProcessor;
use crate::message_source::MessageSource;
use crate::messages::{JobMessage, JobResultMessage, JobStatus};
use crate::result_publisher::ResultPublisher;

pub struct WorkerThread<M: MessageSource, P: ResultPublisher, Proc: JobProcessor> {
    pub topic: String,
    pub slot: usize,
    source: Option<M>,
    publisher: P,
    processor: Option<Proc>,
    handle: Option<JoinHandle<()>>,
    shutdown: Arc<Notify>,
}

impl<M, P, Proc> WorkerThread<M, P, Proc>
where
    M: MessageSource + Send + 'static,
    P: ResultPublisher + Send + 'static,
    Proc: JobProcessor + Send + 'static,
{
    pub fn new(topic: String, slot: usize, source: M, publisher: P, processor: Proc) -> Self {
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
        let source = self.source.take().expect("WorkerThread::run called twice");
        let shutdown = self.shutdown.clone();
        let topic = self.topic.clone();
        let slot = self.slot;
        let publisher = self.publisher.clone();
        let processor = self
            .processor
            .take()
            .expect("WorkerThread::run called twice");

        self.handle = Some(std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build worker runtime");
            Self::run_loop(
                &runtime, topic, slot, source, publisher, processor, shutdown,
            );
        }));
    }

    fn run_loop(
        runtime: &tokio::runtime::Runtime,
        topic: String,
        slot: usize,
        source: M,
        publisher: P,
        processor: Proc,
        shutdown: Arc<Notify>,
    ) {
        info!("Iggy worker ready: topic={topic} slot={slot}");
        loop {
            let message = runtime.block_on(async {
                tokio::select! {
                    _ = shutdown.notified() => None,
                    message = source.recv() => Some(message),
                }
            });

            let Some(message) = message else {
                info!("Iggy worker stopping: topic={topic} slot={slot}");
                return;
            };

            match message {
                Ok(message) => {
                    let job: JobMessage = serde_json::from_slice(&message.payload).unwrap_or_else(|error| {
                        let payload_prefix = message
                            .payload
                            .iter()
                            .take(32)
                            .map(|byte| format!("{byte:02x}"))
                            .collect::<Vec<_>>()
                            .join(" ");
                        panic!(
                            "failed to deserialize Iggy job message: topic={topic} slot={slot} partition={} offset={} payload_length={} payload_prefix_hex=[{payload_prefix}]: {error}",
                            message.partition,
                            message.offset,
                            message.payload.len(),
                        );
                    });
                    let processing = JobResultMessage {
                        reader: String::new(),
                        job_id: job.job_id.clone(),
                        status: JobStatus::Processing,
                        rows_processed: None,
                        expected_rows: None,
                        encoding: None,
                        csv_strict_mode: None,
                        csv_delimiter: None,
                        expected_columns: None,
                        datastore_active: false,
                        error_message: None,
                        preview: None,
                    };
                    runtime
                        .block_on(publisher.publish(processing))
                        .expect("failed to publish PROCESSING");

                    let result = processor.process(job);
                    runtime
                        .block_on(publisher.publish(result))
                        .expect("failed to publish terminal result");

                    runtime
                        .block_on(source.commit(message.partition, message.offset))
                        .expect("failed to store Iggy offset");
                }
                Err(error) => {
                    log::error!("Iggy consumer error: topic={topic} slot={slot}: {error}");
                }
            }
        }
    }

    pub fn shutdown(self) {
        self.shutdown.notify_one();
        if let Some(handle) = self.handle {
            handle.join().expect("worker thread panicked");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job_processor::JobProcessor;
    use crate::message_source::{BrokerMessage, tests::MockSource};
    use crate::result_publisher::tests::MockPublisher;
    use std::time::Duration;
    use tokio::sync::mpsc;

    #[derive(Clone)]
    struct StubProcessor;

    impl JobProcessor for StubProcessor {
        fn process(&self, job: JobMessage) -> JobResultMessage {
            JobResultMessage {
                reader: String::new(),
                job_id: job.job_id,
                status: JobStatus::Success,
                rows_processed: Some(1),
                expected_rows: None,
                encoding: None,
                csv_strict_mode: None,
                csv_delimiter: None,
                expected_columns: None,
                datastore_active: false,
                error_message: None,
                preview: None,
            }
        }
    }

    fn job_payload() -> Vec<u8> {
        serde_json::to_vec(&JobMessage {
            job_id: "job-1".into(),
            resource_id: "resource-1".into(),
            ckan_url: "https://ckan.example".into(),
            resource_url: String::new(),
            resource_format: String::new(),
            csv_delimiter: None,
            datastore_active: false,
        })
        .unwrap()
    }

    #[test]
    fn publishes_processing_and_terminal_result_before_committing() {
        let (tx, rx) = mpsc::channel(1);
        tx.blocking_send(BrokerMessage {
            payload: job_payload(),
            partition: 3,
            offset: 42,
        })
        .unwrap();
        let source = MockSource::new(rx);
        let committed = source.committed.clone();
        let publisher = MockPublisher::new();
        let published = publisher.published.clone();
        let mut worker = WorkerThread::new("jobs".into(), 0, source, publisher, StubProcessor);
        worker.run();

        for _ in 0..100 {
            if committed.lock().unwrap().len() == 1 {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        worker.shutdown();

        assert_eq!(*committed.lock().unwrap(), vec![(3, 42)]);
        let statuses: Vec<JobStatus> = published
            .lock()
            .unwrap()
            .iter()
            .map(|message| message.status)
            .collect();
        assert_eq!(statuses, vec![JobStatus::Processing, JobStatus::Success]);
    }
}
