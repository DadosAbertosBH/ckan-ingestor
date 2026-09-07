// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use anyhow::Context;
use ckan_ingestor_worker_lib::{BrokerMessage, ConsumerWorker, MessageHandler};

use crate::job_processor::JobProcessor;
use crate::message_source::MessageSource;
use crate::messages::{JobMessage, JobResultMessage, JobStatus};
use crate::result_publisher::ResultPublisher;

pub struct WorkerThread<M: MessageSource, P: ResultPublisher + Send + 'static, Proc: JobProcessor> {
    inner: ConsumerWorker<M, JobHandler<P, Proc>>,
}

struct JobHandler<P: ResultPublisher + Send + 'static, Proc: JobProcessor> {
    publisher: P,
    processor: Proc,
}

impl<P: ResultPublisher + Send + 'static, Proc: JobProcessor> MessageHandler
    for JobHandler<P, Proc>
{
    async fn handle(&self, message: &BrokerMessage) -> anyhow::Result<()> {
        let job: JobMessage = serde_json::from_slice(&message.payload).unwrap_or_else(|error| {
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
        let processing = JobResultMessage {
            reader: Some(String::new()),
            job_id: Some(job.job_id.clone()),
            status: JobStatus::Processing,
            rows_processed: None,
            expected_rows: None,
            encoding: None,
            csv_strict_mode: None,
            csv_delimiter: None,
            expected_columns: None,
            datastore_active: Some(false),
            resource_id: job.resource_id.clone(),
            dataset_name: None,
            resource_name: None,
            resource_url: None,
            resource_format: None,
            instance_id: None,
            ckan_url: None,
            error_message: None,
            preview: None,
            artifact: None,
        };
        self.publisher
            .publish(processing)
            .await
            .expect("failed to publish PROCESSING");
        let processor = self.processor.clone();
        let result = tokio::task::spawn_blocking(move || processor.process(job))
            .await
            .context("job processor panicked")?;
        self.publisher
            .publish(result)
            .await
            .expect("failed to publish terminal result");
        Ok(())
    }
}

impl<M, P, Proc> WorkerThread<M, P, Proc>
where
    M: MessageSource + Send + 'static,
    P: ResultPublisher + Send + 'static,
    Proc: JobProcessor + Send + 'static,
{
    pub fn new(topic: String, slot: usize, source: M, publisher: P, processor: Proc) -> Self {
        Self {
            inner: ConsumerWorker::new(
                topic,
                slot,
                source,
                JobHandler {
                    publisher,
                    processor,
                },
            ),
        }
    }

    pub fn run(&mut self) {
        self.inner.run();
    }

    pub fn shutdown(self) {
        self.inner.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job_processor::JobProcessor;
    use crate::message_source::{BrokerMessage, tests::MockSource};
    use crate::result_publisher::tests::MockPublisher;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio::sync::mpsc;

    #[derive(Clone)]
    struct StubProcessor;

    #[derive(Clone)]
    struct RuntimeBlockingProcessor {
        processed: Arc<AtomicBool>,
    }

    impl JobProcessor for StubProcessor {
        fn process(&self, job: JobMessage) -> JobResultMessage {
            JobResultMessage {
                reader: Some(String::new()),
                job_id: Some(job.job_id),
                status: JobStatus::Success,
                rows_processed: Some(1),
                expected_rows: None,
                encoding: None,
                csv_strict_mode: None,
                csv_delimiter: None,
                expected_columns: None,
                datastore_active: Some(false),
                resource_id: job.resource_id,
                dataset_name: None,
                resource_name: None,
                resource_url: None,
                resource_format: None,
                instance_id: None,
                ckan_url: None,
                error_message: None,
                preview: None,
                artifact: None,
            }
        }
    }

    impl JobProcessor for RuntimeBlockingProcessor {
        fn process(&self, job: JobMessage) -> JobResultMessage {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {});
            self.processed.store(true, Ordering::SeqCst);
            StubProcessor.process(job)
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

    #[test]
    fn processes_jobs_outside_the_message_runtime() {
        let (tx, rx) = mpsc::channel(1);
        tx.blocking_send(BrokerMessage {
            payload: job_payload(),
            partition: 3,
            offset: 42,
        })
        .unwrap();
        let source = MockSource::new(rx);
        let committed = source.committed.clone();
        let processed = Arc::new(AtomicBool::new(false));
        let processor = RuntimeBlockingProcessor {
            processed: processed.clone(),
        };
        let mut worker =
            WorkerThread::new("jobs".into(), 0, source, MockPublisher::new(), processor);
        worker.run();

        for _ in 0..100 {
            if committed.lock().unwrap().len() == 1 {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        worker.shutdown();

        assert!(processed.load(Ordering::SeqCst));
        assert_eq!(*committed.lock().unwrap(), vec![(3, 42)]);
    }
}
