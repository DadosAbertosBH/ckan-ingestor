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

use log::info;
use rdkafka::message::Message;
use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::sync::Notify;

use crate::job_processor::JobProcessor;
use crate::message_source::MessageSource;
use crate::messages::{JobMessage, JobResultMessage};
use crate::result_publisher::ResultPublisher;

pub struct WorkerThread<M: MessageSource, P: ResultPublisher, Proc: JobProcessor> {
    pub topic: String,
    pub partition: i32,
    source: Option<M>,
    publisher: P,
    processor: Proc,
    handle: Option<JoinHandle<()>>,
    shutdown: Arc<Notify>,
}

impl<M, P, Proc> WorkerThread<M, P, Proc>
where
    M: MessageSource + Send + 'static,
    P: ResultPublisher + Send + 'static,
    Proc: JobProcessor + Send + 'static,
{
    pub fn new(topic: String, partition: i32, source: M, publisher: P, processor: Proc) -> Self {
        Self {
            topic,
            partition,
            source: Some(source),
            publisher,
            processor,
            handle: None,
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Spawns a dedicated OS thread running the worker event loop.
    pub fn run(&mut self) {
        let source = self.source.take().expect("WorkerThread::run called twice");
        let shutdown = self.shutdown.clone();
        let topic = self.topic.clone();
        let partition = self.partition;
        let publisher = self.publisher.clone();
        let processor = self.processor.clone();

        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build worker runtime");
            rt.block_on(Self::run_loop(
                topic, partition, source, publisher, processor, shutdown,
            ));
        });
        self.handle = Some(handle);
    }

    async fn run_loop(
        topic: String,
        partition: i32,
        source: M,
        publisher: P,
        processor: Proc,
        shutdown: Arc<Notify>,
    ) {
        info!("Partition {}/{} worker ready", topic, partition);
        loop {
            tokio::select! {
                _ = shutdown.notified() => {
                    info!("Partition {}/{} shutting down", topic, partition);
                    return;
                }
                msg = source.recv() => {
                    match msg {
                        Ok(m) => {
                            let payload = m.payload().unwrap_or(&[]);
                            let job: JobMessage = serde_json::from_slice(payload)
                                .expect("failed to deserialize job message");

                            let processing = JobResultMessage {
                                job_id: job.job_id.clone(),
                                status: "PROCESSING".to_string(),
                                rows_processed: None,
                                expected_rows: None,
                                resource_size: None,
                                encoding: None,
                                expected_columns: None,
                                datastore_active: false,
                                labels: vec![],
                                error_message: None,
                                preview: None,
                            };
                            publisher.publish(processing).await
                                .expect("failed to publish PROCESSING");

                            // Run the synchronous, blocking processor on the
                            // blocking pool. The real processor drives async
                            // work (S3 upload) to completion internally via
                            // `Handle::block_on`, which would panic with
                            // "Cannot start a runtime from within a runtime" if
                            // executed on the async runtime's worker thread.
                            let processor = processor.clone();
                            let result = tokio::task::spawn_blocking(move || {
                                processor.process(job)
                            })
                            .await
                            .expect("processor panicked");
                            publisher.publish(result).await
                                .expect("failed to publish result");

                            source.commit(&topic, partition, m.offset()).await
                                .expect("failed to commit offset");
                        }
                        Err(e) => {
                            log::error!("Consumer error for {}/{}: {}", topic, partition, e);
                        }
                    }
                }
            }
        }
    }

    /// Signals the worker to shut down and blocks until it finishes.
    pub fn shutdown(self) {
        self.shutdown.notify_one();
        if let Some(handle) = self.handle {
            handle.join().expect("worker thread panicked");
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_source::tests::{MockMsg, MockSource};
    use crate::result_publisher::tests::MockPublisher;
    use tokio::sync::mpsc;

    #[derive(Clone)]
    struct StubProcessor;

    impl JobProcessor for StubProcessor {
        fn process(&self, _job: JobMessage) -> JobResultMessage {
            JobResultMessage {
                job_id: "stub".into(),
                status: "done".into(),
                rows_processed: Some(1),
                expected_rows: None,
                resource_size: None,
                encoding: None,
                expected_columns: None,
                datastore_active: false,
                labels: vec![],
                error_message: None,
                preview: None,
            }
        }
    }

    /// A processor that calls `block_on` synchronously, mirroring the real
    /// `DocumentReader::do_read` / `S3DocumentIngestor::ingest_blocking`.
    /// This panics with "Cannot start a runtime from within a runtime" when
    /// invoked from inside a tokio runtime.
    #[derive(Clone)]
    struct BlockingProcessor;

    impl JobProcessor for BlockingProcessor {
        fn process(&self, _job: JobMessage) -> JobResultMessage {
            // Simulate the synchronous blocking work that the real processor
            // performs (S3 upload) by blocking on an async future with the
            // current tokio handle. This panics with "Cannot start a runtime
            // from within a runtime" when invoked from inside a tokio runtime.
            let handle = tokio::runtime::Handle::current();
            handle.block_on(async { 42 });
            JobResultMessage {
                job_id: "blocking".into(),
                status: "done".into(),
                rows_processed: Some(1),
                expected_rows: None,
                resource_size: None,
                encoding: None,
                expected_columns: None,
                datastore_active: false,
                labels: vec![],
                error_message: None,
                preview: None,
            }
        }
    }

    fn mock_source(buffer: usize) -> (mpsc::Sender<MockMsg>, MockSource) {
        let (tx, rx) = mpsc::channel(buffer);
        (tx, MockSource::new(rx))
    }

    #[tokio::test]
    async fn run_and_shutdown() {
        let (_tx, source) = mock_source(1);
        let mut worker = WorkerThread::new(
            "test".into(),
            0,
            source,
            MockPublisher::new(),
            StubProcessor,
        );
        worker.run();
        assert_eq!(worker.topic, "test");
        assert_eq!(worker.partition, 0);
        worker.shutdown();
    }

    #[tokio::test]
    async fn worker_keeps_running_on_closed_channel() {
        let (tx, source) = mock_source(1);
        drop(tx);
        let mut worker =
            WorkerThread::new("t".into(), 0, source, MockPublisher::new(), StubProcessor);
        worker.run();
        worker.shutdown();
    }

    #[tokio::test]
    async fn processes_message_and_publishes_result() {
        let (tx, source) = mock_source(1);
        let publisher = MockPublisher::new();
        let published = publisher.published.clone();

        let mut worker = WorkerThread::new("t".into(), 0, source, publisher, StubProcessor);
        worker.run();

        let job = JobMessage {
            job_id: "job-1".into(),
            resource_id: "res-1".into(),
            ckan_url: "http://ckan".into(),
            resource_url: "".into(),
            resource_format: "".into(),
        };
        tx.send(MockMsg {
            payload: serde_json::to_vec(&job).unwrap(),
            offset: 42,
        })
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        worker.shutdown();

        let results = published.lock().unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].status, "PROCESSING");
        assert_eq!(results[1].status, "done");
    }

    #[tokio::test]
    async fn processes_blocking_processor_without_panicking() {
        let (tx, source) = mock_source(1);
        let publisher = MockPublisher::new();
        let published = publisher.published.clone();

        let mut worker = WorkerThread::new("t".into(), 0, source, publisher, BlockingProcessor);
        worker.run();

        let job = JobMessage {
            job_id: "job-1".into(),
            resource_id: "res-1".into(),
            ckan_url: "http://ckan".into(),
            resource_url: "".into(),
            resource_format: "".into(),
        };
        tx.send(MockMsg {
            payload: serde_json::to_vec(&job).unwrap(),
            offset: 42,
        })
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        worker.shutdown();

        let results = published.lock().unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].status, "PROCESSING");
        assert_eq!(results[1].status, "done");
    }

    #[tokio::test]
    async fn commits_offset_after_publish() {
        let (tx, source): (mpsc::Sender<MockMsg>, MockSource) = mock_source(1);
        let committed = source.committed.clone();
        let publisher = MockPublisher::new();

        let mut worker = WorkerThread::new("t".into(), 0, source, publisher, StubProcessor);
        worker.run();

        let job = JobMessage {
            job_id: "job-1".into(),
            resource_id: "res-1".into(),
            ckan_url: "http://ckan".into(),
            resource_url: "".into(),
            resource_format: "".into(),
        };
        tx.send(MockMsg {
            payload: serde_json::to_vec(&job).unwrap(),
            offset: 42,
        })
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        worker.shutdown();

        let commits = committed.lock().unwrap();
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0], ("t".to_string(), 0, 42));
    }

    #[tokio::test]
    #[should_panic(expected = "worker thread panicked")]
    async fn deserialize_failure_panics() {
        let (tx, source) = mock_source(1);
        let mut worker =
            WorkerThread::new("t".into(), 0, source, MockPublisher::new(), StubProcessor);
        worker.run();

        tx.send(MockMsg {
            payload: b"not-json".to_vec(),
            offset: 0,
        })
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        worker.shutdown();
    }
}
