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
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::message_source::MessageSource;
use crate::messages::{JobMessage, JobResultMessage};
use crate::result_publisher::ResultPublisher;
use crate::worker_coordinator::ProcessorFn;

pub struct WorkerThread<M: MessageSource, P: ResultPublisher> {
    pub topic: String,
    pub partition: i32,
    source: Option<M>,
    publisher: P,
    processor: ProcessorFn,
    handle: Option<JoinHandle<()>>,
    shutdown: Arc<Notify>,
}

impl<M: MessageSource + Send + 'static, P: ResultPublisher + Send + 'static> WorkerThread<M, P> {
    pub fn new(
        topic: String,
        partition: i32,
        source: M,
        publisher: P,
        processor: ProcessorFn,
    ) -> Self {
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

    pub fn run(&mut self) {
        let source = self.source.take().expect("WorkerThread::run called twice");
        let shutdown = self.shutdown.clone();
        let topic = self.topic.clone();
        let partition = self.partition;
        let publisher = self.publisher.clone();
        let processor = self.processor;

        let handle = tokio::spawn(async move {
            Self::run_loop(topic, partition, source, publisher, processor, shutdown).await;
        });
        self.handle = Some(handle);
    }

    async fn run_loop(
        topic: String,
        partition: i32,
        source: M,
        publisher: P,
        processor: ProcessorFn,
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
                            let job: JobMessage =
                                serde_json::from_slice(payload).expect("failed to deserialize job message");
                            let result = processor(job);
                            publisher.publish(result).await.expect("failed to publish result");
                        }
                        Err(e) => {
                            log::error!(
                                "Consumer error for {}/{}: {}",
                                topic, partition, e
                            );
                        }
                    }
                }
            }
        }
    }

    pub async fn shutdown(self) {
        self.shutdown.notify_one();
        if let Some(handle) = self.handle {
            handle.await.expect("worker task panicked");
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
    use crate::result_publisher::ResultPublisher;
    use crate::result_publisher::tests::MockPublisher;
    use std::future::Future;
    use std::pin::Pin;
    use tokio::sync::mpsc;

    /// A publisher that always fails — used to test panic on publish failure.
    #[derive(Clone)]
    struct FailingPublisher;

    impl ResultPublisher for FailingPublisher {
        fn publish(
            &self,
            _result: JobResultMessage,
        ) -> impl Future<Output = Result<(), anyhow::Error>> + Send {
            std::future::ready(Err(anyhow::anyhow!("publish failed")))
        }
    }

    fn mock_source(buffer: usize) -> (mpsc::Sender<MockMsg>, MockSource) {
        let (tx, rx) = mpsc::channel(buffer);
        (tx, MockSource::new(rx))
    }

    fn stub_processor(_job: JobMessage) -> JobResultMessage {
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

    #[tokio::test]
    async fn run_and_shutdown() {
        let (_tx, source) = mock_source(1);

        let mut worker = WorkerThread::new(
            "test".into(),
            0,
            source,
            MockPublisher::new(),
            stub_processor,
        );
        worker.run();

        assert_eq!(worker.topic, "test");
        assert_eq!(worker.partition, 0);

        worker.shutdown().await;
    }

    #[tokio::test]
    async fn worker_keeps_running_on_closed_channel() {
        let (tx, source) = mock_source(1);
        drop(tx);
        let mut worker =
            WorkerThread::new("t".into(), 0, source, MockPublisher::new(), stub_processor);
        worker.run();
        worker.shutdown().await;
    }

    #[tokio::test]
    async fn processes_message_and_publishes_result() {
        let (tx, source) = mock_source(1);
        let publisher = MockPublisher::new();
        let published = publisher.published.clone();

        let mut worker = WorkerThread::new("t".into(), 0, source, publisher, stub_processor);
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

        // Give the worker time to process before shutdown
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        worker.shutdown().await;

        let results = published.lock().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].job_id, "stub");
    }

    #[tokio::test]
    #[should_panic(expected = "worker task panicked")]
    async fn deserialize_failure_panics() {
        let (tx, source) = mock_source(1);

        let mut worker =
            WorkerThread::new("t".into(), 0, source, MockPublisher::new(), stub_processor);
        worker.run();

        // Invalid JSON
        tx.send(MockMsg {
            payload: b"not-json".to_vec(),
            offset: 0,
        })
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        worker.shutdown().await;
    }

    #[tokio::test]
    #[should_panic(expected = "worker task panicked")]
    async fn publish_failure_panics() {
        let (tx, source) = mock_source(1);

        let mut worker = WorkerThread::new("t".into(), 0, source, FailingPublisher, stub_processor);
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
            offset: 0,
        })
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        worker.shutdown().await;
    }
}
