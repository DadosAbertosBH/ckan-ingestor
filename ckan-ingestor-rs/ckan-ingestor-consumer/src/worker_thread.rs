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
use crate::messages::{JobMessage, JobResultMessage, JobStatus};
use crate::result_publisher::ResultPublisher;

pub struct WorkerThread<M: MessageSource, P: ResultPublisher, Proc: JobProcessor> {
    pub topic: String,
    pub partition: i32,
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
    pub fn new(topic: String, partition: i32, source: M, publisher: P, processor: Proc) -> Self {
        Self {
            topic,
            partition,
            source: Some(source),
            publisher,
            processor: Some(processor),
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
        let processor = self
            .processor
            .take()
            .expect("WorkerThread::run called twice");

        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build worker runtime");
            Self::run_loop(
                &rt, topic, partition, source, publisher, processor, shutdown,
            );
        });
        self.handle = Some(handle);
    }

    fn run_loop(
        rt: &tokio::runtime::Runtime,
        topic: String,
        partition: i32,
        source: M,
        publisher: P,
        processor: Proc,
        shutdown: Arc<Notify>,
    ) {
        info!("Partition {}/{} worker ready", topic, partition);
        loop {
            let message = rt.block_on(async {
                tokio::select! {
                    _ = shutdown.notified() => None,
                    msg = source.recv() => Some(msg),
                }
            });

            match message {
                None => {
                    info!("Partition {}/{} shutting down", topic, partition);
                    return;
                }
                Some(Ok(m)) => {
                    let payload = m.payload().unwrap_or(&[]);
                    let job: JobMessage =
                        serde_json::from_slice(payload).expect("failed to deserialize job message");

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
                    rt.block_on(publisher.publish(processing))
                        .expect("failed to publish PROCESSING");

                    let result = processor.process(job);
                    rt.block_on(publisher.publish(result))
                        .expect("failed to publish result");

                    rt.block_on(source.commit(&topic, partition, m.offset()))
                        .expect("failed to commit offset");
                }
                Some(Err(e)) => {
                    log::error!("Consumer error for {}/{}: {}", topic, partition, e);
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::message_source::tests::{MockMsg, MockSource};
    use crate::result_publisher::tests::MockPublisher;
    use tokio::sync::mpsc;

    #[derive(Clone)]
    struct StubProcessor;

    impl JobProcessor for StubProcessor {
        fn process(&self, _job: JobMessage) -> JobResultMessage {
            JobResultMessage {
                reader: String::new(),
                job_id: "stub".into(),
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

    struct CloneCountingProcessor {
        clones: Arc<AtomicUsize>,
    }

    impl Clone for CloneCountingProcessor {
        fn clone(&self) -> Self {
            self.clones.fetch_add(1, Ordering::SeqCst);
            Self {
                clones: self.clones.clone(),
            }
        }
    }

    impl JobProcessor for CloneCountingProcessor {
        fn process(&self, job: JobMessage) -> JobResultMessage {
            JobResultMessage {
                reader: String::new(),
                job_id: job.job_id,
                status: JobStatus::Success,
                rows_processed: None,
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

    #[derive(Clone)]
    struct OutsideTokioProcessor;

    impl JobProcessor for OutsideTokioProcessor {
        fn process(&self, job: JobMessage) -> JobResultMessage {
            assert!(tokio::runtime::Handle::try_current().is_err());
            JobResultMessage {
                reader: String::new(),
                job_id: job.job_id,
                status: JobStatus::Success,
                rows_processed: None,
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
        assert_eq!(results[0].status, JobStatus::Processing);
        assert_eq!(results[1].status, JobStatus::Success);
    }

    #[tokio::test]
    async fn keeps_the_same_processor_for_every_message() {
        let (tx, source) = mock_source(1);
        let clones = Arc::new(AtomicUsize::new(0));
        let processor = CloneCountingProcessor {
            clones: clones.clone(),
        };
        let mut worker = WorkerThread::new("t".into(), 0, source, MockPublisher::new(), processor);
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

        assert_eq!(clones.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn runs_the_processor_outside_the_tokio_runtime() {
        let (tx, source) = mock_source(1);
        let publisher = MockPublisher::new();
        let published = publisher.published.clone();
        let mut worker = WorkerThread::new("t".into(), 0, source, publisher, OutsideTokioProcessor);
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

        assert_eq!(
            published.lock().unwrap().last().unwrap().status,
            JobStatus::Success
        );
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
