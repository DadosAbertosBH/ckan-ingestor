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

use std::collections::HashMap;
use std::sync::Arc;

use rdkafka::consumer::{ConsumerContext, StreamConsumer};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::job_processor::JobProcessor;
use crate::message_source::PartitionSource;
use crate::result_publisher::ResultPublisher;
use crate::worker_thread::WorkerThread;

type PartitionKey = (String, i32);

pub enum Command {
    Assign(Vec<PartitionKey>),
    Revoke(Vec<PartitionKey>),
}

pub struct WorkerCoordinator<
    C: ConsumerContext + Send + Sync + 'static,
    P: ResultPublisher + Send + 'static,
    Proc: JobProcessor,
> {
    workers: HashMap<PartitionKey, WorkerThread<PartitionSource<C>, P, Proc>>,
    consumer: Arc<StreamConsumer<C>>,
    processor: Proc,
    rx: UnboundedReceiver<Command>,
}

impl<C, P, Proc> WorkerCoordinator<C, P, Proc>
where
    C: ConsumerContext + Send + Sync + 'static,
    P: ResultPublisher + Send + 'static,
    Proc: JobProcessor,
{
    pub fn new(
        consumer: Arc<StreamConsumer<C>>,
        processor: Proc,
        rx: UnboundedReceiver<Command>,
    ) -> Self {
        Self {
            workers: HashMap::new(),
            consumer,
            processor,
            rx,
        }
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    pub async fn assign(&mut self, keys: Vec<PartitionKey>, publisher: P) {
        for key in keys {
            let queue = self
                .consumer
                .split_partition_queue(&key.0, key.1)
                .expect("split_partition_queue");
            let source = PartitionSource::new(queue, self.consumer.clone());
            let mut worker = WorkerThread::new(
                key.0.clone(),
                key.1,
                source,
                publisher.clone(),
                self.processor.clone(),
            );
            worker.run();
            self.workers.insert(key, worker);
        }
    }

    pub async fn revoke(&mut self, keys: Vec<PartitionKey>) {
        for key in keys {
            if let Some(worker) = self.workers.remove(&key) {
                tokio::task::spawn_blocking(move || worker.shutdown())
                    .await
                    .expect("worker shutdown panicked");
            }
        }
    }

    async fn revoke_all(&mut self) {
        let keys: Vec<PartitionKey> = self.workers.keys().cloned().collect();
        self.revoke(keys).await;
    }

    pub async fn run(&mut self, publisher: P, shutdown: Arc<tokio::sync::Notify>) {
        // Start the main consumer poll loop immediately — it drives the
        // rebalance callback, which in turn triggers split_partition_queue.
        let mc = Arc::clone(&self.consumer);
        let main_recv = tokio::spawn(async move {
            loop {
                let _ = mc.recv().await;
                log::warn!("Main consumer received unexpected message");
            }
        });

        loop {
            tokio::select! {
                _ = shutdown.notified() => break,
                cmd = self.rx.recv() => {
                    match cmd {
                        Some(Command::Assign(keys)) => {
                            self.revoke_all().await;
                            self.assign(keys, publisher.clone()).await;
                        }
                        Some(Command::Revoke(keys)) => self.revoke(keys).await,
                        None => break,
                    }
                }
            }
        }

        // Gracefully stop all remaining workers.
        self.revoke_all().await;

        main_recv.abort();
        let _ = main_recv.await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job_processor::JobProcessor;
    use crate::messages::{JobMessage, JobResultMessage, JobStatus};
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
                expected_columns: None,
                datastore_active: false,
                error_message: None,
                preview: None,
            }
        }
    }

    fn new_coordinator(
        cmd_rx: UnboundedReceiver<Command>,
    ) -> WorkerCoordinator<rdkafka::consumer::DefaultConsumerContext, MockPublisher, StubProcessor>
    {
        let consumer = Arc::new(
            rdkafka::ClientConfig::new()
                .create::<StreamConsumer<rdkafka::consumer::DefaultConsumerContext>>()
                .expect("mock consumer"),
        );
        WorkerCoordinator::new(consumer, StubProcessor, cmd_rx)
    }

    #[tokio::test]
    async fn channel_integration() {
        let key: PartitionKey = ("topic".into(), 0);
        let publisher = MockPublisher::new();

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let mut coordinator = new_coordinator(cmd_rx);
        let shutdown = Arc::new(tokio::sync::Notify::new());

        let handle = tokio::spawn(async move {
            coordinator.run(publisher, shutdown).await;
        });

        cmd_tx.send(Command::Assign(vec![key.clone()])).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        cmd_tx.send(Command::Revoke(vec![key])).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        drop(cmd_tx);
        handle.await.unwrap();
    }
}
