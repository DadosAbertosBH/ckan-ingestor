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

use crate::message_source::PartitionSource;
use crate::messages::JobMessage;
use crate::messages::JobResultMessage;
use crate::result_publisher::ResultPublisher;
use crate::worker_thread::WorkerThread;

type PartitionKey = (String, i32);

pub type ProcessorFn = fn(JobMessage) -> JobResultMessage;

pub enum Command {
    Assign(Vec<PartitionKey>),
    Revoke(Vec<PartitionKey>),
}

pub struct WorkerCoordinator<
    C: ConsumerContext + Send + Sync + 'static,
    P: ResultPublisher + Send + 'static,
> {
    workers: HashMap<PartitionKey, WorkerThread<PartitionSource<C>, P>>,
    consumer: Arc<StreamConsumer<C>>,
    processor: ProcessorFn,
}

impl<C: ConsumerContext + Send + Sync + 'static, P: ResultPublisher + Send + 'static>
    WorkerCoordinator<C, P>
{
    pub fn new(consumer: Arc<StreamConsumer<C>>, processor: ProcessorFn) -> Self {
        Self {
            workers: HashMap::new(),
            consumer,
            processor,
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
                self.processor,
            );
            worker.run();
            self.workers.insert(key, worker);
        }
    }

    pub async fn revoke(&mut self, keys: Vec<PartitionKey>) {
        for key in keys {
            if let Some(worker) = self.workers.remove(&key) {
                worker.shutdown().await;
            }
        }
    }

    async fn revoke_all(&mut self) {
        let keys: Vec<PartitionKey> = self.workers.keys().cloned().collect();
        self.revoke(keys).await;
    }

    pub async fn run(&mut self, mut rx: UnboundedReceiver<Command>, publisher: P) {
        let mut main_recv: Option<tokio::task::JoinHandle<()>> = None;

        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::Assign(keys) => {
                    self.revoke_all().await;
                    self.assign(keys, publisher.clone()).await;

                    if main_recv.is_none() {
                        let mc = Arc::clone(&self.consumer);
                        main_recv = Some(tokio::spawn(async move {
                            loop {
                                let _ = mc.recv().await;
                                log::warn!("Main consumer received unexpected message");
                            }
                        }));
                    }
                }
                Command::Revoke(keys) => self.revoke(keys).await,
            }
        }

        if let Some(recv) = main_recv {
            recv.abort();
            let _ = recv.await;
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result_publisher::tests::MockPublisher;
    use tokio::sync::mpsc;

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

    fn new_coordinator()
    -> WorkerCoordinator<rdkafka::consumer::DefaultConsumerContext, MockPublisher> {
        let consumer = Arc::new(
            rdkafka::ClientConfig::new()
                .create::<StreamConsumer<rdkafka::consumer::DefaultConsumerContext>>()
                .expect("mock consumer"),
        );
        WorkerCoordinator::new(consumer, stub_processor)
    }

    #[tokio::test]
    async fn channel_integration() {
        let key: PartitionKey = ("topic".into(), 0);
        let publisher = MockPublisher::new();

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let mut coordinator = new_coordinator();

        let handle = tokio::spawn(async move {
            coordinator.run(cmd_rx, publisher).await;
        });

        cmd_tx.send(Command::Assign(vec![key.clone()])).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        cmd_tx.send(Command::Revoke(vec![key])).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        drop(cmd_tx);
        handle.await.unwrap();
    }
}
