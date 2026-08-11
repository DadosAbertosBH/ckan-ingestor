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

use rdkafka::consumer::StreamConsumer;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::message_source::MessageSource;
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

pub struct WorkerCoordinator<M: MessageSource + Send + 'static, P: ResultPublisher + Send + 'static>
{
    workers: HashMap<PartitionKey, WorkerThread<M, P>>,
    processor: ProcessorFn,
}

impl<M: MessageSource + Send + 'static, P: ResultPublisher + Send + 'static>
    WorkerCoordinator<M, P>
{
    pub fn new(processor: ProcessorFn) -> Self {
        Self {
            workers: HashMap::new(),
            processor,
        }
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    pub async fn assign(&mut self, keys: Vec<PartitionKey>, sources: Vec<M>, publisher: P) {
        for (key, source) in keys.into_iter().zip(sources) {
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

    /// Destroys all currently running workers.
    async fn revoke_all(&mut self) {
        let keys: Vec<PartitionKey> = self.workers.keys().cloned().collect();
        self.revoke(keys).await;
    }

    /// Runs the coordinator loop.  `main_consumer` is the unsplit
    /// `StreamConsumer` — it must be polled periodically to serve callbacks.
    /// `create_source` is called post-rebalance to produce partition queues
    /// via `split_partition_queue`.
    pub async fn run<CreateF>(
        &mut self,
        mut rx: UnboundedReceiver<Command>,
        main_consumer: Arc<StreamConsumer>,
        publisher: P,
        mut create_source: CreateF,
    ) where
        CreateF: FnMut(&PartitionKey, &Arc<StreamConsumer>) -> Option<M>,
    {
        let mut main_recv: Option<tokio::task::JoinHandle<()>> = None;

        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::Assign(keys) => {
                    self.revoke_all().await;

                    let sources: Vec<M> = keys
                        .iter()
                        .filter_map(|k| create_source(k, &main_consumer))
                        .collect();
                    self.assign(keys, sources, publisher.clone()).await;

                    if main_recv.is_none() {
                        let mc = Arc::clone(&main_consumer);
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
    use crate::message_source::tests::{MockMsg, MockSource};
    use crate::messages::JobResultMessage;
    use crate::result_publisher::tests::MockPublisher;
    use tokio::sync::mpsc;

    fn mock_source(buffer: usize) -> (mpsc::Sender<MockMsg>, MockSource) {
        let (tx, rx) = mpsc::channel(buffer);
        (tx, MockSource::new(rx))
    }

    fn stub_processor(_job: crate::messages::JobMessage) -> crate::messages::JobResultMessage {
        crate::messages::JobResultMessage {
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

    type TestCoordinator = WorkerCoordinator<MockSource, MockPublisher>;

    fn new_coordinator() -> TestCoordinator {
        WorkerCoordinator::new(stub_processor)
    }

    #[tokio::test]
    async fn assign_and_revoke_direct() {
        let (tx, source) = mock_source(1);
        let publisher = MockPublisher::new();
        let mut coordinator = new_coordinator();
        let key: PartitionKey = ("topic".into(), 0);

        coordinator
            .assign(vec![key.clone()], vec![source], publisher)
            .await;
        assert_eq!(coordinator.worker_count(), 1);

        drop(tx);
        coordinator.revoke(vec![key]).await;
        assert_eq!(coordinator.worker_count(), 0);
    }

    #[tokio::test]
    async fn reassign_destroys_all_workers() {
        let (tx1, source1) = mock_source(1);
        let (_tx2, source2) = mock_source(1);
        let publisher = MockPublisher::new();

        let mut coordinator = new_coordinator();
        let key: PartitionKey = ("topic".into(), 0);

        coordinator
            .assign(vec![key.clone()], vec![source1], publisher.clone())
            .await;
        assert_eq!(coordinator.worker_count(), 1);

        coordinator.revoke_all().await;
        assert_eq!(coordinator.worker_count(), 0);

        coordinator
            .assign(vec![key.clone()], vec![source2], publisher)
            .await;
        assert_eq!(coordinator.worker_count(), 1);

        drop(tx1);
        coordinator.revoke_all().await;
    }

    #[tokio::test]
    async fn channel_integration() {
        let (tx, source) = mock_source(1);
        let key: PartitionKey = ("topic".into(), 0);
        let publisher = MockPublisher::new();

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let mut coordinator = new_coordinator();

        let main_consumer = Arc::new(
            rdkafka::ClientConfig::new()
                .create::<StreamConsumer>()
                .expect("mock consumer"),
        );

        let mut factory = Some(source);
        let handle = tokio::spawn(async move {
            coordinator
                .run(cmd_rx, main_consumer, publisher, move |_, _| factory.take())
                .await;
        });

        cmd_tx.send(Command::Assign(vec![key.clone()])).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        drop(tx);
        cmd_tx.send(Command::Revoke(vec![key])).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        drop(cmd_tx);
        handle.await.unwrap();
    }
}
