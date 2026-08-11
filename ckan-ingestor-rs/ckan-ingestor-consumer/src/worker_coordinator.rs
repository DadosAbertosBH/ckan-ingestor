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

use tokio::sync::mpsc::UnboundedReceiver;

use crate::message_source::MessageSource;
use crate::worker_thread::WorkerThread;

type PartitionKey = (String, i32);

pub enum Command {
    Assign(Vec<PartitionKey>),
    Revoke(Vec<PartitionKey>),
}

pub struct WorkerCoordinator<M: MessageSource + Send + 'static> {
    workers: HashMap<PartitionKey, WorkerThread<M>>,
}

impl<M: MessageSource + Send + 'static> WorkerCoordinator<M> {
    pub fn new() -> Self {
        Self {
            workers: HashMap::new(),
        }
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    pub async fn assign(&mut self, keys: Vec<PartitionKey>, sources: Vec<M>) {
        for (key, source) in keys.into_iter().zip(sources) {
            let mut worker = WorkerThread::new(key.0.clone(), key.1, source);
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

    /// Runs the coordinator loop: receives commands from the channel and
    /// calls `create_source` for each assigned partition.
    pub async fn run<F>(&mut self, mut rx: UnboundedReceiver<Command>, mut create_source: F)
    where
        F: FnMut(&PartitionKey) -> Option<M>,
    {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::Assign(keys) => {
                    let sources: Vec<M> = keys.iter().filter_map(|k| create_source(k)).collect();
                    self.assign(keys, sources).await;
                }
                Command::Revoke(keys) => self.revoke(keys).await,
            }
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
    use tokio::sync::mpsc;

    fn mock_source(buffer: usize) -> (mpsc::Sender<MockMsg>, MockSource) {
        let (tx, rx) = mpsc::channel(buffer);
        (tx, MockSource::new(rx))
    }

    #[tokio::test]
    async fn assign_and_revoke_direct() {
        let (tx, source) = mock_source(1);
        let mut coordinator: WorkerCoordinator<MockSource> = WorkerCoordinator::new();
        let key: PartitionKey = ("topic".into(), 0);

        coordinator.assign(vec![key.clone()], vec![source]).await;
        assert_eq!(coordinator.worker_count(), 1);

        drop(tx);
        coordinator.revoke(vec![key]).await;
        assert_eq!(coordinator.worker_count(), 0);
    }

    #[tokio::test]
    async fn channel_integration() {
        let (tx, source) = mock_source(1);
        let key: PartitionKey = ("topic".into(), 0);

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let mut coordinator: WorkerCoordinator<MockSource> = WorkerCoordinator::new();

        // Spawn coordinator so it processes commands in the background
        let mut factory = Some(source);
        let handle = tokio::spawn(async move {
            coordinator.run(cmd_rx, |_| factory.take()).await;
        });

        // Simulate post_rebalance: send Assign through the channel
        cmd_tx.send(Command::Assign(vec![key.clone()])).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        drop(tx);

        // Simulate pre_rebalance: send Revoke through the channel
        cmd_tx.send(Command::Revoke(vec![key])).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Drop sender to stop the coordinator loop
        drop(cmd_tx);
        handle.await.unwrap();
    }
}
