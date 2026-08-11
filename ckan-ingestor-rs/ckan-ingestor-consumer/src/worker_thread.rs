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

// ---------------------------------------------------------------------------
// WorkerThread
// ---------------------------------------------------------------------------

/// A per-partition worker that processes messages sequentially from its
/// dedicated queue.
pub struct WorkerThread<M: MessageSource> {
    pub topic: String,
    pub partition: i32,
    source: Option<M>,
    handle: Option<JoinHandle<()>>,
    shutdown: Arc<Notify>,
}

impl<M: MessageSource + Send + 'static> WorkerThread<M> {
    pub fn new(topic: String, partition: i32, source: M) -> Self {
        Self {
            topic,
            partition,
            source: Some(source),
            handle: None,
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Spawns the internal tokio task and starts the event loop.
    pub fn run(&mut self) {
        let source = self.source.take().expect("WorkerThread::run called twice");
        let shutdown = self.shutdown.clone();
        let topic = self.topic.clone();
        let partition = self.partition;

        let handle = tokio::spawn(async move {
            Self::run_loop(topic, partition, source, shutdown).await;
        });
        self.handle = Some(handle);
    }

    async fn run_loop(topic: String, partition: i32, source: M, shutdown: Arc<Notify>) {
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
                            let _ = m.payload();
                            let _ = m.offset();
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
            let _ = handle.await;
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
    async fn run_and_shutdown() {
        let (_tx, source) = mock_source(1);

        let mut worker = WorkerThread::new("test".into(), 0, source);
        worker.run();

        assert_eq!(worker.topic, "test");
        assert_eq!(worker.partition, 0);

        worker.shutdown().await;
    }

    #[tokio::test]
    async fn worker_keeps_running_on_closed_channel() {
        let (tx, source) = mock_source(1);
        drop(tx);
        let mut worker = WorkerThread::new("t".into(), 0, source);
        worker.run();
        worker.shutdown().await;
    }
}
