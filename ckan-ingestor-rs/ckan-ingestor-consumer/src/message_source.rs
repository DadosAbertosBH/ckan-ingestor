// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

pub use ckan_ingestor_worker_lib::{BrokerMessage, IggySource, MessageSource};

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use anyhow::{Result, anyhow};
    use std::sync::{Arc, Mutex as StdMutex};
    use tokio::sync::mpsc;

    pub(crate) struct MockSource {
        rx: mpsc::Receiver<BrokerMessage>,
        pub committed: Arc<StdMutex<Vec<(u32, u64)>>>,
    }

    impl MockSource {
        pub fn new(rx: mpsc::Receiver<BrokerMessage>) -> Self {
            Self {
                rx,
                committed: Arc::new(StdMutex::new(vec![])),
            }
        }
    }

    impl MessageSource for MockSource {
        async fn recv(&mut self) -> Result<BrokerMessage> {
            self.rx
                .recv()
                .await
                .ok_or_else(|| anyhow!("mock source closed"))
        }

        async fn commit(&mut self, partition: u32, offset: u64) -> Result<()> {
            self.committed.lock().unwrap().push((partition, offset));
            Ok(())
        }
    }
}
