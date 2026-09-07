// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use anyhow::Result;
use ckan_ingestor_worker_lib::{ConsumerWorker, MessageHandler};
use message_processor::{BrokerMessage, MessageSource};
use std::future::Future;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

struct Source {
    receiver: mpsc::Receiver<BrokerMessage>,
    commits: Arc<Mutex<Vec<(u32, u64)>>>,
}

impl MessageSource for Source {
    async fn recv(&mut self) -> Result<BrokerMessage> {
        self.receiver
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("closed"))
    }

    async fn commit(&mut self, partition: u32, offset: u64) -> Result<()> {
        self.commits.lock().unwrap().push((partition, offset));
        Ok(())
    }
}

#[derive(Clone)]
struct Handler(Arc<Mutex<Vec<Vec<u8>>>>);

impl MessageHandler for Handler {
    fn handle(&self, message: &BrokerMessage) -> impl Future<Output = Result<()>> {
        let received = self.0.clone();
        let payload = message.payload.clone();
        async move {
            received.lock().unwrap().push(payload);
            Ok(())
        }
    }
}

#[test]
fn commits_only_after_the_shared_handler_succeeds() {
    let (sender, receiver) = mpsc::channel(1);
    sender
        .blocking_send(BrokerMessage {
            payload: b"message".to_vec(),
            partition: 2,
            offset: 7,
        })
        .unwrap();
    let commits = Arc::new(Mutex::new(vec![]));
    let received = Arc::new(Mutex::new(vec![]));
    let mut worker = ConsumerWorker::new(
        "topic".into(),
        0,
        Source {
            receiver,
            commits: commits.clone(),
        },
        Handler(received.clone()),
    );
    worker.run();
    for _ in 0..100 {
        if commits.lock().unwrap().len() == 1 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    worker.shutdown();
    assert_eq!(*received.lock().unwrap(), vec![b"message".to_vec()]);
    assert_eq!(*commits.lock().unwrap(), vec![(2, 7)]);
}
