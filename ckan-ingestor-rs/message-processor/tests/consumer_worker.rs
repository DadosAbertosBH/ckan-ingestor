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
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_stream::stream;
use futures::Stream;
use message_processor::{
    BrokerMessage, MessageProcessor, OutgoingMessage, WorkerHandler,
    test_support::{MockMessageSource, MockResultPublisher},
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Deserialize)]
struct Input {
    value: u8,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Output(u8);

struct Processor;

impl MessageProcessor for Processor {
    type IncomingMessage = Input;

    fn process(&self, message: Input) -> impl Stream<Item = OutgoingMessage<impl Serialize>> {
        stream! {
            yield OutgoingMessage::new(
                "test".to_string(),
                "test".to_string(),
                Output(message.value),
            );
            yield OutgoingMessage::new(
                "test".to_string(),
                "test".to_string(),
                Output(message.value + 1),
            );
        }
    }
}

struct BlockingProcessor {
    processed: Arc<AtomicBool>,
}

impl MessageProcessor for BlockingProcessor {
    type IncomingMessage = Input;

    fn process(&self, message: Input) -> impl Stream<Item = OutgoingMessage<impl Serialize>> {
        let processed = Arc::clone(&self.processed);
        async_stream::stream! {
            yield OutgoingMessage::new(
                "test".to_string(),
                "test".to_string(),
                Output(message.value),
            );
            tokio::task::spawn_blocking(move || processed.store(true, Ordering::SeqCst))
                .await
                .unwrap();
            yield OutgoingMessage::new(
                "test".to_string(),
                "test".to_string(),
                Output(message.value + 1),
            );
        }
    }
}

#[test]
fn publishes_every_processor_output_before_committing_the_source_message() {
    let (sender, receiver) = mpsc::channel(1);
    sender
        .blocking_send(BrokerMessage {
            payload: serde_json::to_vec(&serde_json::json!({ "value": 41 })).unwrap(),
            partition: 3,
            offset: 42,
        })
        .unwrap();
    let source = MockMessageSource::new(receiver);
    let committed = source.committed.clone();
    let publisher = MockResultPublisher::new();
    let published = publisher.published.clone();
    let mut worker = WorkerHandler::new("source".into(), 0, source, publisher, Processor);

    worker.run();
    for _ in 0..100 {
        if committed.lock().unwrap().len() == 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    worker.shutdown();

    assert_eq!(
        published
            .lock()
            .unwrap()
            .iter()
            .map(|message| message.data.as_str())
            .collect::<Vec<_>>(),
        vec!["41", "42"]
    );
    assert_eq!(*committed.lock().unwrap(), vec![(3, 42)]);
}

#[test]
fn supports_a_processor_that_defers_blocking_work_until_after_its_first_output() {
    let (sender, receiver) = mpsc::channel(1);
    sender
        .blocking_send(BrokerMessage {
            payload: serde_json::to_vec(&serde_json::json!({ "value": 41 })).unwrap(),
            partition: 3,
            offset: 42,
        })
        .unwrap();
    let source = MockMessageSource::new(receiver);
    let committed = source.committed.clone();
    let processed = Arc::new(AtomicBool::new(false));
    let mut worker = WorkerHandler::new(
        "source".into(),
        0,
        source,
        MockResultPublisher::new(),
        BlockingProcessor {
            processed: Arc::clone(&processed),
        },
    );

    worker.run();
    for _ in 0..100 {
        if committed.lock().unwrap().len() == 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    worker.shutdown();

    assert!(processed.load(Ordering::SeqCst));
    assert_eq!(*committed.lock().unwrap(), vec![(3, 42)]);
}
