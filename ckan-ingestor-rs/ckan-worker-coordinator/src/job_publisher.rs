// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use ckan_ingestor_worker_lib::{JobMessage, JobResultMessage};
use iggy_processor::iggy::prelude::{IggyMessage, IggyProducer, Partitioning};
use message_processor::{OutgoingMessage, ResultPublisher};
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Destination {
    Results,
    Jobs,
    Retries,
}

pub trait JobTransport: Clone {
    fn send(
        &self,
        destination: Destination,
        key: &str,
        payload: String,
    ) -> impl std::future::Future<Output = anyhow::Result<()>>;
}

#[derive(Clone)]
pub struct IggyJobTransport {
    results: Arc<IggyProducer>,
    jobs: Arc<IggyProducer>,
    retries: Arc<IggyProducer>,
}
impl IggyJobTransport {
    fn new(results: IggyProducer, jobs: IggyProducer, retries: IggyProducer) -> Self {
        Self {
            results: Arc::new(results),
            jobs: Arc::new(jobs),
            retries: Arc::new(retries),
        }
    }
}
impl JobTransport for IggyJobTransport {
    async fn send(
        &self,
        destination: Destination,
        key: &str,
        payload: String,
    ) -> anyhow::Result<()> {
        let producer = match destination {
            Destination::Results => &self.results,
            Destination::Jobs => &self.jobs,
            Destination::Retries => &self.retries,
        };
        producer
            .send_with_partitioning(
                vec![IggyMessage::from_str(&payload)?],
                Some(Arc::new(Partitioning::messages_key_str(key)?)),
            )
            .await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct JobPublisher<T: JobTransport = IggyJobTransport> {
    transport: T,
}
impl JobPublisher<IggyJobTransport> {
    pub fn new(results: IggyProducer, jobs: IggyProducer, retries: IggyProducer) -> Self {
        Self {
            transport: IggyJobTransport::new(results, jobs, retries),
        }
    }
}
impl<T: JobTransport> JobPublisher<T> {
    async fn send<P: serde::Serialize>(
        &self,
        destination: Destination,
        key: &str,
        payload: &P,
    ) -> anyhow::Result<()> {
        let payload = serde_json::to_string(payload)?;
        self.transport.send(destination, key, payload).await
    }
    pub async fn pending(
        &self,
        result: &JobResultMessage,
        job: &JobMessage,
        retry: bool,
    ) -> anyhow::Result<()> {
        let id = result.job_id.as_str();
        self.send(Destination::Results, id, result).await?;
        self.send(
            if retry {
                Destination::Retries
            } else {
                Destination::Jobs
            },
            id,
            job,
        )
        .await
    }
    pub async fn skipped(&self, result: &JobResultMessage, key: &str) -> anyhow::Result<()> {
        self.send(Destination::Results, key, result).await
    }

    pub async fn result(&self, result: &JobResultMessage, key: &str) -> anyhow::Result<()> {
        self.send(Destination::Results, key, result).await
    }
}

impl<T: JobTransport + Send + 'static> ResultPublisher<JobResultMessage> for JobPublisher<T> {
    async fn publish(&self, result: JobResultMessage) -> anyhow::Result<()> {
        let key = result.partition_key().to_owned();
        self.result(&result, &key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ckan_ingestor_worker_lib::JobStatus;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct Recorder(Arc<Mutex<Vec<Destination>>>);
    impl JobTransport for Recorder {
        async fn send(&self, destination: Destination, _: &str, _: String) -> anyhow::Result<()> {
            self.0.lock().unwrap().push(destination);
            Ok(())
        }
    }
    #[tokio::test]
    async fn publishes_pending_result_before_job_message() {
        let sent = Arc::new(Mutex::new(vec![]));
        let publisher = JobPublisher {
            transport: Recorder(sent.clone()),
        };
        let result = JobResultMessage::pending("job", "resource", "dataset", "instance");
        let job = JobMessage {
            job_id: "job".into(),
            resource_id: "resource".into(),
            ckan_url: String::new(),
            resource_url: String::new(),
            resource_format: String::new(),
            csv_delimiter: None,
            datastore_active: false,
        };
        assert_eq!(result.status, JobStatus::Pending);
        publisher.pending(&result, &job, false).await.unwrap();
        assert_eq!(
            *sent.lock().unwrap(),
            vec![Destination::Results, Destination::Jobs]
        );
    }
}
