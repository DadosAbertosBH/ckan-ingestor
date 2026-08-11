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

use std::sync::Arc;
use std::time::Duration;

use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

use crate::messages::JobResultMessage;

const RESULT_TOPIC: &str = "ckan.ingest.jobs_result";

pub trait ResultPublisher: Clone {
    fn publish(
        &self,
        result: JobResultMessage,
    ) -> impl std::future::Future<Output = Result<(), anyhow::Error>> + Send;
}

impl ResultPublisher for Arc<FutureProducer> {
    fn publish(
        &self,
        result: JobResultMessage,
    ) -> impl std::future::Future<Output = Result<(), anyhow::Error>> + Send {
        let producer = Arc::clone(self);
        async move {
            let payload = serde_json::to_vec(&result)
                .map_err(|e| anyhow::anyhow!("serialize result: {}", e))?;

            let record = FutureRecord::to(RESULT_TOPIC)
                .key(&result.job_id)
                .payload(&payload);

            producer
                .send(record, Timeout::After(Duration::from_secs(10)))
                .await
                .map_err(|(e, _)| {
                    anyhow::anyhow!(
                        "producer send job={} status={}: {}",
                        result.job_id,
                        result.status,
                        e
                    )
                })?;
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Clone)]
    pub(crate) struct MockPublisher {
        pub published: Arc<Mutex<Vec<JobResultMessage>>>,
    }

    impl MockPublisher {
        pub fn new() -> Self {
            Self {
                published: Arc::new(Mutex::new(vec![])),
            }
        }
    }

    impl ResultPublisher for MockPublisher {
        fn publish(
            &self,
            result: JobResultMessage,
        ) -> impl std::future::Future<Output = Result<(), anyhow::Error>> + Send {
            self.published.lock().unwrap().push(result);
            std::future::ready(Ok(()))
        }
    }
}
