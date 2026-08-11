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

use rdkafka::client::ClientContext;
use rdkafka::consumer::{ConsumerContext, Rebalance};
use tokio::sync::mpsc::UnboundedSender;

use crate::worker_coordinator::Command;

pub struct CoordinatorConsumerContext {
    pub(crate) cmd_tx: UnboundedSender<Command>,
}

impl CoordinatorConsumerContext {
    pub fn new(cmd_tx: UnboundedSender<Command>) -> Self {
        Self { cmd_tx }
    }
}

impl ClientContext for CoordinatorConsumerContext {}

impl ConsumerContext for CoordinatorConsumerContext {
    fn pre_rebalance(
        &self,
        _base_consumer: &rdkafka::consumer::BaseConsumer<Self>,
        rebalance: &Rebalance<'_>,
    ) {
        if let Rebalance::Revoke(tpl) = rebalance {
            let revoked = Self::extract_partitions(tpl);
            let _ = self.cmd_tx.send(Command::Revoke(revoked));
        }
    }

    fn post_rebalance(
        &self,
        _base_consumer: &rdkafka::consumer::BaseConsumer<Self>,
        rebalance: &Rebalance<'_>,
    ) {
        if let Rebalance::Assign(tpl) = rebalance {
            let assigned = Self::extract_partitions(tpl);
            let _ = self.cmd_tx.send(Command::Assign(assigned));
        }
    }

    fn commit_callback(
        &self,
        _result: rdkafka::error::KafkaResult<()>,
        _offsets: &rdkafka::TopicPartitionList,
    ) {
    }

    fn main_queue_min_poll_interval(&self) -> rdkafka::util::Timeout {
        rdkafka::util::Timeout::After(std::time::Duration::from_secs(1))
    }
}

impl CoordinatorConsumerContext {
    fn extract_partitions(tpl: &rdkafka::TopicPartitionList) -> Vec<(String, i32)> {
        tpl.elements()
            .iter()
            .map(|e| (e.topic().to_string(), e.partition()))
            .collect()
    }
}
