use rdkafka::client::ClientContext;
use rdkafka::consumer::ConsumerContext;

struct CoordinatorConsumerContext;

impl CoordinatorConsumerContext {}

impl ClientContext for CoordinatorConsumerContext {}
impl ConsumerContext for CoordinatorConsumerContext {
    fn pre_rebalance(
        &self,
        _base_consumer: &rdkafka::consumer::BaseConsumer<Self>,
        _rebalance: &rdkafka::consumer::Rebalance<'_>,
    ) {
    }

    fn post_rebalance(
        &self,
        _base_consumer: &rdkafka::consumer::BaseConsumer<Self>,
        _rebalance: &rdkafka::consumer::Rebalance<'_>,
    ) {
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
