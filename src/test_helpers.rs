use std::time::Duration;

use chrono::{DateTime, Utc};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;
use rdkafka::Message;
use sqlx::PgPool;

use crate::sink::{KafkaAuth, KafkaSink, KafkaSinkConfig};

pub(crate) const FLAGSMITH_WAREHOUSE_TOPIC: &str = "events-default";

pub(crate) type MockKafkaCluster = MockCluster<'static, DefaultProducerContext>;

/// The cluster is torn down on drop, so it must outlive every request in the
/// test.
pub(crate) struct MockKafka {
    pub(crate) cluster: MockKafkaCluster,
    pub(crate) sink: KafkaSink,
}

pub(crate) fn mock_kafka(topics: &[&str]) -> MockKafka {
    let cluster = MockCluster::new(1).expect("mock cluster");
    for topic in topics {
        cluster.create_topic(topic, 1, 1).expect("create topic");
    }
    let mut config = KafkaSinkConfig::new(
        cluster.bootstrap_servers(),
        FLAGSMITH_WAREHOUSE_TOPIC.to_string(),
        KafkaAuth::None,
    );
    config.message_timeout = Duration::from_secs(5);
    let sink = KafkaSink::new(config).expect("sink");
    MockKafka { cluster, sink }
}

fn topic_consumer(cluster: &MockKafkaCluster, topic: &str) -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", cluster.bootstrap_servers())
        .set("group.id", "test-consumer")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    consumer
}

pub(crate) async fn read_records(
    cluster: &MockKafkaCluster,
    topic: &str,
    n: usize,
) -> Vec<serde_json::Value> {
    let consumer = topic_consumer(cluster, topic);
    let mut records = Vec::with_capacity(n);
    for _ in 0..n {
        let message = tokio::time::timeout(Duration::from_secs(15), consumer.recv())
            .await
            .expect("record within 15s")
            .expect("recv");
        let payload = message.payload().expect("record has a payload");
        records.push(serde_json::from_slice(payload).expect("record is valid JSON"));
    }
    records
}

/// `put` waits for the acks before the handler answers, so anything wrongly
/// produced is already committed.
pub(crate) fn assert_topic_empty(cluster: &MockKafkaCluster, topic: &str) {
    let consumer = topic_consumer(cluster, topic);
    let (_low, high) = consumer
        .fetch_watermarks(topic, 0, Duration::from_secs(5))
        .expect("watermarks");
    assert_eq!(high, 0, "expected no records on {topic}");
}

pub(crate) async fn create_environment_keys(pool: &PgPool) {
    sqlx::query(
        "CREATE TABLE experimentation_environment_keys (
            sdk_key text PRIMARY KEY,
            client_api_key text NOT NULL,
            uses_external_warehouse boolean NOT NULL,
            expires_at timestamptz
        )",
    )
    .execute(pool)
    .await
    .unwrap();
}

pub(crate) async fn insert_environment_key(
    pool: &PgPool,
    sdk_key: &str,
    uses_external_warehouse: bool,
    expires_at: Option<DateTime<Utc>>,
) {
    sqlx::query(
        "INSERT INTO experimentation_environment_keys \
         VALUES ($1, 'client-api-key', $2, $3)",
    )
    .bind(sdk_key)
    .bind(uses_external_warehouse)
    .bind(expires_at)
    .execute(pool)
    .await
    .unwrap();
}
