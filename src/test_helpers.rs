use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use clickhouse::Client;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;
use rdkafka::Message;
use sqlx::PgPool;

use crate::sink::{ClickHouseConfig, KafkaAuth, KafkaSink, KafkaSinkConfig, Sink};

pub(crate) const FLAGSMITH_WAREHOUSE_TOPIC: &str = "events-default";

pub(crate) type MockKafkaCluster = MockCluster<'static, DefaultProducerContext>;

/// Event timestamps must be recent to pass validation.
pub(crate) fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

/// The cluster is torn down on drop, so it must outlive every request in the
/// test.
pub(crate) struct MockKafka {
    pub(crate) cluster: MockKafkaCluster,
    pub(crate) sink: Sink,
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
    let sink = Sink::Kafka(KafkaSink::new(config).expect("sink"));
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

pub(crate) const POSTGRES_EVENTS_TABLE: &str = "
    CREATE SCHEMA IF NOT EXISTS flagsmith_exp;

    CREATE TABLE IF NOT EXISTS flagsmith_exp.events (
        environment_key TEXT NOT NULL,
        event           TEXT NOT NULL,
        feature_name    TEXT,
        timestamp       TIMESTAMPTZ NOT NULL,
        collected_at    TIMESTAMPTZ,
        identifier      TEXT NOT NULL,
        value           TEXT,
        traits          TEXT,
        metadata        TEXT,
        sdk_language    TEXT,
        sdk_version     TEXT
    );

    CREATE INDEX IF NOT EXISTS events_env_event_feature_ts ON flagsmith_exp.events (environment_key, event, feature_name, timestamp);
    CREATE INDEX IF NOT EXISTS events_env_event_identifier_ts ON flagsmith_exp.events (environment_key, event, identifier, timestamp);
";

/// The production `events` table, from Flagsmith's
/// experimentation/clickhouse/events.sql.
const EVENTS_TABLE: &str = "
    CREATE TABLE {database}.events
    (
        environment_key      LowCardinality(String),
        event                LowCardinality(String),
        feature_name         LowCardinality(String),
        timestamp            DateTime64(3),
        collected_at         DateTime64(3),
        identifier           String,
        value                String                          CODEC(ZSTD(3)),
        traits               String                          CODEC(ZSTD(3)),
        metadata             String                          CODEC(ZSTD(3)),
        sdk_language         LowCardinality(String),
        sdk_version          LowCardinality(String),
        INDEX idx_identity identifier TYPE bloom_filter GRANULARITY 4,
        CONSTRAINT environment_key_not_empty CHECK environment_key != '',
        CONSTRAINT event_not_empty           CHECK event != '',
        CONSTRAINT timestamp_sane            CHECK timestamp > toDateTime64('2020-01-01 00:00:00', 3)
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMMDD(timestamp)
    ORDER BY (environment_key, event, feature_name, timestamp, identifier)
";

static NEXT_DATABASE: AtomicUsize = AtomicUsize::new(0);

/// A fresh database holding the `events` table, on the ClickHouse at
/// `CLICKHOUSE_URL` (with `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD`, as the
/// server reads them). Call `drop` at the end of the test.
pub(crate) struct ClickHouseTestDatabase {
    pub(crate) config: ClickHouseConfig,
    pub(crate) client: Client,
}

impl ClickHouseTestDatabase {
    pub(crate) async fn create() -> Self {
        let url = env::var("CLICKHOUSE_URL").expect("CLICKHOUSE_URL must be set");
        let user = env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string());
        let password = env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();
        let database = format!(
            "test_{}_{}",
            Utc::now().timestamp_millis(),
            NEXT_DATABASE.fetch_add(1, Ordering::SeqCst)
        );
        let server = Client::default()
            .with_url(&url)
            .with_user(&user)
            .with_password(&password);
        server
            .query(&format!("CREATE DATABASE {database}"))
            .execute()
            .await
            .expect("create database");
        server
            .query(&EVENTS_TABLE.replace("{database}", &database))
            .execute()
            .await
            .expect("create events table");
        Self {
            client: server.with_database(&database),
            config: ClickHouseConfig {
                url,
                user,
                password,
                database,
                table: "events".to_string(),
            },
        }
    }

    pub(crate) async fn count(&self) -> u64 {
        self.client
            .query("SELECT count() FROM events")
            .fetch_one()
            .await
            .expect("count events")
    }

    /// Polls until `n` rows are stored; flushes run in the background.
    pub(crate) async fn wait_for_rows(&self, n: u64) {
        for _ in 0..100 {
            if self.count().await >= n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("expected {n} rows within 10s, found {}", self.count().await);
    }

    pub(crate) async fn drop_db(self) {
        self.client
            .query(&format!("DROP DATABASE {}", self.config.database))
            .execute()
            .await
            .expect("drop database");
    }
}
