use std::fmt;
use std::time::Duration;

use futures::future::join_all;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::statistics::Statistics;
use rdkafka::util::Timeout;
use tracing::{debug, error, info, warn};

const CLIENT_ID: &str = "events-ingestion-api";
/// How long `put` waits for room in the local queue before failing the batch.
const ENQUEUE_TIMEOUT: Duration = Duration::from_millis(500);
/// Records for a topic the brokers do not know fail after this, not after the
/// 30 s librdkafka default that outlives the message timeout.
const UNKNOWN_TOPIC_TIMEOUT_MS: &str = "3000";

/// How the producer authenticates to the brokers.
#[derive(Clone, PartialEq, Eq)]
pub enum KafkaAuth {
    /// SASL/SCRAM-SHA-512 over TLS. MSK serves it on port 9096.
    Scram { username: String, password: String },
    /// No authentication, plaintext. Local brokers and tests only.
    None,
}

/// Keeps the password out of logs.
impl fmt::Debug for KafkaAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scram { username, .. } => f
                .debug_struct("Scram")
                .field("username", username)
                .finish_non_exhaustive(),
            Self::None => f.write_str("None"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct KafkaSinkConfig {
    pub bootstrap_servers: String,
    pub default_topic: String,
    pub auth: KafkaAuth,
    /// Bound on a record's time in the producer once queued. A dead broker
    /// turns into a failed request after this plus the enqueue wait.
    pub message_timeout: Duration,
}

impl KafkaSinkConfig {
    pub fn new(bootstrap_servers: String, default_topic: String, auth: KafkaAuth) -> Self {
        Self {
            bootstrap_servers,
            default_topic,
            auth,
            message_timeout: Duration::from_secs(10),
        }
    }
}

/// Routes librdkafka's logs, stats and errors into tracing.
struct SinkContext;

impl ClientContext for SinkContext {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, message: &str) {
        match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => error!(facility = fac, "{message}"),
            RDKafkaLogLevel::Warning => warn!(facility = fac, "{message}"),
            RDKafkaLogLevel::Notice | RDKafkaLogLevel::Info => info!(facility = fac, "{message}"),
            RDKafkaLogLevel::Debug => debug!(facility = fac, "{message}"),
        }
    }

    fn stats(&self, statistics: Statistics) {
        info!(
            delivered = statistics.txmsgs,
            delivered_bytes = statistics.txmsg_bytes,
            queued = statistics.msg_cnt,
            "Kafka producer stats"
        );
    }

    fn error(&self, error: KafkaError, reason: &str) {
        error!(error = %error, reason, "Kafka client error");
    }
}

#[derive(Clone)]
pub struct KafkaSink {
    producer: FutureProducer<SinkContext>,
    default_topic: String,
}

impl KafkaSink {
    /// Builds the producer. Brokers are contacted lazily, so this fails only
    /// on settings librdkafka rejects.
    pub fn new(config: KafkaSinkConfig) -> Result<Self, KafkaError> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("client.id", CLIENT_ID)
            .set("enable.idempotence", "true")
            .set("compression.type", "zstd")
            .set(
                "message.timeout.ms",
                config.message_timeout.as_millis().to_string(),
            )
            .set("request.timeout.ms", "5000")
            .set("socket.connection.setup.timeout.ms", "3000")
            .set(
                "topic.metadata.propagation.max.ms",
                UNKNOWN_TOPIC_TIMEOUT_MS,
            )
            .set("queue.buffering.max.messages", "200000")
            .set("queue.buffering.max.kbytes", "131072")
            .set("statistics.interval.ms", "60000")
            .set_log_level(RDKafkaLogLevel::Info);
        match &config.auth {
            KafkaAuth::Scram { username, password } => {
                client_config
                    .set("security.protocol", "SASL_SSL")
                    .set("sasl.mechanisms", "SCRAM-SHA-512")
                    .set("sasl.username", username)
                    .set("sasl.password", password);
            }
            KafkaAuth::None => {
                client_config.set("security.protocol", "PLAINTEXT");
            }
        }
        let producer = client_config.create_with_context(SinkContext)?;
        Ok(Self {
            producer,
            default_topic: config.default_topic,
        })
    }

    /// Produce every record to the destination topic (the default when `None`)
    /// and wait for the brokers to acknowledge all of them. Fails if any record
    /// is not acknowledged within the message timeout plus the enqueue wait; the
    /// caller surfaces that to the SDK, which retries the whole batch.
    pub async fn put(
        &self,
        destination: Option<&str>,
        records: Vec<Vec<u8>>,
    ) -> Result<(), SinkError> {
        let topic = destination.unwrap_or(&self.default_topic);
        let total = records.len();
        let sends = records.iter().map(|bytes| {
            self.producer.send(
                FutureRecord::<(), _>::to(topic).payload(bytes),
                Timeout::After(ENQUEUE_TIMEOUT),
            )
        });
        let mut failures = join_all(sends).await.into_iter().filter_map(Result::err);
        match failures.next() {
            None => Ok(()),
            Some((error, _record)) => Err(SinkError::Delivery {
                failed: 1 + failures.count(),
                total,
                error,
            }),
        }
    }

    /// A fatal producer error (idempotence violation, fenced producer) is
    /// permanent: every produce fails until the task is replaced. Local flag,
    /// no network call.
    pub fn fatal_error(&self) -> Option<String> {
        self.producer
            .client()
            .fatal_error()
            .map(|(code, message)| format!("{code}: {message}"))
    }

    /// Drain records still queued from requests dropped mid-flight. Answered
    /// requests hold nothing here: `put` waits for the acks.
    pub async fn shutdown(self, timeout: Duration) {
        let producer = self.producer;
        match tokio::task::spawn_blocking(move || producer.flush(Timeout::After(timeout))).await {
            Ok(Ok(())) => info!("Kafka producer flushed"),
            Ok(Err(e)) => warn!(error = %e, "Kafka producer flush incomplete"),
            Err(e) => warn!(error = %e, "Kafka producer flush task failed"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error("Kafka delivery failed for {failed} of {total} records: {error}")]
    Delivery {
        failed: usize,
        total: usize,
        error: KafkaError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scram() -> KafkaAuth {
        KafkaAuth::Scram {
            username: "producer".to_string(),
            password: "hunter2".to_string(),
        }
    }

    /// librdkafka rejects SCRAM at creation when built without it; no broker is contacted.
    #[test]
    fn scram_settings_are_accepted() {
        let config = KafkaSinkConfig::new("127.0.0.1:1".to_string(), "events".to_string(), scram());
        KafkaSink::new(config).expect("producer with SCRAM settings");
    }

    #[test]
    fn debug_output_omits_the_password() {
        let shown = format!("{:?}", scram());
        assert!(shown.contains("producer"));
        assert!(!shown.contains("hunter2"));
    }
}
