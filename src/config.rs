use std::env;
use std::str::FromStr;
use std::time::Duration;

use crate::sink::{BatchConfig, ClickHouseSinkConfig, KafkaAuth};

#[derive(Debug, Clone)]
pub struct Config {
    pub listen_addr: String,
    pub sink: SinkConfig,
    pub database_url: String,
}

#[derive(Debug, Clone)]
pub enum SinkConfig {
    Kafka(KafkaConfig),
    ClickHouse {
        clickhouse: ClickHouseSinkConfig,
        batch: BatchConfig,
    },
}

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    pub external_topic: String,
    pub auth: KafkaAuth,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let sink = match parse_sink(env::var("SINK").ok().as_deref())? {
            SinkKind::Kafka => SinkConfig::Kafka(KafkaConfig::from_env()?),
            SinkKind::ClickHouse => SinkConfig::ClickHouse {
                clickhouse: clickhouse_from_env()?,
                batch: batch_from_env()?,
            },
        };

        Ok(Config {
            listen_addr: env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string()),
            sink,
            database_url: required("DATABASE_URL")?,
        })
    }
}

impl KafkaConfig {
    fn from_env() -> Result<Self, ConfigError> {
        let auth = parse_auth(
            &env::var("KAFKA_AUTH").unwrap_or_else(|_| "scram".to_string()),
            env::var("KAFKA_USERNAME").ok(),
            env::var("KAFKA_PASSWORD").ok(),
        )?;

        Ok(KafkaConfig {
            bootstrap_servers: required("KAFKA_BOOTSTRAP_SERVERS")?,
            topic: required("KAFKA_TOPIC")?,
            external_topic: required("KAFKA_EXTERNAL_TOPIC")?,
            auth,
        })
    }
}

fn required(name: &'static str) -> Result<String, ConfigError> {
    env::var(name).map_err(|_| ConfigError::MissingRequired(name))
}

#[derive(Debug, PartialEq)]
enum SinkKind {
    Kafka,
    ClickHouse,
}

/// Which sink `SINK` selects; unset means Kafka.
fn parse_sink(value: Option<&str>) -> Result<SinkKind, ConfigError> {
    match value.unwrap_or("kafka") {
        "kafka" => Ok(SinkKind::Kafka),
        "clickhouse" => Ok(SinkKind::ClickHouse),
        other => Err(ConfigError::InvalidSink(other.to_string())),
    }
}

fn clickhouse_from_env() -> Result<ClickHouseSinkConfig, ConfigError> {
    Ok(ClickHouseSinkConfig {
        url: required("CLICKHOUSE_URL")?,
        user: env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string()),
        password: env::var("CLICKHOUSE_PASSWORD").unwrap_or_default(),
        database: env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "flagsmith_exp".to_string()),
        table: env::var("CLICKHOUSE_TABLE").unwrap_or_else(|_| "events".to_string()),
    })
}

/// Shared by every batching sink.
fn batch_from_env() -> Result<BatchConfig, ConfigError> {
    Ok(BatchConfig {
        flush_interval: Duration::from_millis(parse_positive(
            "SINK_FLUSH_INTERVAL_MS",
            env::var("SINK_FLUSH_INTERVAL_MS").ok(),
            1000,
        )?),
        max_rows: parse_positive(
            "SINK_BATCH_MAX_ROWS",
            env::var("SINK_BATCH_MAX_ROWS").ok(),
            50_000,
        )?,
        max_bytes: parse_positive(
            "SINK_BUFFER_MAX_BYTES",
            env::var("SINK_BUFFER_MAX_BYTES").ok(),
            64 * 1024 * 1024,
        )?,
    })
}

fn parse_positive<T: FromStr + Default + PartialOrd>(
    name: &'static str,
    value: Option<String>,
    default: T,
) -> Result<T, ConfigError> {
    match value {
        None => Ok(default),
        Some(v) => match v.parse() {
            Ok(n) if n > T::default() => Ok(n),
            _ => Err(ConfigError::InvalidNumber(name, v)),
        },
    }
}

fn parse_auth(
    mode: &str,
    username: Option<String>,
    password: Option<String>,
) -> Result<KafkaAuth, ConfigError> {
    match mode {
        "scram" => Ok(KafkaAuth::Scram {
            username: username.ok_or(ConfigError::MissingRequired("KAFKA_USERNAME"))?,
            password: password.ok_or(ConfigError::MissingRequired("KAFKA_PASSWORD"))?,
        }),
        "none" => Ok(KafkaAuth::None),
        other => Err(ConfigError::Invalid("KAFKA_AUTH", other.to_string())),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Missing required environment variable: {0}")]
    MissingRequired(&'static str),
    #[error("Invalid value for {0}: {1:?} (expected scram or none)")]
    Invalid(&'static str, String),
    #[error("Invalid value for SINK: {0:?} (expected kafka or clickhouse)")]
    InvalidSink(String),
    #[error("Invalid value for {0}: {1:?} (expected a positive integer)")]
    InvalidNumber(&'static str, String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_auth_builds_scram_from_credentials() {
        let auth = parse_auth("scram", Some("u".to_string()), Some("p".to_string())).unwrap();
        assert_eq!(
            auth,
            KafkaAuth::Scram {
                username: "u".to_string(),
                password: "p".to_string(),
            }
        );
    }

    #[test]
    fn parse_auth_requires_both_scram_credentials() {
        assert!(matches!(
            parse_auth("scram", Some("u".to_string()), None),
            Err(ConfigError::MissingRequired("KAFKA_PASSWORD"))
        ));
        assert!(matches!(
            parse_auth("scram", None, Some("p".to_string())),
            Err(ConfigError::MissingRequired("KAFKA_USERNAME"))
        ));
    }

    #[test]
    fn parse_auth_accepts_none_without_credentials() {
        assert_eq!(parse_auth("none", None, None).unwrap(), KafkaAuth::None);
    }

    #[test]
    fn parse_auth_rejects_unknown_value() {
        assert!(matches!(
            parse_auth("iam", None, None),
            Err(ConfigError::Invalid("KAFKA_AUTH", v)) if v == "iam"
        ));
    }

    #[test]
    fn parse_sink_defaults_to_kafka() {
        assert_eq!(parse_sink(None).unwrap(), SinkKind::Kafka);
        assert_eq!(parse_sink(Some("kafka")).unwrap(), SinkKind::Kafka);
        assert_eq!(
            parse_sink(Some("clickhouse")).unwrap(),
            SinkKind::ClickHouse
        );
    }

    #[test]
    fn parse_sink_rejects_unknown_value() {
        assert!(matches!(
            parse_sink(Some("postgres")),
            Err(ConfigError::InvalidSink(v)) if v == "postgres"
        ));
    }

    #[test]
    fn parse_positive_defaults_when_unset() {
        assert_eq!(parse_positive("N", None, 1000u64).unwrap(), 1000);
    }

    #[test]
    fn parse_positive_reads_a_positive_integer() {
        assert_eq!(
            parse_positive("N", Some("50".to_string()), 1000usize).unwrap(),
            50
        );
    }

    #[test]
    fn parse_positive_rejects_zero_and_non_numbers() {
        for value in ["0", "-1", "ten", ""] {
            assert!(matches!(
                parse_positive("N", Some(value.to_string()), 1000u64),
                Err(ConfigError::InvalidNumber("N", v)) if v == value
            ));
        }
    }
}
