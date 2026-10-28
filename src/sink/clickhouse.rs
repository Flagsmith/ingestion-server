use std::borrow::Cow;
use std::fmt;
use std::time::Duration;

use clickhouse::{Client, Compression, Row};
use serde::Serialize;

use super::batching::BatchWriter;
use super::SinkError;
use crate::models::EventRecord;

/// Bounds sending and committing one batch of up to `SINK_FLUSH_MAX_EVENTS` rows.
const INSERT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct ClickHouseConfig {
    pub url: String,
    pub user: String,
    pub password: String,
    pub database: String,
    pub table: String,
}

/// Keeps the password out of logs.
impl fmt::Debug for ClickHouseConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickHouseConfig")
            .field("url", &self.url)
            .field("user", &self.user)
            .field("database", &self.database)
            .field("table", &self.table)
            .finish_non_exhaustive()
    }
}

/// Field names must match the `events` columns. Every column is
/// non-Nullable, so absent optionals are written as "", as ClickPipes does.
#[derive(Debug, PartialEq, Row, Serialize)]
struct ClickHouseRow<'a> {
    environment_key: &'a str,
    event: &'a str,
    feature_name: &'a str,
    /// DateTime64(3), sent as epoch millis.
    timestamp: i64,
    collected_at: i64,
    identifier: &'a str,
    value: Cow<'a, str>,
    traits: String,
    metadata: String,
    sdk_language: &'a str,
    sdk_version: &'a str,
}

impl<'a> From<&'a EventRecord> for ClickHouseRow<'a> {
    fn from(record: &'a EventRecord) -> Self {
        ClickHouseRow {
            environment_key: &record.environment_key,
            event: &record.event,
            feature_name: record.feature_name.as_deref().unwrap_or_default(),
            timestamp: record.timestamp,
            collected_at: record.collected_at,
            identifier: record.identifier.as_deref().unwrap_or_default(),
            value: value_to_text(&record.value),
            traits: record
                .traits
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
            metadata: record
                .metadata
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
            sdk_language: &record.sdk_language,
            sdk_version: &record.sdk_version,
        }
    }
}

fn value_to_text(value: &serde_json::Value) -> Cow<'_, str> {
    match value {
        serde_json::Value::String(s) => Cow::Borrowed(s),
        serde_json::Value::Null => Cow::Borrowed(""),
        other => Cow::Owned(other.to_string()),
    }
}

pub struct ClickHouseWriter {
    client: Client,
    table: String,
}

impl ClickHouseWriter {
    /// Connects lazily, on the first insert.
    pub fn new(config: ClickHouseConfig) -> Self {
        let client = Client::default()
            .with_url(config.url)
            .with_user(config.user)
            .with_password(config.password)
            .with_database(config.database)
            .with_compression(Compression::Lz4);
        Self {
            client,
            table: config.table,
        }
    }
}

impl BatchWriter for ClickHouseWriter {
    /// One plain `INSERT` per batch; succeeds once ClickHouse has committed it.
    async fn write(&self, records: &[EventRecord]) -> Result<(), SinkError> {
        let mut insert = self
            .client
            .insert::<ClickHouseRow<'_>>(&self.table)
            .await?
            .with_timeouts(Some(INSERT_TIMEOUT), Some(INSERT_TIMEOUT));
        for record in records {
            insert.write(&ClickHouseRow::from(record)).await?;
        }
        insert.end().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::json;

    use super::*;
    use crate::sink::{BatchConfig, BatchingSink};
    use crate::test_helpers::{now_ms, ClickHouseTestDatabase};

    fn record(event: &str, timestamp: i64) -> EventRecord {
        EventRecord {
            environment_key: "env".to_string(),
            collected_at: timestamp + 1,
            sdk_language: "python".to_string(),
            sdk_version: "1.0.0".to_string(),
            event: event.to_string(),
            feature_name: None,
            identifier: None,
            value: serde_json::Value::Null,
            traits: None,
            metadata: None,
            timestamp,
        }
    }

    #[test]
    fn test_absent_optionals_become_empty_strings() {
        let record = EventRecord {
            value: json!(42),
            traits: Some(json!({"plan": "pro"})),
            ..record("purchase", 1_700_000_000_000)
        };

        assert_eq!(
            ClickHouseRow::from(&record),
            ClickHouseRow {
                environment_key: "env",
                event: "purchase",
                feature_name: "",
                timestamp: 1_700_000_000_000,
                collected_at: 1_700_000_000_001,
                identifier: "",
                value: Cow::Borrowed("42"),
                traits: r#"{"plan":"pro"}"#.to_string(),
                metadata: String::new(),
                sdk_language: "python",
                sdk_version: "1.0.0",
            }
        );
    }

    #[test]
    fn test_value_to_text_leaves_strings_unquoted() {
        assert_eq!(value_to_text(&json!("control")), "control");
        assert_eq!(value_to_text(&json!(null)), "");
        assert_eq!(value_to_text(&json!(42)), "42");
        assert_eq!(value_to_text(&json!(true)), "true");
        assert_eq!(value_to_text(&json!({"a": [1, 2]})), r#"{"a":[1,2]}"#);
    }

    #[test]
    fn test_debug_output_omits_the_password() {
        let config = ClickHouseConfig {
            url: "https://clickhouse:8443".to_string(),
            user: "ingest".to_string(),
            password: "hunter2".to_string(),
            database: "flagsmith_exp".to_string(),
            table: "events".to_string(),
        };
        let shown = format!("{config:?}");
        assert!(shown.contains("ingest"));
        assert!(!shown.contains("hunter2"));
    }

    fn batch_config(flush_interval: Duration) -> BatchConfig {
        BatchConfig {
            flush_interval,
            flush_max_events: 1000,
        }
    }

    /// A row as read back from the table.
    #[derive(Debug, PartialEq, Row, Deserialize)]
    struct StoredRow {
        environment_key: String,
        event: String,
        feature_name: String,
        timestamp: i64,
        collected_at: i64,
        identifier: String,
        value: String,
        traits: String,
        metadata: String,
        sdk_language: String,
        sdk_version: String,
    }

    #[tokio::test]
    async fn test_flushed_rows_land_in_the_events_table() {
        let db = ClickHouseTestDatabase::create().await;
        let sink = BatchingSink::new(
            ClickHouseWriter::new(db.config.clone()),
            batch_config(Duration::from_millis(100)),
        );
        let ts = now_ms();
        let records = vec![
            EventRecord {
                feature_name: Some("dark_mode".to_string()),
                identifier: Some("user_1".to_string()),
                value: json!("control"),
                traits: Some(json!({"plan": "pro"})),
                metadata: Some(json!({"source": "csv"})),
                ..record("$flag_exposure", ts)
            },
            record("purchase", ts + 2),
        ];

        sink.put(records);
        db.wait_for_rows(2).await;

        let stored: Vec<StoredRow> = db
            .client
            .query("SELECT ?fields FROM events ORDER BY timestamp")
            .fetch_all()
            .await
            .unwrap();
        assert_eq!(
            stored,
            vec![
                StoredRow {
                    environment_key: "env".to_string(),
                    event: "$flag_exposure".to_string(),
                    feature_name: "dark_mode".to_string(),
                    timestamp: ts,
                    collected_at: ts + 1,
                    identifier: "user_1".to_string(),
                    value: "control".to_string(),
                    traits: r#"{"plan":"pro"}"#.to_string(),
                    metadata: r#"{"source":"csv"}"#.to_string(),
                    sdk_language: "python".to_string(),
                    sdk_version: "1.0.0".to_string(),
                },
                StoredRow {
                    environment_key: "env".to_string(),
                    event: "purchase".to_string(),
                    feature_name: String::new(),
                    timestamp: ts + 2,
                    collected_at: ts + 3,
                    identifier: String::new(),
                    value: String::new(),
                    traits: String::new(),
                    metadata: String::new(),
                    sdk_language: "python".to_string(),
                    sdk_version: "1.0.0".to_string(),
                },
            ]
        );

        sink.shutdown(Duration::from_secs(5)).await;
        db.drop_db().await;
    }

    #[tokio::test]
    async fn test_shutdown_writes_buffered_rows() {
        let db = ClickHouseTestDatabase::create().await;
        let sink = BatchingSink::new(
            ClickHouseWriter::new(db.config.clone()),
            batch_config(Duration::from_secs(3600)),
        );
        let record = record("purchase", now_ms());

        sink.put(vec![record.clone(), record.clone(), record]);
        assert_eq!(db.count().await, 0);
        sink.shutdown(Duration::from_secs(5)).await;

        assert_eq!(db.count().await, 3);
        db.drop_db().await;
    }
}
