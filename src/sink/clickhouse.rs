use std::borrow::Cow;
use std::fmt;
use std::time::Duration;

use clickhouse::error::Error;
use clickhouse::{Client, Compression, Row};
use serde::Serialize;

use super::batching::BatchWriter;
use super::{ErrorKind, SinkError};
use crate::models::EventRecord;

/// Bounds sending and committing one batch of up to `SINK_FLUSH_MAX_EVENTS` rows.
const INSERT_TIMEOUT: Duration = Duration::from_secs(30);

/// Server error codes for rows the table rejects.
const BAD_DATA_ERROR_CODES: [u32; 5] = [
    6,   // CANNOT_PARSE_TEXT
    27,  // CANNOT_PARSE_INPUT_ASSERTION_FAILED
    53,  // TYPE_MISMATCH
    117, // INCORRECT_DATA
    469, // VIOLATED_CONSTRAINT
];

/// Server error codes for bad credentials or grants, or a missing database,
/// table or column.
const CONFIG_ERROR_CODES: [u32; 7] = [
    16,  // NO_SUCH_COLUMN_IN_TABLE
    60,  // UNKNOWN_TABLE
    81,  // UNKNOWN_DATABASE
    192, // UNKNOWN_USER
    194, // REQUIRED_PASSWORD
    497, // ACCESS_DENIED
    516, // AUTHENTICATION_FAILED
];

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

/// Anything unrecognised, such as a network error, timeout or overloaded
/// server, counts as transient.
pub(super) fn error_kind(error: &Error) -> ErrorKind {
    match error {
        // A row that fails to serialize.
        Error::Custom(_) => ErrorKind::BadData,
        // `ClickHouseRow` doesn't match the table.
        Error::SchemaMismatch(_) => ErrorKind::Config,
        Error::BadResponse(reason) => match error_code(reason) {
            Some(code) if BAD_DATA_ERROR_CODES.contains(&code) => ErrorKind::BadData,
            Some(code) if CONFIG_ERROR_CODES.contains(&code) => ErrorKind::Config,
            _ => ErrorKind::Transient,
        },
        _ => ErrorKind::Transient,
    }
}

/// The code of a server exception, e.g. 60 in "Code: 60. DB::Exception: ...".
fn error_code(reason: &str) -> Option<u32> {
    let rest = reason.strip_prefix("Code: ")?;
    rest[..rest.find('.')?].parse().ok()
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

    /// Fails if ClickHouse is unreachable, rejects the credentials, or the
    /// table's columns don't match `ClickHouseRow`. Column types are only
    /// checked on the first write.
    pub async fn check(&self) -> Result<(), SinkError> {
        // Opening an insert fetches the table's schema and matches it against
        // the row; dropping it unused sends nothing.
        let _ = self.client.insert::<ClickHouseRow<'_>>(&self.table).await?;
        Ok(())
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

    #[test]
    fn test_error_code_reads_the_server_exception_code() {
        assert_eq!(
            error_code("Code: 60. DB::Exception: Table x.events does not exist."),
            Some(60)
        );
        assert_eq!(error_code("Code: abc. DB::Exception"), None);
        assert_eq!(error_code("502 Bad Gateway"), None);
    }

    #[test]
    fn test_error_kind_separates_bad_data_and_config_from_outages() {
        let bad_response = |reason: &str| Error::BadResponse(reason.to_string());
        let cases = [
            (
                bad_response("Code: 469. DB::Exception: Constraint violated"),
                ErrorKind::BadData,
            ),
            (Error::Custom("bad row".to_string()), ErrorKind::BadData),
            (
                bad_response("Code: 516. DB::Exception: Authentication failed"),
                ErrorKind::Config,
            ),
            (
                bad_response("Code: 81. DB::Exception: Database x does not exist"),
                ErrorKind::Config,
            ),
            (Error::SchemaMismatch("x".to_string()), ErrorKind::Config),
            (
                bad_response("Code: 202. DB::Exception: Too many simultaneous queries"),
                ErrorKind::Transient,
            ),
            (
                bad_response("503 Service Unavailable"),
                ErrorKind::Transient,
            ),
            (Error::TimedOut, ErrorKind::Transient),
            (Error::Network("refused".into()), ErrorKind::Transient),
        ];
        for (error, kind) in cases {
            assert_eq!(error_kind(&error), kind, "{error}");
        }
    }

    #[tokio::test]
    async fn test_check_passes_for_the_events_table() {
        let db = ClickHouseTestDatabase::create().await;
        ClickHouseWriter::new(db.config.clone())
            .check()
            .await
            .unwrap();
        db.drop_db().await;
    }

    #[tokio::test]
    async fn test_check_fails_with_a_config_error_on_bad_config() {
        let db = ClickHouseTestDatabase::create().await;
        db.client
            .query("ALTER TABLE events RENAME COLUMN sdk_version TO version")
            .execute()
            .await
            .unwrap();
        let configs = [
            ClickHouseConfig {
                password: "wrong".to_string(),
                ..db.config.clone()
            },
            ClickHouseConfig {
                database: "missing".to_string(),
                ..db.config.clone()
            },
            ClickHouseConfig {
                table: "missing".to_string(),
                ..db.config.clone()
            },
            // The renamed column.
            db.config.clone(),
        ];

        for config in configs {
            let error = ClickHouseWriter::new(config.clone())
                .check()
                .await
                .unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Config, "{config:?}: {error}");
        }
        db.drop_db().await;
    }

    #[tokio::test]
    async fn test_check_fails_with_a_transient_error_when_unreachable() {
        let writer = ClickHouseWriter::new(ClickHouseConfig {
            url: "http://127.0.0.1:1".to_string(),
            user: "default".to_string(),
            password: String::new(),
            database: "default".to_string(),
            table: "events".to_string(),
        });
        assert_eq!(
            writer.check().await.unwrap_err().kind(),
            ErrorKind::Transient
        );
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
