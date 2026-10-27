use std::fmt;
use std::mem;
use std::time::Duration;

use clickhouse::{Client, Compression, Row};
use serde::Serialize;

use super::batching::BatchWriter;
use super::SinkError;
use crate::models::EventRecord;

/// Bounds sending and committing one batch of up to `SINK_BATCH_MAX_ROWS` rows.
const INSERT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct ClickHouseSinkConfig {
    pub url: String,
    pub user: String,
    pub password: String,
    pub database: String,
    pub table: String,
}

/// Keeps the password out of logs.
impl fmt::Debug for ClickHouseSinkConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickHouseSinkConfig")
            .field("url", &self.url)
            .field("user", &self.user)
            .field("database", &self.database)
            .field("table", &self.table)
            .finish_non_exhaustive()
    }
}

/// One row of the `events` table. Field names are the column names. Every
/// column is non-Nullable, so absent optionals are written as "", as
/// ClickPipes does.
#[derive(Debug, PartialEq, Row, Serialize)]
pub struct ClickHouseRow {
    environment_key: String,
    event: String,
    feature_name: String,
    /// DateTime64(3), sent as epoch millis.
    timestamp: i64,
    collected_at: i64,
    identifier: String,
    value: String,
    traits: String,
    metadata: String,
    sdk_language: String,
    sdk_version: String,
}

/// `value` is a String column: a JSON string goes in raw, null as "", and
/// anything else as its JSON text.
fn value_column(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

pub struct ClickHouseWriter {
    client: Client,
    table: String,
}

impl ClickHouseWriter {
    /// Connects lazily, on the first insert.
    pub fn new(config: ClickHouseSinkConfig) -> Self {
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
    type Row = ClickHouseRow;

    fn row(record: &EventRecord<'_>) -> ClickHouseRow {
        ClickHouseRow {
            environment_key: record.environment_key.to_string(),
            event: record.event.to_string(),
            feature_name: record.feature_name.unwrap_or_default().to_string(),
            timestamp: record.timestamp,
            collected_at: record.collected_at,
            identifier: record.identifier.unwrap_or_default().to_string(),
            value: value_column(record.value),
            traits: record.traits.map(ToString::to_string).unwrap_or_default(),
            metadata: record.metadata.map(ToString::to_string).unwrap_or_default(),
            sdk_language: record.sdk_language.to_string(),
            sdk_version: record.sdk_version.to_string(),
        }
    }

    fn row_size(row: &ClickHouseRow) -> usize {
        mem::size_of::<ClickHouseRow>()
            + row.environment_key.len()
            + row.event.len()
            + row.feature_name.len()
            + row.identifier.len()
            + row.value.len()
            + row.traits.len()
            + row.metadata.len()
            + row.sdk_language.len()
            + row.sdk_version.len()
    }

    /// One plain `INSERT` per batch; succeeds once ClickHouse has committed it.
    async fn write(&self, rows: &[ClickHouseRow]) -> Result<(), SinkError> {
        let mut insert = self
            .client
            .insert::<ClickHouseRow>(&self.table)
            .await?
            .with_timeouts(Some(INSERT_TIMEOUT), Some(INSERT_TIMEOUT));
        for row in rows {
            insert.write(row).await?;
        }
        insert.end().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn row_fills_every_column_and_blanks_absent_optionals() {
        let value = json!(42);
        let traits = json!({"plan": "pro"});
        let record = EventRecord {
            environment_key: "env",
            collected_at: 1_700_000_000_001,
            sdk_language: "python",
            sdk_version: "1.0.0",
            event: "purchase",
            feature_name: None,
            identifier: None,
            value: &value,
            traits: Some(&traits),
            metadata: None,
            timestamp: 1_700_000_000_000,
        };

        assert_eq!(
            ClickHouseWriter::row(&record),
            ClickHouseRow {
                environment_key: "env".to_string(),
                event: "purchase".to_string(),
                feature_name: String::new(),
                timestamp: 1_700_000_000_000,
                collected_at: 1_700_000_000_001,
                identifier: String::new(),
                value: "42".to_string(),
                traits: r#"{"plan":"pro"}"#.to_string(),
                metadata: String::new(),
                sdk_language: "python".to_string(),
                sdk_version: "1.0.0".to_string(),
            }
        );
    }

    #[test]
    fn value_column_writes_strings_raw_and_null_as_empty() {
        assert_eq!(value_column(&json!("control")), "control");
        assert_eq!(value_column(&json!(null)), "");
        assert_eq!(value_column(&json!(42)), "42");
        assert_eq!(value_column(&json!(true)), "true");
        assert_eq!(value_column(&json!({"a": [1, 2]})), r#"{"a":[1,2]}"#);
    }

    #[test]
    fn debug_output_omits_the_password() {
        let config = ClickHouseSinkConfig {
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
}
