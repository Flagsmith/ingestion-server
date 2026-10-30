use std::borrow::Cow;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use super::batching::BatchWriter;
use super::{ErrorKind, SinkError};
use crate::models::EventRecord;

/// SQLSTATE classes for rows the table rejects: data exceptions and integrity
/// constraint violations.
const BAD_DATA_SQLSTATE_CLASSES: [&str; 2] = ["22", "23"];

/// SQLSTATEs for bad credentials or grants, or a missing database, schema,
/// table or column.
const CONFIG_SQLSTATES: [&str; 7] = [
    "28000", // invalid_authorization_specification
    "28P01", // invalid_password
    "3D000", // invalid_catalog_name
    "3F000", // invalid_schema_name
    "42501", // insufficient_privilege
    "42P01", // undefined_table
    "42703", // undefined_column
];

/// `statement_timeout` is enforced by the server, so a stalled connection is
/// caught by this instead.
const INSERT_TIMEOUT: Duration = Duration::from_secs(30);
const SET_STATEMENT_TIMEOUT: &str = "SET LOCAL statement_timeout = 30000";

const INSERT_EVENTS: &str = "
    INSERT INTO flagsmith_exp.events (
        environment_key, event, feature_name, timestamp, collected_at, identifier,
        value, traits, metadata, sdk_language, sdk_version
    )
    SELECT * FROM UNNEST(
        $1::text[], $2::text[], $3::text[], $4::timestamptz[], $5::timestamptz[], $6::text[],
        $7::text[], $8::text[], $9::text[], $10::text[], $11::text[]
    )";

#[derive(Debug, PartialEq)]
struct PostgresRow<'a> {
    environment_key: Cow<'a, str>,
    event: Cow<'a, str>,
    feature_name: Option<Cow<'a, str>>,
    timestamp: DateTime<Utc>,
    collected_at: DateTime<Utc>,
    identifier: Cow<'a, str>,
    value: Option<Cow<'a, str>>,
    traits: Option<String>,
    metadata: Option<String>,
    sdk_language: Cow<'a, str>,
    sdk_version: Cow<'a, str>,
}

impl<'a> From<&'a EventRecord> for PostgresRow<'a> {
    fn from(record: &'a EventRecord) -> Self {
        PostgresRow {
            environment_key: storable_text(&record.environment_key),
            event: storable_text(&record.event),
            feature_name: record.feature_name.as_deref().map(storable_text),
            timestamp: epoch_ms_to_datetime(record.timestamp),
            collected_at: epoch_ms_to_datetime(record.collected_at),
            identifier: storable_text(record.identifier.as_deref().unwrap_or_default()),
            value: value_to_text(&record.value),
            traits: record.traits.as_ref().map(ToString::to_string),
            metadata: record.metadata.as_ref().map(ToString::to_string),
            sdk_language: storable_text(&record.sdk_language),
            sdk_version: storable_text(&record.sdk_version),
        }
    }
}

fn value_to_text(value: &serde_json::Value) -> Option<Cow<'_, str>> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(storable_text(s)),
        other => Some(Cow::Owned(other.to_string())),
    }
}

/// Postgres text cannot hold NUL, which JSON strings allow; one would fail the
/// whole batch.
fn storable_text(text: &str) -> Cow<'_, str> {
    if text.contains('\0') {
        Cow::Owned(text.replace('\0', ""))
    } else {
        Cow::Borrowed(text)
    }
}

/// Ingest validation keeps timestamps in range, so the epoch fallback is
/// never used; it avoids a panic, which aborts the process.
fn epoch_ms_to_datetime(epoch_ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(epoch_ms).unwrap_or_default()
}

/// Anything unrecognised, such as a network error, pool timeout or
/// cancelled statement, counts as transient.
pub(super) fn error_kind(error: &sqlx::Error) -> ErrorKind {
    let Some(sqlstate) = error.as_database_error().and_then(|e| e.code()) else {
        return ErrorKind::Transient;
    };
    if CONFIG_SQLSTATES.contains(&sqlstate.as_ref()) {
        ErrorKind::Config
    } else if sqlstate
        .get(..2)
        .is_some_and(|class| BAD_DATA_SQLSTATE_CLASSES.contains(&class))
    {
        ErrorKind::BadData
    } else {
        ErrorKind::Transient
    }
}

fn column_array<'r, 'a, T>(
    rows: &'r [PostgresRow<'a>],
    field: impl Fn(&'r PostgresRow<'a>) -> T,
) -> Vec<T> {
    rows.iter().map(field).collect()
}

pub struct PostgresWriter {
    pool: PgPool,
}

impl BatchWriter for PostgresWriter {
    async fn write(&self, records: &[EventRecord]) -> Result<(), SinkError> {
        tokio::time::timeout(INSERT_TIMEOUT, self.insert_in_transaction(records))
            .await
            .map_err(|_| SinkError::PostgresTimeout)?
    }
}

impl PostgresWriter {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn insert_in_transaction(&self, records: &[EventRecord]) -> Result<(), SinkError> {
        let rows: Vec<PostgresRow<'_>> = records.iter().map(PostgresRow::from).collect();
        let mut transaction = self.pool.begin().await?;
        sqlx::query(SET_STATEMENT_TIMEOUT)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(INSERT_EVENTS)
            .bind(column_array(&rows, |row| row.environment_key.as_ref()))
            .bind(column_array(&rows, |row| row.event.as_ref()))
            .bind(column_array(&rows, |row| row.feature_name.as_deref()))
            .bind(column_array(&rows, |row| row.timestamp))
            .bind(column_array(&rows, |row| row.collected_at))
            .bind(column_array(&rows, |row| row.identifier.as_ref()))
            .bind(column_array(&rows, |row| row.value.as_deref()))
            .bind(column_array(&rows, |row| row.traits.as_deref()))
            .bind(column_array(&rows, |row| row.metadata.as_deref()))
            .bind(column_array(&rows, |row| row.sdk_language.as_ref()))
            .bind(column_array(&rows, |row| row.sdk_version.as_ref()))
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::sink::{BatchConfig, BatchingSink};
    use crate::test_helpers::{now_ms, POSTGRES_EVENTS_TABLE};

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

    fn utc_datetime(epoch_ms: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(epoch_ms).unwrap()
    }

    #[derive(Debug, PartialEq, sqlx::FromRow)]
    struct StoredRow {
        environment_key: String,
        event: String,
        feature_name: Option<String>,
        timestamp: DateTime<Utc>,
        collected_at: Option<DateTime<Utc>>,
        identifier: String,
        value: Option<String>,
        traits: Option<String>,
        metadata: Option<String>,
        sdk_language: Option<String>,
        sdk_version: Option<String>,
    }

    #[sqlx::test]
    async fn test_shutdown_writes_buffered_rows_to_the_events_table(pool: PgPool) {
        sqlx::raw_sql(POSTGRES_EVENTS_TABLE)
            .execute(&pool)
            .await
            .unwrap();
        let sink = BatchingSink::new(
            PostgresWriter::new(pool.clone()),
            BatchConfig {
                flush_interval: Duration::from_secs(3600),
                flush_max_events: 1000,
            },
        );
        let timestamp = now_ms();

        sink.put(vec![
            EventRecord {
                feature_name: Some("dark_mode".to_string()),
                identifier: Some("user\0_1".to_string()),
                value: json!("control"),
                traits: Some(json!({"plan": "pro"})),
                metadata: Some(json!({"source": "csv"})),
                ..record("$flag_exposure", timestamp)
            },
            record("purchase", timestamp + 2),
            EventRecord {
                value: json!(42),
                ..record("purchase", timestamp + 4)
            },
        ]);
        sink.shutdown(Duration::from_secs(5)).await;

        let stored_rows: Vec<StoredRow> =
            sqlx::query_as("SELECT * FROM flagsmith_exp.events ORDER BY timestamp")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(
            stored_rows,
            vec![
                StoredRow {
                    environment_key: "env".to_string(),
                    event: "$flag_exposure".to_string(),
                    feature_name: Some("dark_mode".to_string()),
                    timestamp: utc_datetime(timestamp),
                    collected_at: Some(utc_datetime(timestamp + 1)),
                    identifier: "user_1".to_string(),
                    value: Some("control".to_string()),
                    traits: Some(r#"{"plan":"pro"}"#.to_string()),
                    metadata: Some(r#"{"source":"csv"}"#.to_string()),
                    sdk_language: Some("python".to_string()),
                    sdk_version: Some("1.0.0".to_string()),
                },
                StoredRow {
                    environment_key: "env".to_string(),
                    event: "purchase".to_string(),
                    feature_name: None,
                    timestamp: utc_datetime(timestamp + 2),
                    collected_at: Some(utc_datetime(timestamp + 3)),
                    identifier: String::new(),
                    value: None,
                    traits: None,
                    metadata: None,
                    sdk_language: Some("python".to_string()),
                    sdk_version: Some("1.0.0".to_string()),
                },
                StoredRow {
                    environment_key: "env".to_string(),
                    event: "purchase".to_string(),
                    feature_name: None,
                    timestamp: utc_datetime(timestamp + 4),
                    collected_at: Some(utc_datetime(timestamp + 5)),
                    identifier: String::new(),
                    value: Some("42".to_string()),
                    traits: None,
                    metadata: None,
                    sdk_language: Some("python".to_string()),
                    sdk_version: Some("1.0.0".to_string()),
                },
            ]
        );
    }
}
