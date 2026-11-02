use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::batching::BatchWriter;
use super::SinkError;
use crate::models::EventRecord;

/// Records every write attempt; each takes `delay`, and the first
/// `failures` fail with `error`.
#[derive(Clone, Default)]
pub(super) struct FakeWriter {
    pub failures: usize,
    pub error: Option<fn() -> clickhouse::error::Error>,
    pub delay: Duration,
    pub attempts: Arc<AtomicUsize>,
    pub written: Arc<Mutex<Vec<Vec<i64>>>>,
}

impl BatchWriter for FakeWriter {
    async fn write(&self, records: &[EventRecord]) -> Result<(), SinkError> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        if attempt < self.failures {
            let error = self.error.expect("an error to fail with");
            return Err(error().into());
        }
        let timestamps = records.iter().map(|record| record.timestamp).collect();
        self.written.lock().unwrap().push(timestamps);
        Ok(())
    }
}

pub(super) fn records(timestamps: &[i64]) -> Vec<EventRecord> {
    timestamps
        .iter()
        .map(|&timestamp| EventRecord {
            environment_key: "env".to_string(),
            collected_at: timestamp,
            sdk_language: "rust".to_string(),
            sdk_version: "1.0.0".to_string(),
            event: "purchase".to_string(),
            feature_name: None,
            identifier: None,
            value: serde_json::Value::Null,
            traits: None,
            metadata: None,
            timestamp,
        })
        .collect()
}
