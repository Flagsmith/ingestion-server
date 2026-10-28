use serde::{Deserialize, Serialize, Serializer};

/// Reserved system event for flag-exposure tracking. `feature_name` is required
/// for events of this type.
pub const FLAG_EXPOSURE_EVENT: &str = "$flag_exposure";

#[derive(Debug, Clone, Deserialize)]
pub struct Event {
    /// Event type/discriminator. Caller-supplied name for user-defined events
    /// (e.g. `"purchase"`), or a reserved `$`-prefixed literal for system events.
    /// Defaulted so a missing field is rejected by validation, not serde (422).
    #[serde(default)]
    pub event: String,
    /// Required for exposures; top-level so it can be indexed/joined.
    #[serde(default)]
    pub feature_name: Option<String>,
    #[serde(default)]
    pub identifier: Option<String>,
    #[serde(default)]
    pub value: serde_json::Value,
    #[serde(default)]
    pub traits: Option<serde_json::Value>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
    /// Epoch milliseconds. Defaulted so a missing field is rejected by
    /// validation, not serde (422); `validate_event` requires an epoch-millis
    /// value after 2020-01-01.
    #[serde(default)]
    pub timestamp: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EventBatch {
    pub events: Vec<Event>,
}

/// Per-event rejection in an ingest response; `index` points into the
/// submitted `events` array. Rejected events should be dropped, not
/// resubmitted — the event itself is invalid as sent.
#[derive(Debug, Clone, Serialize)]
pub struct RejectedEvent {
    pub index: usize,
    pub error: String,
}

/// Response body for POST /v1/events.
#[derive(Debug, Clone, Serialize)]
pub struct IngestResponse {
    pub accepted: usize,
    pub rejected: Vec<RejectedEvent>,
    /// Set only for request-level failures (e.g. empty `events` array).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// On-wire shape produced to Kafka: one record per event, flat JSON.
///
/// `traits` and `metadata` are JSON-encoded strings, not nested objects: the
/// destination `events` table stores them in String columns, and ClickPipes
/// only maps flat scalar fields — a nested object field is left out of the
/// pipe's field mapping and the column silently defaults to ''.
#[derive(Debug, Clone, Serialize)]
pub struct EventRecord {
    pub environment_key: String,
    pub collected_at: i64,
    pub sdk_language: String,
    pub sdk_version: String,

    pub event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feature_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<String>,
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub value: serde_json::Value,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "json_string"
    )]
    pub traits: Option<serde_json::Value>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "json_string"
    )]
    pub metadata: Option<serde_json::Value>,
    pub timestamp: i64,
}

/// Serializes a JSON value as its encoded text, in a string.
fn json_string<S: Serializer>(
    value: &Option<serde_json::Value>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match value {
        Some(value) => serializer.collect_str(value),
        None => serializer.serialize_none(),
    }
}
