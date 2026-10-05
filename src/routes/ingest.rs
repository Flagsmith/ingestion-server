use axum::{
    extract::{Extension, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use chrono::Utc;
use tracing::{debug, warn};

use crate::auth::EnvironmentContext;
use crate::lookup::mask_key;
use crate::models::{
    Event, EventBatch, EventRecord, IngestResponse, RejectedEvent, FLAG_EXPOSURE_EVENT,
};
use crate::sink::KafkaSink;

/// Header-derived fields are copied into every record of the batch, so an
/// unbounded header would inflate every record.
const MAX_SDK_FIELD_CHARS: usize = 100;

fn truncate_chars(s: &str, max: usize) -> &str {
    match s.char_indices().nth(max) {
        Some((i, _)) => &s[..i],
        None => s,
    }
}

fn parse_sdk_user_agent(header: &str) -> (String, String) {
    let (sdk_part, version) = header.split_once('/').unwrap_or((header, "unknown"));
    let language = sdk_part
        .strip_prefix("flagsmith-")
        .and_then(|s| s.strip_suffix("-sdk"))
        .unwrap_or("unknown");
    (
        truncate_chars(language, MAX_SDK_FIELD_CHARS).to_string(),
        truncate_chars(version, MAX_SDK_FIELD_CHARS).to_string(),
    )
}

/// 2020-01-01T00:00:00Z in epoch-millis; mirrors ClickHouse's `timestamp_sane` constraint.
const MIN_TIMESTAMP_MS: i64 = 1_577_836_800_000;
/// Future clock-skew allowance.
const MAX_FUTURE_SKEW_MS: i64 = 24 * 60 * 60 * 1000;

/// Event names live in our own namespace and a LowCardinality ClickHouse column.
const MAX_EVENT_NAME_CHARS: usize = 256;
/// Mirrors Flagsmith's Feature.name CharField(max_length=2000).
const MAX_FEATURE_NAME_CHARS: usize = 2000;
/// Mirrors Flagsmith's Identity.identifier CharField(max_length=2000).
const MAX_IDENTIFIER_CHARS: usize = 2000;
/// Mirrors Flagsmith's Trait.trait_key CharField(max_length=200).
const MAX_TRAIT_KEY_CHARS: usize = 200;
/// Mirrors Flagsmith's TRAIT_STRING_VALUE_MAX_LENGTH.
const MAX_TRAIT_STRING_VALUE_CHARS: usize = 2000;

/// Must fit a Flagsmith flag value (FEATURE_VALUE_LIMIT, default 20,000
/// chars) at serde_json's worst-case 6-bytes-per-char escaping, since the
/// SDKs send served flag values verbatim on $flag_exposure events.
const MAX_VALUE_BYTES: usize = 128 * 1024;
/// Headroom for ~16 max-length traits per event.
const MAX_TRAITS_BYTES: usize = 32 * 1024;
/// SDK envelope data; small by design.
const MAX_METADATA_BYTES: usize = 8 * 1024;
// Worst-case serialized record under these caps is ~185 KiB — comfortably
// inside Kafka's default 1 MB message.max.bytes.

/// Serialized JSON size in bytes. Value serialization cannot fail; if it
/// somehow does, treat the field as oversized rather than propagate.
fn json_size(value: &serde_json::Value) -> usize {
    serde_json::to_vec(value)
        .map(|v| v.len())
        .unwrap_or(usize::MAX)
}

/// Validates one event against the ingest schema rules. Error messages name
/// the offending field; the caller prefixes the batch index.
fn validate_event(event: &Event, max_timestamp: i64) -> Result<(), String> {
    if event.event.trim().is_empty() {
        return Err("event must be non-empty".to_string());
    }
    if event.event.chars().count() > MAX_EVENT_NAME_CHARS {
        return Err(format!(
            "event must be at most {MAX_EVENT_NAME_CHARS} characters"
        ));
    }
    // The `$` prefix is reserved for system events; reject anything that
    // looks like one but isn't recognised, so callers can't squat the namespace.
    if event.event.starts_with('$') && event.event != FLAG_EXPOSURE_EVENT {
        return Err(format!(
            "event \"{}\" uses the reserved `$` prefix but is not a known system event",
            event.event
        ));
    }
    if event.event == FLAG_EXPOSURE_EVENT && event.feature_name.is_none() {
        return Err(format!(
            "feature_name is required for {FLAG_EXPOSURE_EVENT} events"
        ));
    }
    if let Some(name) = &event.feature_name {
        if name.chars().count() > MAX_FEATURE_NAME_CHARS {
            return Err(format!(
                "feature_name must be at most {MAX_FEATURE_NAME_CHARS} characters"
            ));
        }
    }
    if let Some(identifier) = &event.identifier {
        if identifier.chars().count() > MAX_IDENTIFIER_CHARS {
            return Err(format!(
                "identifier must be at most {MAX_IDENTIFIER_CHARS} characters"
            ));
        }
    }
    if event.timestamp <= MIN_TIMESTAMP_MS {
        return Err("timestamp must be an epoch-millis value after 2020-01-01".to_string());
    }
    if event.timestamp > max_timestamp {
        return Err("timestamp must not be more than 24h in the future".to_string());
    }
    if json_size(&event.value) > MAX_VALUE_BYTES {
        return Err(format!(
            "value must serialize to at most {MAX_VALUE_BYTES} bytes"
        ));
    }
    if let Some(traits) = &event.traits {
        if json_size(traits) > MAX_TRAITS_BYTES {
            return Err(format!(
                "traits must serialize to at most {MAX_TRAITS_BYTES} bytes"
            ));
        }
        // Upstream traits are strictly a key -> value map; other shapes have
        // no product meaning and would dodge the per-trait checks below.
        let Some(map) = traits.as_object() else {
            return Err("traits must be a JSON object".to_string());
        };
        for (key, value) in map {
            if key.chars().count() > MAX_TRAIT_KEY_CHARS {
                return Err(format!(
                    "trait keys must be at most {MAX_TRAIT_KEY_CHARS} characters"
                ));
            }
            if let Some(s) = value.as_str() {
                if s.chars().count() > MAX_TRAIT_STRING_VALUE_CHARS {
                    return Err(format!(
                        "traits[\"{key}\"] string values must be at most {MAX_TRAIT_STRING_VALUE_CHARS} characters"
                    ));
                }
            }
        }
    }
    if let Some(metadata) = &event.metadata {
        if json_size(metadata) > MAX_METADATA_BYTES {
            return Err(format!(
                "metadata must serialize to at most {MAX_METADATA_BYTES} bytes"
            ));
        }
    }
    Ok(())
}

/// Splits a batch into forwardable events and per-event rejections. Each
/// event is validated independently: one invalid event never blocks its
/// neighbours.
fn partition_events(events: &[Event]) -> (Vec<&Event>, Vec<RejectedEvent>) {
    let max_timestamp = Utc::now().timestamp_millis() + MAX_FUTURE_SKEW_MS;
    let mut valid = Vec::with_capacity(events.len());
    let mut rejected = Vec::new();
    for (i, event) in events.iter().enumerate() {
        match validate_event(event, max_timestamp) {
            Ok(()) => valid.push(event),
            Err(error) => rejected.push(RejectedEvent {
                index: i,
                error: format!("events[{i}].{error}"),
            }),
        }
    }
    (valid, rejected)
}

/// POST /v1/events
/// Validates each event independently: valid events are serialized and
/// forwarded to the sink, invalid ones are reported back as `rejected`
/// entries without blocking the rest of the batch.
/// Returns 202 when at least one event was accepted and forwarded, 400 when
/// none were (all invalid, or empty batch), 503 on sink failure
/// (delivery not confirmed; retrying is safe but may duplicate events from
/// a partially delivered batch).
pub async fn ingest_batch(
    State(sink): State<KafkaSink>,
    Extension(ctx): Extension<EnvironmentContext>,
    headers: HeaderMap,
    Json(batch): Json<EventBatch>,
) -> impl IntoResponse {
    if batch.events.is_empty() {
        let body = IngestResponse {
            accepted: 0,
            rejected: Vec::new(),
            error: Some("events must be non-empty".to_string()),
        };
        return (StatusCode::BAD_REQUEST, Json(body)).into_response();
    }

    let (valid, rejected) = partition_events(&batch.events);
    if !rejected.is_empty() {
        warn!(
            client_api_key = %mask_key(&ctx.client_api_key),
            rejected_count = rejected.len(),
            batch_size = batch.events.len(),
            first_error = %rejected[0].error,
            "Rejected invalid events from batch"
        );
    }
    if valid.is_empty() {
        let body = IngestResponse {
            accepted: 0,
            rejected,
            error: None,
        };
        return (StatusCode::BAD_REQUEST, Json(body)).into_response();
    }

    let collected_at = Utc::now().timestamp_millis();
    let (sdk_language, sdk_version) = headers
        .get("Flagsmith-SDK-User-Agent")
        .and_then(|v| v.to_str().ok())
        .map(parse_sdk_user_agent)
        .unwrap_or_else(|| ("unknown".to_string(), "unknown".to_string()));

    let records: Vec<Vec<u8>> = valid
        .iter()
        .map(|event| {
            let record = EventRecord {
                environment_key: &ctx.client_api_key,
                collected_at,
                sdk_language: &sdk_language,
                sdk_version: &sdk_version,
                event: &event.event,
                feature_name: event.feature_name.as_deref(),
                identifier: event.identifier.as_deref(),
                value: &event.value,
                traits: event.traits.as_ref().map(ToString::to_string),
                metadata: event.metadata.as_ref().map(ToString::to_string),
                timestamp: event.timestamp,
            };
            serde_json::to_vec(&record).expect("EventRecord serialization cannot fail")
        })
        .collect();

    let record_count = records.len();
    match sink.put(ctx.destination.as_deref(), records).await {
        Ok(()) => {
            debug!(
                client_api_key = %mask_key(&ctx.client_api_key),
                destination = %ctx.destination.as_deref().unwrap_or("default"),
                event_count = record_count,
                "Forwarded events to sink"
            );
            let body = IngestResponse {
                accepted: record_count,
                rejected,
                error: None,
            };
            (StatusCode::ACCEPTED, Json(body)).into_response()
        }
        Err(e) => {
            warn!(
                client_api_key = %mask_key(&ctx.client_api_key),
                destination = %ctx.destination.as_deref().unwrap_or("default"),
                event_count = record_count,
                error = %e,
                "Sink delivery failed"
            );
            (StatusCode::SERVICE_UNAVAILABLE, "Downstream unavailable").into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use axum::routing::post;
    use axum::Router;
    use rdkafka::types::RDKafkaRespErr;
    use tower::ServiceExt;

    use crate::sink::{KafkaAuth, KafkaSinkConfig};
    use crate::test_helpers::{
        assert_topic_empty, mock_kafka, read_records, FLAGSMITH_WAREHOUSE_TOPIC,
    };

    fn parse(json: &str) -> EventBatch {
        serde_json::from_str(json).expect("valid EventBatch JSON")
    }

    /// Partition the batch and return (accepted count, rejections).
    fn partition(json: &str) -> (usize, Vec<RejectedEvent>) {
        let batch = parse(json);
        let (valid, rejected) = partition_events(&batch.events);
        (valid.len(), rejected)
    }

    fn rejects(json: &str) -> bool {
        !partition(json).1.is_empty()
    }

    fn accepts(json: &str) -> bool {
        let (accepted, rejected) = partition(json);
        rejected.is_empty() && accepted > 0
    }

    #[test]
    fn test_parse_sdk_user_agent() {
        let (lang, ver) = parse_sdk_user_agent("flagsmith-js-sdk/11.0.0");
        assert_eq!(lang, "js");
        assert_eq!(ver, "11.0.0");
    }

    #[test]
    fn test_parse_sdk_user_agent_fallback() {
        let (lang, ver) = parse_sdk_user_agent("something-random");
        assert_eq!(lang, "unknown");
        assert_eq!(ver, "unknown");
    }

    #[test]
    fn test_partition_empty_batch_yields_nothing() {
        // The handler turns an empty batch into a request-level 400.
        let (accepted, rejected) = partition(r#"{"events":[]}"#);
        assert_eq!(accepted, 0);
        assert!(rejected.is_empty());
    }

    #[test]
    fn test_validate_rejects_missing_event_name() {
        // `event` omitted -> defaults to "" -> rejected by validation, not serde (422).
        assert!(rejects(r#"{"events":[{"timestamp":1700000000000}]}"#));
    }

    #[test]
    fn test_validate_rejects_exposure_without_feature_name() {
        assert!(rejects(
            r#"{"events":[{"event":"$flag_exposure","timestamp":1700000000000}]}"#
        ));
    }

    #[test]
    fn test_validate_accepts_exposure_with_feature_name() {
        assert!(accepts(
            r#"{"events":[{"event":"$flag_exposure","feature_name":"dark_mode","value":"control","timestamp":1700000000000}]}"#,
        ));
    }

    #[test]
    fn test_validate_rejects_whitespace_only_event() {
        assert!(rejects(
            r#"{"events":[{"event":"   ","timestamp":1700000000000}]}"#
        ));
    }

    #[test]
    fn test_validate_rejects_unknown_system_event() {
        assert!(rejects(
            r#"{"events":[{"event":"$made_up","timestamp":1700000000000}]}"#
        ));
    }

    #[test]
    fn test_validate_rejects_missing_timestamp() {
        // `timestamp` omitted -> defaults to 0 -> rejected by validation, not serde (422).
        assert!(rejects(r#"{"events":[{"event":"purchase","value":42}]}"#));
    }

    #[test]
    fn test_validate_rejects_nonpositive_timestamp() {
        assert!(rejects(
            r#"{"events":[{"event":"purchase","timestamp":0}]}"#
        ));
    }

    #[test]
    fn test_validate_rejects_pre_2020_timestamp() {
        // A placeholder like `timestamp: 1` reads as 1970-01-01 in ClickHouse and
        // violates the table's `timestamp_sane` constraint; reject it at the API.
        assert!(rejects(
            r#"{"events":[{"event":"purchase","timestamp":1}]}"#
        ));
    }

    #[test]
    fn test_validate_rejects_2020_boundary_timestamp() {
        // ClickHouse constraint is strictly `> 2020-01-01`, so the boundary itself fails.
        assert!(rejects(
            r#"{"events":[{"event":"purchase","timestamp":1577836800000}]}"#
        ));
    }

    #[test]
    fn test_validate_rejects_epoch_seconds_timestamp() {
        // Epoch-seconds (a common SDK mixup) reads as Jan 1970 when treated as millis.
        assert!(rejects(
            r#"{"events":[{"event":"purchase","timestamp":1751900000}]}"#
        ));
    }

    #[test]
    fn test_validate_rejects_far_future_timestamp() {
        let ts = Utc::now().timestamp_millis() + 2 * 24 * 60 * 60 * 1000;
        assert!(rejects(&format!(
            r#"{{"events":[{{"event":"purchase","timestamp":{ts}}}]}}"#
        )));
    }

    #[test]
    fn test_validate_accepts_current_timestamp() {
        let ts = Utc::now().timestamp_millis();
        assert!(accepts(&format!(
            r#"{{"events":[{{"event":"purchase","timestamp":{ts}}}]}}"#
        )));
    }

    #[test]
    fn test_validate_accepts_custom_event() {
        assert!(accepts(
            r#"{"events":[{"event":"purchase","value":42,"timestamp":1700000000000}]}"#
        ));
    }

    #[test]
    fn test_partition_mixed_batch_keeps_valid_events() {
        // One invalid event never blocks its neighbours; the rejection names
        // the offending index and carries the same message the old whole-batch
        // 400 used.
        let (accepted, rejected) = partition(
            r#"{"events":[
                {"event":"purchase","value":42,"timestamp":1700000000000},
                {"event":"purchase","timestamp":1},
                {"event":"$flag_exposure","feature_name":"dark_mode","timestamp":1700000000000}
            ]}"#,
        );
        assert_eq!(accepted, 2);
        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0].index, 1);
        assert_eq!(
            rejected[0].error,
            "events[1].timestamp must be an epoch-millis value after 2020-01-01"
        );
    }

    #[test]
    fn test_partition_all_invalid_batch_accepts_nothing() {
        // The handler turns this into a 400 with the full rejection list.
        let (accepted, rejected) = partition(
            r#"{"events":[
                {"event":"$made_up","timestamp":1700000000000},
                {"event":"purchase","timestamp":0}
            ]}"#,
        );
        assert_eq!(accepted, 0);
        assert_eq!(rejected.len(), 2);
        assert_eq!(rejected[0].index, 0);
        assert_eq!(rejected[1].index, 1);
    }

    #[test]
    fn test_ingest_response_omits_error_when_none() {
        let body = IngestResponse {
            accepted: 2,
            rejected: vec![RejectedEvent {
                index: 1,
                error: "events[1].event must be non-empty".to_string(),
            }],
            error: None,
        };
        let json = serde_json::to_value(&body).unwrap();
        assert_eq!(json["accepted"], 2);
        assert_eq!(json["rejected"][0]["index"], 1);
        assert_eq!(
            json["rejected"][0]["error"],
            "events[1].event must be non-empty"
        );
        assert!(json.get("error").is_none());
    }

    #[test]
    fn test_ingest_response_includes_request_level_error() {
        let body = IngestResponse {
            accepted: 0,
            rejected: Vec::new(),
            error: Some("events must be non-empty".to_string()),
        };
        let json = serde_json::to_value(&body).unwrap();
        assert_eq!(json["accepted"], 0);
        assert_eq!(json["rejected"].as_array().unwrap().len(), 0);
        assert_eq!(json["error"], "events must be non-empty");
    }

    #[test]
    fn test_event_record_omits_nulls_and_renames_fields() {
        let batch = parse(
            r#"{"events":[{"event":"$flag_exposure","feature_name":"dark_mode","identifier":"user_1","value":"control","timestamp":1700000000000}]}"#,
        );
        let event = &batch.events[0];
        let record = EventRecord {
            environment_key: "env_key",
            collected_at: 1700000000001,
            sdk_language: "python",
            sdk_version: "1.0.0",
            event: &event.event,
            feature_name: event.feature_name.as_deref(),
            identifier: event.identifier.as_deref(),
            value: &event.value,
            traits: event.traits.as_ref().map(ToString::to_string),
            metadata: event.metadata.as_ref().map(ToString::to_string),
            timestamp: event.timestamp,
        };

        let json = serde_json::to_value(&record).unwrap();
        assert_eq!(json["event"], "$flag_exposure");
        assert_eq!(json["feature_name"], "dark_mode");
        assert_eq!(json["identifier"], "user_1");
        assert_eq!(json["timestamp"], 1700000000000i64);
        // Absent optionals are omitted entirely.
        assert!(json.get("traits").is_none());
        assert!(json.get("metadata").is_none());
        // Dropped legacy fields are gone.
        assert!(json.get("event_id").is_none());
        assert!(json.get("event_type").is_none());
        assert!(json.get("enabled").is_none());
    }

    /// A sink whose bootstrap server never answers, so every put times out.
    fn unreachable_sink() -> KafkaSink {
        let mut config = KafkaSinkConfig::new(
            "127.0.0.1:1".to_string(),
            FLAGSMITH_WAREHOUSE_TOPIC.to_string(),
            KafkaAuth::None,
        );
        config.message_timeout = Duration::from_secs(2);
        KafkaSink::new(config).expect("sink")
    }

    fn app_with_destination(sink: &KafkaSink, destination: Option<&str>) -> Router {
        let ctx = EnvironmentContext {
            client_api_key: Arc::from("test-client-key"),
            destination: destination.map(Arc::from),
        };
        Router::new()
            .route("/v1/events", post(ingest_batch))
            .layer(Extension(ctx))
            .with_state(sink.clone())
    }

    fn app(sink: &KafkaSink) -> Router {
        app_with_destination(sink, None)
    }

    fn post_events(json: &str) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .body(Body::from(json.to_string()))
            .unwrap()
    }

    async fn body_json(response: axum::response::Response) -> serde_json::Value {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn test_handler_mixed_batch_returns_202_and_forwards_valid_only() {
        let kafka = mock_kafka(&[FLAGSMITH_WAREHOUSE_TOPIC]);
        let response = app(&kafka.sink)
            .oneshot(post_events(
                r#"{"events":[
                    {"event":"purchase","value":42,"timestamp":1700000000000},
                    {"event":"purchase","timestamp":1},
                    {"event":"$flag_exposure","feature_name":"dark_mode","timestamp":1700000000000}
                ]}"#,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let json = body_json(response).await;
        assert_eq!(json["accepted"], 2);
        assert_eq!(json["rejected"].as_array().unwrap().len(), 1);
        assert_eq!(json["rejected"][0]["index"], 1);

        let events = read_records(&kafka.cluster, FLAGSMITH_WAREHOUSE_TOPIC, 2).await;
        assert_eq!(events[0]["event"], "purchase");
        assert_eq!(events[1]["event"], "$flag_exposure");
    }

    #[tokio::test]
    async fn test_handler_forwards_traits_and_metadata_as_json_strings() {
        // The destination String columns (and the ClickPipes field mapping)
        // take flat scalar fields, so dict fields go on the wire JSON-encoded.
        let kafka = mock_kafka(&[FLAGSMITH_WAREHOUSE_TOPIC]);
        let response = app(&kafka.sink)
            .oneshot(post_events(
                r#"{"events":[{"event":"purchase","traits":{"logins":42,"plan":"pro"},"metadata":{"source":"csv"},"timestamp":1700000000000}]}"#,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let events = read_records(&kafka.cluster, FLAGSMITH_WAREHOUSE_TOPIC, 1).await;
        let traits_str = events[0]["traits"].as_str().expect("traits is a string");
        let traits: serde_json::Value = serde_json::from_str(traits_str).unwrap();
        assert_eq!(traits["logins"], 42);
        assert_eq!(traits["plan"], "pro");
        let metadata_str = events[0]["metadata"]
            .as_str()
            .expect("metadata is a string");
        let metadata: serde_json::Value = serde_json::from_str(metadata_str).unwrap();
        assert_eq!(metadata["source"], "csv");
    }

    #[tokio::test]
    async fn test_handler_all_invalid_returns_400_with_rejections() {
        let kafka = mock_kafka(&[FLAGSMITH_WAREHOUSE_TOPIC]);
        let response = app(&kafka.sink)
            .oneshot(post_events(
                r#"{"events":[{"event":"$made_up","timestamp":1700000000000},{"event":"purchase","timestamp":0}]}"#,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let json = body_json(response).await;
        assert_eq!(json["accepted"], 0);
        assert_eq!(json["rejected"].as_array().unwrap().len(), 2);
        assert!(json.get("error").is_none());
        assert_topic_empty(&kafka.cluster, FLAGSMITH_WAREHOUSE_TOPIC);
    }

    #[tokio::test]
    async fn test_handler_empty_batch_returns_400_with_error() {
        let kafka = mock_kafka(&[FLAGSMITH_WAREHOUSE_TOPIC]);
        let response = app(&kafka.sink)
            .oneshot(post_events(r#"{"events":[]}"#))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let json = body_json(response).await;
        assert_eq!(json["accepted"], 0);
        assert_eq!(json["error"], "events must be non-empty");
        assert_topic_empty(&kafka.cluster, FLAGSMITH_WAREHOUSE_TOPIC);
    }

    #[tokio::test]
    async fn test_handler_sink_failure_returns_503_without_accepted_claim() {
        let sink = unreachable_sink();
        let started = std::time::Instant::now();
        let response = app(&sink)
            .oneshot(post_events(
                r#"{"events":[{"event":"purchase","value":42,"timestamp":1700000000000}]}"#,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&bytes[..], b"Downstream unavailable");
        // Bounded by message_timeout (2s) plus the enqueue wait, never an open-ended hang.
        assert!(
            started.elapsed() < Duration::from_secs(8),
            "took {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn test_handler_broker_rejection_returns_503_quickly() {
        let kafka = mock_kafka(&[FLAGSMITH_WAREHOUSE_TOPIC]);
        kafka
            .cluster
            .topic_error(
                FLAGSMITH_WAREHOUSE_TOPIC,
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED,
            )
            .expect("topic error");
        let started = std::time::Instant::now();
        let response = app(&kafka.sink)
            .oneshot(post_events(
                r#"{"events":[{"event":"purchase","value":42,"timestamp":1700000000000}]}"#,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        // A permanent broker error fails the records at once, inside the 5 s message timeout.
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "took {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn test_handler_forwards_to_configured_destination() {
        let kafka = mock_kafka(&[FLAGSMITH_WAREHOUSE_TOPIC, "events-dest-x"]);
        let response = app_with_destination(&kafka.sink, Some("events-dest-x"))
            .oneshot(post_events(
                r#"{"events":[{"event":"purchase","value":42,"timestamp":1700000000000}]}"#,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let events = read_records(&kafka.cluster, "events-dest-x", 1).await;
        assert_eq!(events[0]["event"], "purchase");
        assert_eq!(events[0]["value"], 42);
    }

    #[tokio::test]
    async fn test_handler_delivers_large_batches() {
        let kafka = mock_kafka(&[FLAGSMITH_WAREHOUSE_TOPIC]);
        let events: Vec<String> = (0..501)
            .map(|i| format!(r#"{{"event":"purchase","value":{i},"timestamp":1700000000000}}"#))
            .collect();
        let body = format!(r#"{{"events":[{}]}}"#, events.join(","));
        let response = app(&kafka.sink).oneshot(post_events(&body)).await.unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let json = body_json(response).await;
        assert_eq!(json["accepted"], 501);
        let events = read_records(&kafka.cluster, FLAGSMITH_WAREHOUSE_TOPIC, 501).await;
        assert_eq!(events.len(), 501);
        assert_eq!(events[500]["value"], 500);
    }

    fn event_json(fields: &str) -> String {
        format!(r#"{{"events":[{{{fields},"timestamp":1700000000000}}]}}"#)
    }

    #[test]
    fn test_validate_rejects_long_event_name() {
        let name = "e".repeat(257);
        assert!(rejects(&event_json(&format!(r#""event":"{name}""#))));
    }

    #[test]
    fn test_validate_accepts_event_name_at_limit() {
        let name = "e".repeat(256);
        assert!(accepts(&event_json(&format!(r#""event":"{name}""#))));
    }

    #[test]
    fn test_validate_rejects_long_feature_name() {
        let name = "f".repeat(2001);
        assert!(rejects(&event_json(&format!(
            r#""event":"purchase","feature_name":"{name}""#
        ))));
    }

    #[test]
    fn test_validate_accepts_feature_name_at_limit() {
        let name = "f".repeat(2000);
        assert!(accepts(&event_json(&format!(
            r#""event":"purchase","feature_name":"{name}""#
        ))));
    }

    #[test]
    fn test_validate_rejects_long_identifier() {
        let id = "u".repeat(2001);
        assert!(rejects(&event_json(&format!(
            r#""event":"purchase","identifier":"{id}""#
        ))));
    }

    #[test]
    fn test_validate_accepts_identifier_at_limit() {
        let id = "u".repeat(2000);
        assert!(accepts(&event_json(&format!(
            r#""event":"purchase","identifier":"{id}""#
        ))));
    }

    #[test]
    fn test_validate_rejects_long_trait_key() {
        let key = "k".repeat(201);
        assert!(rejects(&event_json(&format!(
            r#""event":"purchase","traits":{{"{key}":1}}"#
        ))));
    }

    #[test]
    fn test_validate_rejects_long_trait_string_value() {
        let value = "v".repeat(2001);
        let (_, rejected) = partition(&event_json(&format!(
            r#""event":"purchase","traits":{{"plan":"{value}"}}"#
        )));
        assert_eq!(rejected.len(), 1);
        assert_eq!(
            rejected[0].error,
            "events[0].traits[\"plan\"] string values must be at most 2000 characters"
        );
    }

    #[test]
    fn test_validate_accepts_trait_string_value_at_limit() {
        let value = "v".repeat(2000);
        assert!(accepts(&event_json(&format!(
            r#""event":"purchase","traits":{{"plan":"{value}"}}"#
        ))));
    }

    #[test]
    fn test_validate_accepts_non_string_trait_values() {
        assert!(accepts(&event_json(
            r#""event":"purchase","traits":{"logins":42,"beta":true,"score":1.5}"#
        )));
    }

    #[test]
    fn test_validate_rejects_oversized_value() {
        let blob = "x".repeat(140_000);
        assert!(rejects(&event_json(&format!(
            r#""event":"purchase","value":"{blob}""#
        ))));
    }

    #[test]
    fn test_validate_accepts_value_at_byte_limit() {
        // String of MAX_VALUE_BYTES - 2 chars serializes to exactly the cap
        // (two quote bytes); the check is strictly greater-than.
        let blob = "x".repeat(MAX_VALUE_BYTES - 2);
        let v: serde_json::Value = serde_json::from_str(&format!(r#""{blob}""#)).unwrap();
        assert_eq!(json_size(&v), MAX_VALUE_BYTES);
        assert!(accepts(&event_json(&format!(
            r#""event":"purchase","value":"{blob}""#
        ))));
    }

    #[test]
    fn test_validate_rejects_oversized_traits_blob() {
        // 20 entries x ~1.9KB string values: each entry is fine, total is not.
        let value = "v".repeat(1900);
        let entries: Vec<String> = (0..20).map(|i| format!(r#""t{i}":"{value}""#)).collect();
        assert!(rejects(&event_json(&format!(
            r#""event":"purchase","traits":{{{}}}"#,
            entries.join(",")
        ))));
    }

    #[test]
    fn test_validate_rejects_oversized_metadata() {
        let blob = "m".repeat(9000);
        assert!(rejects(&event_json(&format!(
            r#""event":"purchase","metadata":{{"note":"{blob}"}}"#
        ))));
    }

    #[test]
    fn test_validate_accepts_metadata_at_byte_limit() {
        // {"note":"<N chars>"} serializes to N + 11 bytes.
        let blob = "m".repeat(MAX_METADATA_BYTES - 11);
        let json = format!(r#"{{"note":"{blob}"}}"#);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(json_size(&v), MAX_METADATA_BYTES);
        assert!(accepts(&event_json(&format!(
            r#""event":"purchase","metadata":{json}"#
        ))));
    }

    #[test]
    fn test_validate_rejects_non_object_traits() {
        // Upstream traits are strictly a key -> value map; a bare string or
        // array would dodge the per-trait checks.
        assert!(rejects(&event_json(
            r#""event":"purchase","traits":"not-a-map""#
        )));
        assert!(rejects(&event_json(
            r#""event":"purchase","traits":["a","b"]"#
        )));
        let (_, rejected) = partition(&event_json(r#""event":"purchase","traits":[]"#));
        assert_eq!(rejected[0].error, "events[0].traits must be a JSON object");
    }

    #[test]
    fn test_validate_accepts_trait_key_at_limit() {
        let key = "k".repeat(200);
        assert!(accepts(&event_json(&format!(
            r#""event":"purchase","traits":{{"{key}":1}}"#
        ))));
    }

    #[test]
    fn test_validate_char_limits_count_chars_not_bytes() {
        // 2000 two-byte chars (4000 UTF-8 bytes) must pass the char limits,
        // matching Django max_length semantics; a byte-count regression fails here.
        let id = "é".repeat(2000);
        assert!(accepts(&event_json(&format!(
            r#""event":"purchase","identifier":"{id}""#
        ))));
        let value = "é".repeat(2000);
        assert!(accepts(&event_json(&format!(
            r#""event":"purchase","traits":{{"plan":"{value}"}}"#
        ))));
        let name = "é".repeat(256);
        assert!(accepts(&event_json(&format!(r#""event":"{name}""#))));
    }

    #[test]
    fn test_parse_sdk_user_agent_truncates_oversized_fields() {
        let header = format!("flagsmith-{}-sdk/{}", "l".repeat(400), "9".repeat(400));
        let (lang, ver) = parse_sdk_user_agent(&header);
        assert_eq!(lang.chars().count(), 100);
        assert_eq!(ver.chars().count(), 100);
    }
}
