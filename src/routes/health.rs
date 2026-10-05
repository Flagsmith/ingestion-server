use axum::{extract::State, http::StatusCode, Json};
use serde::Serialize;

use crate::sink::KafkaSink;

#[derive(Serialize)]
pub struct HealthResponse {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Liveness plus one local check: a fatal producer error is permanent, so
/// report it and let ECS replace the task. No downstream call, so this cannot hang.
pub async fn health_check(State(sink): State<KafkaSink>) -> (StatusCode, Json<HealthResponse>) {
    match sink.fatal_error() {
        None => (
            StatusCode::OK,
            Json(HealthResponse {
                status: "healthy",
                error: None,
            }),
        ),
        Some(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthResponse {
                status: "unhealthy",
                error: Some(error),
            }),
        ),
    }
}
