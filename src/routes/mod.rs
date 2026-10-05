mod health;
mod ingest;

use axum::{middleware, routing::get, routing::post, Router};
use tower_http::cors::{Any, CorsLayer};
use tower_http::decompression::RequestDecompressionLayer;

use crate::auth::{extract_environment_context, ContextState};
use crate::sink::KafkaSink;

pub use health::health_check;
pub use ingest::ingest_batch;

pub fn app(sink: KafkaSink, context_state: ContextState) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route(
            "/v1/events",
            post(ingest_batch).layer(middleware::from_fn_with_state(
                context_state,
                extract_environment_context,
            )),
        )
        .layer(RequestDecompressionLayer::new())
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(sink)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use sqlx::PgPool;
    use tower::ServiceExt;

    use super::*;
    use crate::environment_keys::EnvironmentKeys;
    use crate::test_helpers::{
        assert_topic_empty, create_environment_keys, insert_environment_key, mock_kafka,
        read_records, FLAGSMITH_WAREHOUSE_TOPIC,
    };

    const EXTERNAL_WAREHOUSE_TOPIC: &str = "events-external";

    fn context_state(pool: PgPool) -> ContextState {
        ContextState {
            environment_keys: EnvironmentKeys::new(pool, Duration::from_secs(300)),
            external_topic: Arc::from(EXTERNAL_WAREHOUSE_TOPIC),
        }
    }

    fn post_event_with_key(key: &str) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/v1/events")
            .header("content-type", "application/json")
            .header("X-Environment-Key", key)
            .body(Body::from(
                r#"{"events":[{"event":"purchase","value":42,"timestamp":1700000000000}]}"#,
            ))
            .unwrap()
    }

    #[sqlx::test]
    async fn environment_using_an_external_warehouse_sends_events_to_the_external_topic(
        pool: PgPool,
    ) {
        // Given
        create_environment_keys(&pool).await;
        insert_environment_key(&pool, "ser.server-key", true, None).await;
        let kafka = mock_kafka(&[FLAGSMITH_WAREHOUSE_TOPIC, EXTERNAL_WAREHOUSE_TOPIC]);
        let app = app(kafka.sink.clone(), context_state(pool));

        // When
        let response = app
            .oneshot(post_event_with_key("ser.server-key"))
            .await
            .unwrap();

        // Then
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let records = read_records(&kafka.cluster, EXTERNAL_WAREHOUSE_TOPIC, 1).await;
        assert_eq!(records[0]["environment_key"], "client-api-key");
        assert_topic_empty(&kafka.cluster, FLAGSMITH_WAREHOUSE_TOPIC);
    }

    #[sqlx::test]
    async fn environment_using_the_flagsmith_warehouse_sends_events_to_the_flagsmith_topic(
        pool: PgPool,
    ) {
        // Given
        create_environment_keys(&pool).await;
        insert_environment_key(&pool, "ser.server-key", false, None).await;
        let kafka = mock_kafka(&[FLAGSMITH_WAREHOUSE_TOPIC, EXTERNAL_WAREHOUSE_TOPIC]);
        let app = app(kafka.sink.clone(), context_state(pool));

        // When
        let response = app
            .oneshot(post_event_with_key("ser.server-key"))
            .await
            .unwrap();

        // Then
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let records = read_records(&kafka.cluster, FLAGSMITH_WAREHOUSE_TOPIC, 1).await;
        assert_eq!(records[0]["environment_key"], "client-api-key");
        assert_topic_empty(&kafka.cluster, EXTERNAL_WAREHOUSE_TOPIC);
    }
}
