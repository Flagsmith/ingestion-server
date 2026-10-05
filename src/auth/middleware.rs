use std::sync::Arc;

use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};

use crate::environment_keys::{EnvironmentKeys, LookupError};

#[derive(Debug, Clone)]
pub struct EnvironmentContext {
    /// The environment's client API key, as resolved from the presented key
    /// (which may be a server-side key).
    pub client_api_key: Arc<str>,
    /// Destination for this environment, an opaque routing token the sink
    /// interprets; `None` routes to the default.
    pub destination: Option<Arc<str>>,
}

#[derive(Clone)]
pub struct ContextState {
    pub environment_keys: EnvironmentKeys,
    pub external_topic: Arc<str>,
}

pub async fn extract_environment_context(
    State(state): State<ContextState>,
    mut request: Request,
    next: Next,
) -> Response {
    let env_key = request
        .headers()
        .get("X-Environment-Key")
        .and_then(|v| v.to_str().ok());

    let key = match env_key {
        Some(k) if !k.is_empty() => k.to_string(),
        _ => {
            return (
                StatusCode::UNAUTHORIZED,
                "Missing or invalid X-Environment-Key header",
            )
                .into_response();
        }
    };

    let environment_key = match state.environment_keys.lookup(&key).await {
        Ok(Some(environment_key)) => environment_key,
        Ok(None) => return (StatusCode::FORBIDDEN, "Unknown environment key").into_response(),
        Err(LookupError::Unavailable) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Authentication backend unavailable",
            )
                .into_response();
        }
    };

    let destination = environment_key
        .uses_external_warehouse
        .then(|| state.external_topic.clone());

    request.extensions_mut().insert(EnvironmentContext {
        client_api_key: environment_key.client_api_key,
        destination,
    });
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::get;
    use axum::Router;
    use sqlx::PgPool;
    use tower::ServiceExt;

    use crate::test_helpers::create_environment_keys;

    fn test_state(pool: PgPool) -> ContextState {
        ContextState {
            environment_keys: EnvironmentKeys::new(pool, Duration::from_secs(300)),
            external_topic: Arc::from("external-topic"),
        }
    }

    fn unconnected_pool() -> PgPool {
        PgPool::connect_lazy("postgres://localhost/unused").unwrap()
    }

    fn app(state: ContextState) -> Router {
        Router::new()
            .route("/", get(|| async {}))
            .layer(axum::middleware::from_fn_with_state(
                state,
                extract_environment_context,
            ))
    }

    fn request_with_key(key: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder().uri("/");
        if let Some(k) = key {
            builder = builder.header("X-Environment-Key", k);
        }
        builder.body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn missing_header_is_unauthorized() {
        // Given
        let app = app(test_state(unconnected_pool()));

        // When
        let response = app.oneshot(request_with_key(None)).await.unwrap();

        // Then
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn empty_header_is_unauthorized() {
        // Given
        let app = app(test_state(unconnected_pool()));

        // When
        let response = app.oneshot(request_with_key(Some(""))).await.unwrap();

        // Then
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[sqlx::test]
    async fn unknown_key_is_forbidden(pool: PgPool) {
        // Given
        create_environment_keys(&pool).await;
        let app = app(test_state(pool));

        // When
        let response = app
            .oneshot(request_with_key(Some("unknown-key")))
            .await
            .unwrap();

        // Then
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[sqlx::test]
    async fn unreachable_postgres_is_service_unavailable(pool: PgPool) {
        // Given
        pool.close().await;
        let app = app(test_state(pool));

        // When
        let response = app
            .oneshot(request_with_key(Some("ser.server-key")))
            .await
            .unwrap();

        // Then
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
