mod auth;
mod config;
mod environment_keys;
mod lookup;
mod models;
mod routes;
mod sink;
#[cfg(test)]
mod test_helpers;

use std::sync::Arc;
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use tokio::signal;
use tracing::info;

use auth::ContextState;
use config::Config;
use environment_keys::EnvironmentKeys;
use sink::{KafkaSink, KafkaSinkConfig};

const ENVIRONMENT_KEY_REFRESH_AFTER: Duration = Duration::from_secs(300);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("flagsmith_analytics=info".parse().unwrap()),
        )
        .json()
        .init();

    let config = Config::from_env()?;
    info!(
        listen_addr = %config.listen_addr,
        kafka_bootstrap_servers = %config.kafka_bootstrap_servers,
        kafka_topic = %config.kafka_topic,
        kafka_auth = ?config.kafka_auth,
        "Starting flagsmith-analytics server"
    );

    let sink = KafkaSink::new(KafkaSinkConfig::new(
        config.kafka_bootstrap_servers.clone(),
        config.kafka_topic.clone(),
        config.kafka_auth.clone(),
    ))?;

    let pool = PgPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(1))
        .connect_lazy(&config.database_url)?;
    let context_state = ContextState {
        environment_keys: EnvironmentKeys::new(pool, ENVIRONMENT_KEY_REFRESH_AFTER),
        external_topic: Arc::from(config.kafka_external_topic.as_str()),
    };

    let app = routes::app(sink.clone(), context_state);

    let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    info!("Server listening on {}", config.listen_addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    sink.shutdown(Duration::from_secs(10)).await;

    info!("Server shutdown complete");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.ok();
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    info!("Shutdown signal received");
}
