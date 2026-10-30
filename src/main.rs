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

use anyhow::Context;
use sqlx::postgres::PgPoolOptions;
use tokio::signal;
use tracing::info;

use auth::ContextState;
use config::{Config, SinkConfig};
use environment_keys::EnvironmentKeys;
use sink::{BatchingSink, ClickHouseWriter, KafkaSink, KafkaSinkConfig, Sink};

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
    let (sink, external_topic) = match &config.sink {
        SinkConfig::Kafka(kafka) => {
            info!(
                listen_addr = %config.listen_addr,
                kafka_bootstrap_servers = %kafka.bootstrap_servers,
                kafka_topic = %kafka.topic,
                kafka_auth = ?kafka.auth,
                "Starting flagsmith-analytics server"
            );
            let sink = KafkaSink::new(KafkaSinkConfig::new(
                kafka.bootstrap_servers.clone(),
                kafka.topic.clone(),
                kafka.auth.clone(),
            ))?;
            (Sink::Kafka(sink), Some(kafka.external_topic.as_str()))
        }
        SinkConfig::ClickHouse { clickhouse, batch } => {
            info!(
                listen_addr = %config.listen_addr,
                clickhouse = ?clickhouse,
                batch = ?batch,
                "Starting flagsmith-analytics server"
            );
            let writer = ClickHouseWriter::new(clickhouse.clone());
            writer
                .check()
                .await
                .context("ClickHouse startup check failed")?;
            (
                Sink::ClickHouse(BatchingSink::new(writer, batch.clone())),
                None,
            )
        }
    };

    let pool = PgPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(1))
        .connect_lazy(&config.database_url)?;
    let context_state = ContextState {
        environment_keys: EnvironmentKeys::new(pool, ENVIRONMENT_KEY_REFRESH_AFTER),
        external_topic: external_topic.map(Arc::from),
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
