mod auth;
mod config;
mod environment_keys;
mod lookup;
mod models;
mod routes;
mod sink;
#[cfg(test)]
mod test_helpers;
mod wal;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use sqlx::postgres::PgPoolOptions;
use tokio::signal;
use tracing::info;

use auth::ContextState;
use config::{Config, SinkConfig};
use environment_keys::EnvironmentKeys;
use sink::{BatchingSink, ClickHouseWriter, KafkaSink, KafkaSinkConfig, Sink, WalSink};
use wal::{Wal, WalHandle};

const ENVIRONMENT_KEY_REFRESH_AFTER: Duration = Duration::from_secs(300);
/// How long shutdown waits for a sink to write what it holds; the WAL sink
/// uses `SINK_DRAIN_TIMEOUT_SECS` instead.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(20);

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
    let mut shutdown_timeout = SHUTDOWN_TIMEOUT;
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
        SinkConfig::ClickHouse {
            clickhouse,
            batch,
            wal,
        } => {
            info!(
                listen_addr = %config.listen_addr,
                clickhouse = ?clickhouse,
                batch = ?batch,
                wal = ?wal,
                "Starting flagsmith-analytics server"
            );
            let writer = ClickHouseWriter::new(clickhouse.clone());
            writer
                .check()
                .await
                .context("ClickHouse startup check failed")?;
            let sink = match wal {
                Some(wal) => {
                    let dir = &wal.dir;
                    shutdown_timeout = wal.drain_timeout;
                    let wal = Wal::open(dir)
                        .with_context(|| format!("Could not open the WAL in {}", dir.display()))?;
                    Sink::ClickHouseWal(WalSink::new(writer, WalHandle::spawn(wal), batch.clone()))
                }
                None => Sink::ClickHouse(BatchingSink::new(writer, batch.clone())),
            };
            (sink, None)
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

    sink.shutdown(shutdown_timeout).await;

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
