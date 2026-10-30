mod batching;
mod clickhouse;
mod kafka;
mod postgres;

use std::time::Duration;

pub use batching::{BatchConfig, BatchingSink};
pub use clickhouse::{ClickHouseConfig, ClickHouseWriter};
pub use kafka::{KafkaAuth, KafkaSink, KafkaSinkConfig};
pub use postgres::PostgresWriter;

use crate::models::EventRecord;

/// Where accepted events are written.
#[derive(Clone)]
pub enum Sink {
    Kafka(KafkaSink),
    ClickHouse(BatchingSink<ClickHouseWriter>),
    Postgres(BatchingSink<PostgresWriter>),
}

impl Sink {
    /// Write every record to the destination (the sink's default when `None`).
    /// Returns once the sink has accepted all of them.
    pub async fn put(
        &self,
        destination: Option<&str>,
        records: Vec<EventRecord>,
    ) -> Result<(), SinkError> {
        match self {
            Self::Kafka(sink) => Ok(sink.put(destination, &records).await?),
            // A single table: there is no per-environment destination.
            Self::ClickHouse(sink) => {
                sink.put(records);
                Ok(())
            }
            Self::Postgres(sink) => {
                sink.put(records);
                Ok(())
            }
        }
    }

    /// A permanent sink failure that should fail the health check.
    pub fn fatal_error(&self) -> Option<String> {
        match self {
            Self::Kafka(sink) => sink.fatal_error(),
            Self::ClickHouse(sink) => sink.fatal_error(),
            Self::Postgres(sink) => sink.fatal_error(),
        }
    }

    /// Drain anything still buffered, bounded by `timeout`.
    pub async fn shutdown(self, timeout: Duration) {
        match self {
            Self::Kafka(sink) => sink.shutdown(timeout).await,
            Self::ClickHouse(sink) => sink.shutdown(timeout).await,
            Self::Postgres(sink) => sink.shutdown(timeout).await,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error(transparent)]
    Kafka(#[from] kafka::DeliveryError),
    #[error("ClickHouse insert failed: {0}")]
    ClickHouse(#[from] ::clickhouse::error::Error),
    #[error("PostgreSQL insert failed: {0}")]
    Postgres(#[from] sqlx::Error),
    #[error("PostgreSQL insert timed out")]
    PostgresTimeout,
}

/// What a failed write means for the batch and the server.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ErrorKind {
    /// May pass on retry: network, timeout, overload, or unrecognised.
    Transient,
    /// The store rejects the rows; retrying cannot help.
    BadData,
    /// The server is misconfigured, e.g. bad credentials or a missing
    /// table; nothing will be written until it is fixed.
    Config,
}

impl SinkError {
    /// Only batching sinks retry; the Kafka producer retries internally.
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::Kafka(_) => ErrorKind::Transient,
            Self::ClickHouse(e) => clickhouse::error_kind(e),
            Self::Postgres(e) => postgres::error_kind(e),
            Self::PostgresTimeout => ErrorKind::Transient,
        }
    }
}
