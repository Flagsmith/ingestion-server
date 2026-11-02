mod batching;
mod clickhouse;
mod kafka;
#[cfg(test)]
mod test_support;
mod wal_sink;
mod warehouse;

use std::sync::Arc;
use std::time::Duration;

pub use batching::{BatchConfig, BatchingSink};
pub use clickhouse::ClickHouseConfig;
pub use kafka::{KafkaAuth, KafkaSink, KafkaSinkConfig};
pub use wal_sink::WalSink;
pub use warehouse::{Warehouse, WarehouseConfig};

use crate::models::EventRecord;
use crate::wal::WalError;

/// Where accepted events are written.
#[derive(Clone)]
pub enum Sink {
    Kafka(KafkaSink),
    /// A warehouse, buffered in memory.
    Memory(BatchingSink<Warehouse>),
    /// A warehouse, buffered in the WAL when `SINK_WAL_DIR` is set.
    Wal(WalSink<Warehouse>),
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
            Self::Memory(sink) => {
                sink.put(records);
                Ok(())
            }
            Self::Wal(sink) => sink.put(records).await,
        }
    }

    /// A permanent sink failure that should fail the health check.
    pub fn fatal_error(&self) -> Option<String> {
        match self {
            Self::Kafka(sink) => sink.fatal_error(),
            Self::Memory(sink) => sink.fatal_error(),
            Self::Wal(sink) => sink.fatal_error(),
        }
    }

    /// Drain anything still buffered, bounded by `timeout`.
    pub async fn shutdown(self, timeout: Duration) {
        match self {
            Self::Kafka(sink) => sink.shutdown(timeout).await,
            Self::Memory(sink) => sink.shutdown(timeout).await,
            Self::Wal(sink) => sink.shutdown(timeout).await,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error(transparent)]
    Kafka(#[from] kafka::DeliveryError),
    #[error("ClickHouse insert failed: {0}")]
    ClickHouse(#[from] ::clickhouse::error::Error),
    #[error(transparent)]
    Wal(#[from] Arc<WalError>),
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
            Self::Kafka(_) | Self::Wal(_) => ErrorKind::Transient,
            Self::ClickHouse(e) => clickhouse::error_kind(e),
        }
    }
}
