use super::batching::BatchWriter;
use super::clickhouse::{ClickHouseConfig, ClickHouseWriter};
use super::SinkError;
use crate::models::EventRecord;

#[derive(Debug, Clone)]
pub enum WarehouseConfig {
    ClickHouse(ClickHouseConfig),
}

/// The warehouse a direct sink writes to, buffered in memory or in the WAL.
pub enum Warehouse {
    ClickHouse(ClickHouseWriter),
}

impl Warehouse {
    /// Connects lazily, on the first write.
    pub fn new(config: WarehouseConfig) -> Self {
        match config {
            WarehouseConfig::ClickHouse(config) => Self::ClickHouse(ClickHouseWriter::new(config)),
        }
    }

    /// Fails if the warehouse is unreachable, rejects the credentials, or
    /// has no table that matches the events.
    pub async fn check(&self) -> Result<(), SinkError> {
        match self {
            Self::ClickHouse(writer) => writer.check().await,
        }
    }
}

impl BatchWriter for Warehouse {
    async fn write(&self, records: &[EventRecord]) -> Result<(), SinkError> {
        match self {
            Self::ClickHouse(writer) => writer.write(records).await,
        }
    }
}
