mod kafka;

use std::time::Duration;

pub use kafka::{KafkaAuth, KafkaSink, KafkaSinkConfig};

use crate::models::EventRecord;

/// Where accepted events are written.
#[derive(Clone)]
pub enum Sink {
    Kafka(KafkaSink),
}

impl Sink {
    /// Write every record to the destination (the sink's default when `None`).
    /// Returns once the sink has accepted all of them.
    pub async fn put(
        &self,
        destination: Option<&str>,
        records: &[EventRecord<'_>],
    ) -> Result<(), kafka::SinkError> {
        match self {
            Self::Kafka(sink) => sink.put(destination, records).await,
        }
    }

    /// A permanent sink failure that should fail the health check.
    pub fn fatal_error(&self) -> Option<String> {
        match self {
            Self::Kafka(sink) => sink.fatal_error(),
        }
    }

    /// Drain anything still buffered, bounded by `timeout`.
    pub async fn shutdown(self, timeout: Duration) {
        match self {
            Self::Kafka(sink) => sink.shutdown(timeout).await,
        }
    }
}
