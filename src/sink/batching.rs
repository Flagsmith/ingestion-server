use std::future::Future;
use std::mem;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{error, warn};

use super::{ErrorKind, SinkError};
use crate::models::EventRecord;

/// Waits between attempts to write a batch that failed with a transient error,
/// before it is dropped: about 17 s in all, to ride out a ClickHouse restart.
const RETRY_DELAYS: [Duration; 5] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(5),
    Duration::from_secs(5),
];

/// Writes one batch of records to a store.
pub trait BatchWriter: Send + Sync + 'static {
    fn write(&self, records: &[EventRecord]) -> impl Future<Output = Result<(), SinkError>> + Send;
}

#[derive(Debug, Clone)]
pub struct BatchConfig {
    pub flush_interval: Duration,
    /// Flush early once this many events are buffered. Not a cap: the buffer
    /// is unbounded and keeps growing while a write is slow or failing.
    pub flush_max_events: usize,
}

struct Shared<W: BatchWriter> {
    writer: W,
    config: BatchConfig,
    buffer: Mutex<Vec<EventRecord>>,
    flush_now: Notify,
    stop: Notify,
    /// The first config error; stays set until restart.
    fatal_error: OnceLock<String>,
}

pub struct BatchingSink<W: BatchWriter> {
    shared: Arc<Shared<W>>,
    flusher: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl<W: BatchWriter> Clone for BatchingSink<W> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            flusher: self.flusher.clone(),
        }
    }
}

impl<W: BatchWriter> BatchingSink<W> {
    pub fn new(writer: W, config: BatchConfig) -> Self {
        let shared = Arc::new(Shared {
            writer,
            config,
            buffer: Mutex::new(Vec::new()),
            flush_now: Notify::new(),
            stop: Notify::new(),
            fatal_error: OnceLock::new(),
        });
        let flusher = tokio::spawn(run(shared.clone()));
        Self {
            shared,
            flusher: Arc::new(Mutex::new(Some(flusher))),
        }
    }

    /// Buffer the records; never blocks or fails.
    pub fn put(&self, records: Vec<EventRecord>) {
        let mut buffer = self.shared.buffer.lock().expect("buffer lock");
        buffer.extend(records);
        if buffer.len() >= self.shared.config.flush_max_events {
            self.shared.flush_now.notify_one();
        }
    }

    /// A config error, such as bad credentials or a missing table, that
    /// stops every write until the server is fixed.
    pub fn fatal_error(&self) -> Option<String> {
        self.shared.fatal_error.get().cloned()
    }

    /// Stop the flusher and write what is buffered, bounded by `timeout`.
    pub async fn shutdown(self, timeout: Duration) {
        self.shared.stop.notify_one();
        let Some(flusher) = self.flusher.lock().expect("flusher lock").take() else {
            return;
        };
        match tokio::time::timeout(timeout, flusher).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!(error = %e, "Sink flush task failed"),
            Err(_) => warn!("Sink final flush incomplete"),
        }
    }
}

async fn run<W: BatchWriter>(shared: Arc<Shared<W>>) {
    let period = shared.config.flush_interval;
    let mut interval = tokio::time::interval_at(Instant::now() + period, period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        let stopping = tokio::select! {
            _ = interval.tick() => false,
            _ = shared.flush_now.notified() => false,
            _ = shared.stop.notified() => true,
        };
        flush(&shared).await;
        if stopping {
            return;
        }
    }
}

async fn flush<W: BatchWriter>(shared: &Shared<W>) {
    let records = mem::take(&mut *shared.buffer.lock().expect("buffer lock"));
    if records.is_empty() {
        return;
    }
    write_with_retries(shared, &records).await;
}

/// Retries transient errors per `RETRY_DELAYS`. Drops the batch at once on
/// bad data or a config error, and records a config error to fail the health
/// check.
async fn write_with_retries<W: BatchWriter>(shared: &Shared<W>, records: &[EventRecord]) {
    let rows = records.len();
    for retry in 0.. {
        let Err(e) = shared.writer.write(records).await else {
            return;
        };
        match e.kind() {
            ErrorKind::Transient => {}
            ErrorKind::BadData => {
                error!(rows, error = %e, "Sink batch dropped: rows rejected");
                return;
            }
            ErrorKind::Config => {
                error!(rows, error = %e, "Sink batch dropped: sink misconfigured");
                let _ = shared.fatal_error.set(e.to_string());
                return;
            }
        }
        let Some(&delay) = RETRY_DELAYS.get(retry) else {
            error!(rows, error = %e, "Sink batch dropped after retries");
            return;
        };
        warn!(rows, retry, error = %e, "Sink batch write failed, retrying");
        tokio::time::sleep(delay).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// Records every write attempt; each takes `delay`, and the first
    /// `failures` fail with `error`.
    #[derive(Clone, Default)]
    struct FakeWriter {
        failures: usize,
        error: Option<fn() -> clickhouse::error::Error>,
        delay: Duration,
        attempts: Arc<AtomicUsize>,
        written: Arc<Mutex<Vec<Vec<i64>>>>,
    }

    impl BatchWriter for FakeWriter {
        async fn write(&self, records: &[EventRecord]) -> Result<(), SinkError> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            if attempt < self.failures {
                let error = self.error.expect("an error to fail with");
                return Err(error().into());
            }
            let timestamps = records.iter().map(|record| record.timestamp).collect();
            self.written.lock().unwrap().push(timestamps);
            Ok(())
        }
    }

    fn records(timestamps: &[i64]) -> Vec<EventRecord> {
        timestamps
            .iter()
            .map(|&timestamp| EventRecord {
                environment_key: "env".to_string(),
                collected_at: timestamp,
                sdk_language: "rust".to_string(),
                sdk_version: "1.0.0".to_string(),
                event: "purchase".to_string(),
                feature_name: None,
                identifier: None,
                value: serde_json::Value::Null,
                traits: None,
                metadata: None,
                timestamp,
            })
            .collect()
    }

    fn config(flush_interval: Duration, flush_max_events: usize) -> BatchConfig {
        BatchConfig {
            flush_interval,
            flush_max_events,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_flushes_on_the_interval() {
        let writer = FakeWriter::default();
        let sink = BatchingSink::new(writer.clone(), config(Duration::from_secs(1), 100));

        sink.put(records(&[1, 2]));
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(writer.written.lock().unwrap().is_empty());

        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(*writer.written.lock().unwrap(), vec![vec![1, 2]]);
    }

    #[tokio::test(start_paused = true)]
    async fn test_flushes_early_at_flush_max_events() {
        let writer = FakeWriter::default();
        let sink = BatchingSink::new(writer.clone(), config(Duration::from_secs(3600), 2));

        sink.put(records(&[1]));
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(writer.written.lock().unwrap().is_empty());

        sink.put(records(&[2]));
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(*writer.written.lock().unwrap(), vec![vec![1, 2]]);
    }

    #[tokio::test(start_paused = true)]
    async fn test_keeps_buffering_while_a_write_is_slow() {
        let writer = FakeWriter {
            delay: Duration::from_secs(1),
            ..FakeWriter::default()
        };
        let sink = BatchingSink::new(writer.clone(), config(Duration::from_secs(3600), 2));

        sink.put(records(&[1, 2]));
        tokio::time::sleep(Duration::from_millis(1)).await;
        // [1, 2] is being written; the buffer grows past the threshold.
        sink.put(records(&[3, 4]));
        sink.put(records(&[5]));

        sink.shutdown(Duration::from_secs(5)).await;
        assert_eq!(
            *writer.written.lock().unwrap(),
            vec![vec![1, 2], vec![3, 4, 5]]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_drops_a_batch_after_the_retries() {
        let writer = FakeWriter {
            failures: usize::MAX,
            error: Some(|| clickhouse::error::Error::TimedOut),
            ..FakeWriter::default()
        };
        let sink = BatchingSink::new(writer.clone(), config(Duration::from_secs(1), 1));

        sink.put(records(&[1]));
        tokio::time::sleep(Duration::from_secs(60)).await;

        assert_eq!(
            writer.attempts.load(Ordering::SeqCst),
            1 + RETRY_DELAYS.len()
        );
        assert_eq!(sink.fatal_error(), None);
    }

    #[tokio::test(start_paused = true)]
    async fn test_drops_bad_data_without_retries_and_stays_healthy() {
        let writer = FakeWriter {
            failures: 1,
            error: Some(|| clickhouse::error::Error::Custom("bad row".to_string())),
            ..FakeWriter::default()
        };
        let sink = BatchingSink::new(writer.clone(), config(Duration::from_secs(3600), 1));

        sink.put(records(&[1]));
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(writer.attempts.load(Ordering::SeqCst), 1);
        assert!(writer.written.lock().unwrap().is_empty());
        assert_eq!(sink.fatal_error(), None);
    }

    #[tokio::test(start_paused = true)]
    async fn test_drops_a_batch_without_retries_on_a_config_error() {
        let writer = FakeWriter {
            failures: 1,
            error: Some(|| clickhouse::error::Error::SchemaMismatch("no column".to_string())),
            ..FakeWriter::default()
        };
        let sink = BatchingSink::new(writer.clone(), config(Duration::from_secs(3600), 1));

        sink.put(records(&[1]));
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(writer.attempts.load(Ordering::SeqCst), 1);
        assert!(writer.written.lock().unwrap().is_empty());
        let fatal_error = sink.fatal_error();
        assert!(fatal_error.is_some());

        // Stays set after a later write succeeds.
        sink.put(records(&[2]));
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(*writer.written.lock().unwrap(), vec![vec![2]]);
        assert_eq!(sink.fatal_error(), fatal_error);
    }

    #[tokio::test(start_paused = true)]
    async fn test_shutdown_writes_what_is_buffered() {
        let writer = FakeWriter::default();
        let sink = BatchingSink::new(writer.clone(), config(Duration::from_secs(3600), 100));

        sink.put(records(&[1, 2, 3]));
        sink.shutdown(Duration::from_secs(1)).await;

        assert_eq!(*writer.written.lock().unwrap(), vec![vec![1, 2, 3]]);
    }
}
