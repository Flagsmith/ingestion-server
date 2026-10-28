use std::future::Future;
use std::hash::{BuildHasher, RandomState};
use std::mem;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{error, warn};

use super::SinkError;
use crate::models::EventRecord;

/// Retries after the first failed write, before the batch is dropped.
const MAX_RETRIES: u32 = 3;
/// Doubles per retry: about 200 ms, 400 ms, 800 ms before jitter.
const RETRY_BASE_DELAY: Duration = Duration::from_millis(200);

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
}

/// Buffers records in memory and writes them in batches from one background
/// task. `put` returns once the records are buffered, so records still
/// buffered are lost if the process dies.
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
    /// Starts the flusher task; needs a Tokio runtime.
    pub fn new(writer: W, config: BatchConfig) -> Self {
        let shared = Arc::new(Shared {
            writer,
            config,
            buffer: Mutex::new(Vec::new()),
            flush_now: Notify::new(),
            stop: Notify::new(),
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
    write_with_retries(&shared.writer, &records).await;
}

async fn write_with_retries<W: BatchWriter>(writer: &W, records: &[EventRecord]) {
    let mut retry = 0;
    loop {
        match writer.write(records).await {
            Ok(()) => return,
            Err(e) if retry < MAX_RETRIES => {
                warn!(rows = records.len(), retry, error = %e, "Sink batch write failed, retrying");
                tokio::time::sleep(backoff(retry)).await;
                retry += 1;
            }
            Err(e) => {
                error!(rows = records.len(), error = %e, "Sink batch dropped after retries");
                return;
            }
        }
    }
}

/// Exponential backoff with ±50% jitter, so tasks that failed together do not
/// retry together.
fn backoff(retry: u32) -> Duration {
    let jitter_permille = RandomState::new().hash_one(retry) % 1000;
    (RETRY_BASE_DELAY * 2u32.pow(retry)).mul_f64(0.5 + jitter_permille as f64 / 1000.0)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// Records every write attempt; each takes `delay`, and all fail when
    /// `fail` is set.
    #[derive(Clone, Default)]
    struct FakeWriter {
        fail: bool,
        delay: Duration,
        attempts: Arc<AtomicUsize>,
        written: Arc<Mutex<Vec<Vec<i64>>>>,
    }

    impl BatchWriter for FakeWriter {
        async fn write(&self, records: &[EventRecord]) -> Result<(), SinkError> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            if self.fail {
                return Err(clickhouse::error::Error::Custom("failed".to_string()).into());
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
    async fn flushes_on_the_interval() {
        let writer = FakeWriter::default();
        let sink = BatchingSink::new(writer.clone(), config(Duration::from_secs(1), 100));

        sink.put(records(&[1, 2]));
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(writer.written.lock().unwrap().is_empty());

        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(*writer.written.lock().unwrap(), vec![vec![1, 2]]);
    }

    #[tokio::test(start_paused = true)]
    async fn flushes_early_at_flush_max_events() {
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
    async fn keeps_buffering_while_a_write_is_slow() {
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
    async fn drops_a_batch_after_the_retries() {
        let writer = FakeWriter {
            fail: true,
            ..FakeWriter::default()
        };
        let sink = BatchingSink::new(writer.clone(), config(Duration::from_secs(1), 1));

        sink.put(records(&[1]));
        tokio::time::sleep(Duration::from_secs(10)).await;

        assert_eq!(
            writer.attempts.load(Ordering::SeqCst),
            1 + MAX_RETRIES as usize
        );
        sink.put(records(&[2]));
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_writes_what_is_buffered() {
        let writer = FakeWriter::default();
        let sink = BatchingSink::new(writer.clone(), config(Duration::from_secs(3600), 100));

        sink.put(records(&[1, 2, 3]));
        sink.shutdown(Duration::from_secs(1)).await;

        assert_eq!(*writer.written.lock().unwrap(), vec![vec![1, 2, 3]]);
    }

    #[test]
    fn backoff_doubles_within_the_jitter_band() {
        for retry in 0..MAX_RETRIES {
            let base = RETRY_BASE_DELAY * 2u32.pow(retry);
            let delay = backoff(retry);
            assert!(delay >= base / 2 && delay < base * 3 / 2, "{delay:?}");
        }
    }
}
