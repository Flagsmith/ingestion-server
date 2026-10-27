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

/// Writes one batch of owned rows to a store.
pub trait BatchWriter: Send + Sync + 'static {
    type Row: Send + Sync + 'static;

    fn row(record: &EventRecord<'_>) -> Self::Row;

    /// Approximate in-memory size, counted against the buffer limit.
    fn row_size(row: &Self::Row) -> usize;

    fn write(&self, rows: &[Self::Row]) -> impl Future<Output = Result<(), SinkError>> + Send;
}

#[derive(Debug, Clone)]
pub struct BatchConfig {
    pub flush_interval: Duration,
    /// Flush early once this many rows are buffered; also the most rows per write.
    pub max_rows: usize,
    /// Bound on buffered plus in-flight rows; `put` fails beyond it.
    pub max_bytes: usize,
}

struct Buffer<R> {
    rows: Vec<R>,
    /// Size of `rows` plus the batch being flushed.
    bytes: usize,
}

struct Shared<W: BatchWriter> {
    writer: W,
    config: BatchConfig,
    buffer: Mutex<Buffer<W::Row>>,
    flush_now: Notify,
    stop: Notify,
}

/// Buffers rows in memory and writes them in batches from one background
/// task. `put` returns once the rows are buffered, so rows still buffered are
/// lost if the process dies.
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
            buffer: Mutex::new(Buffer {
                rows: Vec::new(),
                bytes: 0,
            }),
            flush_now: Notify::new(),
            stop: Notify::new(),
        });
        let flusher = tokio::spawn(run(shared.clone()));
        Self {
            shared,
            flusher: Arc::new(Mutex::new(Some(flusher))),
        }
    }

    /// Buffer every record, or none of them if that would pass the byte limit.
    pub fn put(&self, records: &[EventRecord<'_>]) -> Result<(), SinkError> {
        let rows: Vec<W::Row> = records.iter().map(W::row).collect();
        let bytes: usize = rows.iter().map(W::row_size).sum();
        let config = &self.shared.config;
        let mut buffer = self.shared.buffer.lock().expect("buffer lock");
        if buffer.bytes + bytes > config.max_bytes {
            return Err(SinkError::BufferFull);
        }
        buffer.bytes += bytes;
        buffer.rows.extend(rows);
        if buffer.rows.len() >= config.max_rows {
            self.shared.flush_now.notify_one();
        }
        Ok(())
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
    let rows = mem::take(&mut shared.buffer.lock().expect("buffer lock").rows);
    if rows.is_empty() {
        return;
    }
    for chunk in rows.chunks(shared.config.max_rows) {
        write_with_retries(&shared.writer, chunk).await;
    }
    let flushed: usize = rows.iter().map(W::row_size).sum();
    shared.buffer.lock().expect("buffer lock").bytes -= flushed;
}

async fn write_with_retries<W: BatchWriter>(writer: &W, rows: &[W::Row]) {
    let mut retry = 0;
    loop {
        match writer.write(rows).await {
            Ok(()) => return,
            Err(e) if retry < MAX_RETRIES => {
                warn!(rows = rows.len(), retry, error = %e, "Sink batch write failed, retrying");
                tokio::time::sleep(backoff(retry)).await;
                retry += 1;
            }
            Err(e) => {
                error!(rows = rows.len(), error = %e, "Sink batch dropped after retries");
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

    const ROW_SIZE: usize = 100;
    static NULL: serde_json::Value = serde_json::Value::Null;

    /// Records every write attempt; fails all of them when `fail` is set.
    #[derive(Clone, Default)]
    struct FakeWriter {
        fail: bool,
        attempts: Arc<AtomicUsize>,
        written: Arc<Mutex<Vec<Vec<i64>>>>,
    }

    impl BatchWriter for FakeWriter {
        type Row = i64;

        fn row(record: &EventRecord<'_>) -> i64 {
            record.timestamp
        }

        fn row_size(_row: &i64) -> usize {
            ROW_SIZE
        }

        async fn write(&self, rows: &[i64]) -> Result<(), SinkError> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(SinkError::BufferFull);
            }
            self.written.lock().unwrap().push(rows.to_vec());
            Ok(())
        }
    }

    fn records(timestamps: &[i64]) -> Vec<EventRecord<'static>> {
        timestamps
            .iter()
            .map(|&timestamp| EventRecord {
                environment_key: "env",
                collected_at: timestamp,
                sdk_language: "rust",
                sdk_version: "1.0.0",
                event: "purchase",
                feature_name: None,
                identifier: None,
                value: &NULL,
                traits: None,
                metadata: None,
                timestamp,
            })
            .collect()
    }

    fn config(flush_interval: Duration, max_rows: usize, max_bytes: usize) -> BatchConfig {
        BatchConfig {
            flush_interval,
            max_rows,
            max_bytes,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn flushes_on_the_interval() {
        let writer = FakeWriter::default();
        let sink = BatchingSink::new(writer.clone(), config(Duration::from_secs(1), 100, 10_000));

        sink.put(&records(&[1, 2])).unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(writer.written.lock().unwrap().is_empty());

        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(*writer.written.lock().unwrap(), vec![vec![1, 2]]);
    }

    #[tokio::test(start_paused = true)]
    async fn flushes_early_on_max_rows() {
        let writer = FakeWriter::default();
        let sink = BatchingSink::new(writer.clone(), config(Duration::from_secs(3600), 2, 10_000));

        sink.put(&records(&[1, 2])).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        assert_eq!(*writer.written.lock().unwrap(), vec![vec![1, 2]]);
    }

    #[tokio::test(start_paused = true)]
    async fn rejects_a_put_that_would_overflow_the_buffer() {
        let writer = FakeWriter::default();
        let sink = BatchingSink::new(
            writer.clone(),
            config(Duration::from_secs(3600), 100, 2 * ROW_SIZE),
        );

        sink.put(&records(&[1])).unwrap();
        assert!(matches!(
            sink.put(&records(&[2, 3])),
            Err(SinkError::BufferFull)
        ));
        sink.put(&records(&[4])).unwrap();

        sink.shutdown(Duration::from_secs(1)).await;
        assert_eq!(*writer.written.lock().unwrap(), vec![vec![1, 4]]);
    }

    #[tokio::test(start_paused = true)]
    async fn drops_a_batch_after_the_retries_and_frees_its_space() {
        let writer = FakeWriter {
            fail: true,
            ..FakeWriter::default()
        };
        let sink = BatchingSink::new(
            writer.clone(),
            config(Duration::from_secs(1), 100, ROW_SIZE),
        );

        sink.put(&records(&[1])).unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;

        assert_eq!(
            writer.attempts.load(Ordering::SeqCst),
            1 + MAX_RETRIES as usize
        );
        sink.put(&records(&[2])).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_writes_what_is_buffered() {
        let writer = FakeWriter::default();
        let sink = BatchingSink::new(
            writer.clone(),
            config(Duration::from_secs(3600), 100, 10_000),
        );

        sink.put(&records(&[1, 2, 3])).unwrap();
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
