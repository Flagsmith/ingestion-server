use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{error, warn};

use super::batching::{BatchConfig, BatchWriter};
use super::{ErrorKind, SinkError};
use crate::models::EventRecord;
use crate::wal::{Chunk, WalHandle};

/// Waits between attempts to write a chunk that failed with a transient or
/// config error: doubling from the first, up to the max, until it succeeds.
const FIRST_RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

struct Shared<W: BatchWriter> {
    writer: W,
    wal: WalHandle,
    config: BatchConfig,
    /// Events appended since the flusher last read the WAL.
    pending: AtomicUsize,
    flush_now: Notify,
    stop: Notify,
    /// The first config error; stays set until restart.
    fatal_error: OnceLock<String>,
}

/// Like `BatchingSink`, but buffers in the WAL rather than in memory, so a
/// `put` that returns is durable, and nothing is dropped on transient errors.
pub struct WalSink<W: BatchWriter> {
    shared: Arc<Shared<W>>,
    flusher: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl<W: BatchWriter> Clone for WalSink<W> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            flusher: self.flusher.clone(),
        }
    }
}

impl<W: BatchWriter> WalSink<W> {
    /// Starts by writing whatever the WAL already holds.
    pub fn new(writer: W, wal: WalHandle, config: BatchConfig) -> Self {
        let shared = Arc::new(Shared {
            writer,
            wal,
            config,
            pending: AtomicUsize::new(0),
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

    /// Returns once the records are durable in the WAL.
    pub async fn put(&self, records: Vec<EventRecord>) -> Result<(), SinkError> {
        let events = records.len();
        self.shared.wal.append(records).await?;
        let pending = self.shared.pending.fetch_add(events, Ordering::SeqCst) + events;
        if pending >= self.shared.config.flush_max_events {
            self.shared.flush_now.notify_one();
        }
        Ok(())
    }

    /// A config error, such as bad credentials or a missing table, that
    /// stops every write until the server is fixed.
    pub fn fatal_error(&self) -> Option<String> {
        self.shared.fatal_error.get().cloned()
    }

    /// Stop the flusher, bounded by `timeout`. Rows not yet written stay in
    /// the WAL for the next start.
    pub async fn shutdown(self, timeout: Duration) {
        self.shared.stop.notify_one();
        let Some(flusher) = self.flusher.lock().expect("flusher lock").take() else {
            return;
        };
        match tokio::time::timeout(timeout, flusher).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!(error = %e, "Sink flush task failed"),
            Err(_) => warn!("Sink flusher still writing at shutdown"),
        }
    }
}

/// Writes the WAL's oldest chunk, then the next one straight away while
/// there is a backlog; otherwise waits for the interval or an early flush.
async fn run<W: BatchWriter>(shared: Arc<Shared<W>>) {
    let period = shared.config.flush_interval;
    let mut interval = tokio::time::interval_at(Instant::now() + period, period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut backlog = true;
    loop {
        if !backlog {
            tokio::select! {
                _ = interval.tick() => {}
                _ = shared.flush_now.notified() => {}
                _ = shared.stop.notified() => return,
            }
        }
        shared.pending.store(0, Ordering::SeqCst);
        let chunk = match shared.wal.read_oldest(shared.config.flush_max_events).await {
            Ok(chunk) => chunk,
            Err(e) => {
                error!(error = %e, "Sink could not read the WAL");
                backlog = false;
                continue;
            }
        };
        backlog = chunk.records.len() >= shared.config.flush_max_events;
        if chunk.is_empty() {
            continue;
        }
        if !write_with_retries(&shared, chunk).await {
            return;
        }
    }
}

/// Writes the chunk and removes it from the WAL. Retries transient and
/// config errors until the write succeeds, recording a config error to fail
/// the health check; moves the chunk to the rejected table on bad data.
/// Returns false if stopped while waiting to retry.
async fn write_with_retries<W: BatchWriter>(shared: &Shared<W>, chunk: Chunk) -> bool {
    if chunk.undecodable > 0 {
        error!(
            rows = chunk.undecodable,
            "Sink rows undecodable, moved to the WAL's rejected table"
        );
    }
    let rows = chunk.records.len();
    let mut delay = FIRST_RETRY_DELAY;
    loop {
        let result = if rows == 0 {
            Ok(())
        } else {
            shared.writer.write(&chunk.records).await
        };
        let e = match result {
            Ok(()) => {
                if let Err(e) = shared.wal.remove(chunk).await {
                    error!(rows, error = %e, "Sink rows written but left in the WAL; they will be written again");
                }
                return true;
            }
            Err(e) => e,
        };
        match e.kind() {
            ErrorKind::Transient => {
                warn!(rows, error = %e, "Sink chunk write failed, retrying");
            }
            ErrorKind::BadData => {
                error!(rows, error = %e, "Sink rows rejected, moved to the WAL's rejected table");
                if let Err(e) = shared.wal.reject(chunk).await {
                    error!(rows, error = %e, "Sink rows not moved to the rejected table");
                }
                return true;
            }
            ErrorKind::Config => {
                error!(rows, error = %e, "Sink chunk write failed: sink misconfigured");
                let _ = shared.fatal_error.set(e.to_string());
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = shared.stop.notified() => return false,
        }
        delay = (delay * 2).min(MAX_RETRY_DELAY);
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::sink::test_support::{records, FakeWriter};
    use crate::wal::Wal;

    fn config(flush_max_events: usize) -> BatchConfig {
        BatchConfig {
            flush_interval: Duration::from_secs(3600),
            flush_max_events,
        }
    }

    fn spawn_wal(dir: &Path) -> WalHandle {
        WalHandle::spawn(Wal::open(dir).unwrap())
    }

    /// Waits up to 5 s for `condition`.
    async fn eventually(condition: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !condition() {
            assert!(Instant::now() < deadline, "timed out");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// The WAL writer thread closes the file a moment after its last handle
    /// is dropped.
    async fn reopen(dir: &Path) -> Wal {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match Wal::open(dir) {
                Ok(wal) => return wal,
                Err(e) => assert!(Instant::now() < deadline, "{e}"),
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wal_events(wal: &WalHandle) -> Vec<i64> {
        let chunk = wal.read_oldest(usize::MAX).await.unwrap();
        chunk
            .records
            .iter()
            .map(|record| record.timestamp)
            .collect()
    }

    /// Waits up to 5 s for the flusher to empty the WAL.
    async fn eventually_empty(wal: &WalHandle) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !wal_events(wal).await.is_empty() {
            assert!(Instant::now() < deadline, "timed out");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn test_writes_records_and_removes_them_from_the_wal() {
        let dir = tempfile::tempdir().unwrap();
        let wal = spawn_wal(dir.path());
        let writer = FakeWriter::default();
        let sink = WalSink::new(writer.clone(), wal.clone(), config(2));

        // One put: the flusher's startup read sees all of it or none.
        sink.put(records(&[1, 2])).await.unwrap();

        eventually(|| !writer.written.lock().unwrap().is_empty()).await;
        assert_eq!(*writer.written.lock().unwrap(), vec![vec![1, 2]]);
        eventually_empty(&wal).await;
    }

    #[tokio::test]
    async fn test_writes_what_the_wal_held_at_startup() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path()).unwrap();
        wal.append(&[records(&[1, 2])]).unwrap();
        let writer = FakeWriter::default();
        let _sink = WalSink::new(writer.clone(), WalHandle::spawn(wal), config(100));

        eventually(|| !writer.written.lock().unwrap().is_empty()).await;
        assert_eq!(*writer.written.lock().unwrap(), vec![vec![1, 2]]);
    }

    #[tokio::test]
    async fn test_retries_a_transient_error_until_written() {
        let dir = tempfile::tempdir().unwrap();
        let writer = FakeWriter {
            failures: 1,
            error: Some(|| clickhouse::error::Error::TimedOut),
            ..FakeWriter::default()
        };
        let sink = WalSink::new(writer.clone(), spawn_wal(dir.path()), config(1));

        sink.put(records(&[1])).await.unwrap();

        eventually(|| !writer.written.lock().unwrap().is_empty()).await;
        assert_eq!(writer.attempts.load(Ordering::SeqCst), 2);
        assert_eq!(sink.fatal_error(), None);
    }

    #[tokio::test]
    async fn test_rows_survive_a_restart_while_writes_fail() {
        let dir = tempfile::tempdir().unwrap();
        let writer = FakeWriter {
            failures: usize::MAX,
            error: Some(|| clickhouse::error::Error::TimedOut),
            ..FakeWriter::default()
        };
        let sink = WalSink::new(writer.clone(), spawn_wal(dir.path()), config(1));

        sink.put(records(&[1, 2])).await.unwrap();
        eventually(|| writer.attempts.load(Ordering::SeqCst) > 0).await;
        sink.shutdown(Duration::from_secs(1)).await;

        let wal = reopen(dir.path()).await;
        assert_eq!(wal.read_oldest(100).unwrap().records, records(&[1, 2]));
    }

    #[tokio::test]
    async fn test_moves_bad_data_out_of_the_wal_and_stays_healthy() {
        let dir = tempfile::tempdir().unwrap();
        let wal = spawn_wal(dir.path());
        let writer = FakeWriter {
            failures: 1,
            error: Some(|| clickhouse::error::Error::Custom("bad row".to_string())),
            ..FakeWriter::default()
        };
        let sink = WalSink::new(writer.clone(), wal.clone(), config(1));

        sink.put(records(&[1])).await.unwrap();

        eventually(|| writer.attempts.load(Ordering::SeqCst) > 0).await;
        eventually_empty(&wal).await;
        assert!(writer.written.lock().unwrap().is_empty());
        assert_eq!(sink.fatal_error(), None);
    }

    #[tokio::test]
    async fn test_keeps_rows_and_fails_health_on_a_config_error() {
        let dir = tempfile::tempdir().unwrap();
        let wal = spawn_wal(dir.path());
        let writer = FakeWriter {
            failures: usize::MAX,
            error: Some(|| clickhouse::error::Error::SchemaMismatch("no column".to_string())),
            ..FakeWriter::default()
        };
        let sink = WalSink::new(writer.clone(), wal.clone(), config(1));

        sink.put(records(&[1])).await.unwrap();

        eventually(|| sink.fatal_error().is_some()).await;
        assert_eq!(wal_events(&wal).await, vec![1]);
    }
}
