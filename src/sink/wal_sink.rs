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
use crate::wal::{Chunk, WalError, WalHandle};

/// Waits between attempts to write a chunk that failed with a transient or
/// config error: doubling from the first, up to the max, until it succeeds.
const FIRST_RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

struct Shared<W: BatchWriter> {
    writer: W,
    wal: WalHandle,
    config: BatchConfig,
    /// Events appended since the flusher last read the WAL.
    events_since_read: AtomicUsize,
    flush_now: Notify,
    stop: Notify,
    /// The first config or WAL error; stays set until restart.
    fatal_error: OnceLock<String>,
}

impl<W: BatchWriter> Shared<W> {
    /// Fails the health check unless the WAL is only full.
    fn wal_failed(&self, e: &WalError) {
        if !matches!(e, WalError::Full { .. }) {
            let _ = self.fatal_error.set(e.to_string());
        }
    }
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
            events_since_read: AtomicUsize::new(0),
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
        if let Err(e) = self.shared.wal.append(records).await {
            self.shared.wal_failed(&e);
            return Err(e.into());
        }
        let events_since_read = self
            .shared
            .events_since_read
            .fetch_add(events, Ordering::SeqCst)
            + events;
        if events_since_read >= self.shared.config.flush_max_events {
            self.shared.flush_now.notify_one();
        }
        Ok(())
    }

    /// A config error, such as bad credentials or a missing table, or a
    /// broken WAL, that stops every write until the server is fixed.
    pub fn fatal_error(&self) -> Option<String> {
        self.shared.fatal_error.get().cloned()
    }

    /// Write the WAL out until it is empty, retrying transient errors, bounded
    /// by `timeout`. Rows not yet written stay in the WAL for the next start.
    /// Then closes the WAL.
    pub async fn shutdown(self, timeout: Duration) {
        self.shared.stop.notify_one();
        let flusher = self.flusher.lock().expect("flusher lock").take();
        if let Some(mut flusher) = flusher {
            match tokio::time::timeout(timeout, &mut flusher).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => warn!(error = %e, "Sink flush task failed"),
                Err(_) => {
                    flusher.abort();
                    warn!(
                        "Sink drain timed out; unwritten rows stay in the WAL for the next start"
                    );
                }
            }
        }
        self.shared.wal.close().await;
    }
}

/// Writes the WAL's oldest chunk, then the next one straight away while
/// there is a backlog; otherwise waits for the interval or an early flush.
/// Once stopped, writes chunk after chunk until the WAL is empty.
async fn run<W: BatchWriter>(shared: Arc<Shared<W>>) {
    let period = shared.config.flush_interval;
    let mut interval = tokio::time::interval_at(Instant::now() + period, period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut has_backlog = true;
    let mut draining = false;
    loop {
        if !has_backlog && !draining {
            tokio::select! {
                _ = interval.tick() => {}
                _ = shared.flush_now.notified() => {}
                _ = shared.stop.notified() => draining = true,
            }
        }
        shared.events_since_read.store(0, Ordering::SeqCst);
        let chunk = match shared.wal.read_oldest(shared.config.flush_max_events).await {
            Ok(chunk) => chunk,
            Err(e) => {
                error!(error = %e, "Sink could not read the WAL");
                shared.wal_failed(&e);
                if draining {
                    return;
                }
                has_backlog = false;
                continue;
            }
        };
        has_backlog = chunk.records.len() >= shared.config.flush_max_events;
        if chunk.is_empty() {
            if draining {
                return;
            }
            continue;
        }
        write_with_retries(&shared, chunk).await;
    }
}

/// Writes the chunk and removes it from the WAL. Retries transient and
/// config errors until the write succeeds, recording a config error to fail
/// the health check; drops the chunk on bad data.
async fn write_with_retries<W: BatchWriter>(shared: &Shared<W>, chunk: Chunk) {
    if chunk.undecodable_rows > 0 {
        error!(
            rows = chunk.undecodable_rows,
            "Sink rows undecodable, dropped"
        );
    }
    let events = chunk.records.len();
    let mut delay = FIRST_RETRY_DELAY;
    loop {
        let result = if events == 0 {
            Ok(())
        } else {
            shared.writer.write(&chunk.records).await
        };
        let e = match result {
            Ok(()) => {
                if let Err(e) = shared.wal.remove(chunk).await {
                    error!(events, error = %e, "Sink rows written but left in the WAL; they will be written again");
                    shared.wal_failed(&e);
                }
                return;
            }
            Err(e) => e,
        };
        match e.kind() {
            ErrorKind::Transient => {
                warn!(events, error = %e, "Sink chunk write failed, retrying");
            }
            // TODO: one bad row drops the whole chunk; split it to find the bad rows if this happens often.
            ErrorKind::BadData => {
                error!(events, error = %e, "Sink chunk dropped: rows rejected");
                if let Err(e) = shared.wal.remove(chunk).await {
                    error!(events, error = %e, "Sink rejected rows left in the WAL; they will be written again");
                    shared.wal_failed(&e);
                }
                return;
            }
            ErrorKind::Config => {
                error!(events, error = %e, "Sink chunk write failed: sink misconfigured");
                let _ = shared.fatal_error.set(e.to_string());
            }
        }
        tokio::time::sleep(delay).await;
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
    async fn wait_for(condition: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !condition() {
            assert!(Instant::now() < deadline, "timed out");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wal_timestamps(wal: &WalHandle) -> Vec<i64> {
        let chunk = wal.read_oldest(usize::MAX).await.unwrap();
        chunk
            .records
            .iter()
            .map(|record| record.timestamp)
            .collect()
    }

    /// Waits up to 5 s for the flusher to empty the WAL.
    async fn wait_for_empty_wal(wal: &WalHandle) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !wal_timestamps(wal).await.is_empty() {
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

        wait_for(|| !writer.written.lock().unwrap().is_empty()).await;
        assert_eq!(*writer.written.lock().unwrap(), vec![vec![1, 2]]);
        wait_for_empty_wal(&wal).await;
    }

    #[tokio::test]
    async fn test_writes_what_the_wal_held_at_startup() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path()).unwrap();
        wal.append(&[records(&[1, 2])]).unwrap();
        let writer = FakeWriter::default();
        let _sink = WalSink::new(writer.clone(), WalHandle::spawn(wal), config(100));

        wait_for(|| !writer.written.lock().unwrap().is_empty()).await;
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

        wait_for(|| !writer.written.lock().unwrap().is_empty()).await;
        assert_eq!(writer.attempts.load(Ordering::SeqCst), 2);
        assert_eq!(sink.fatal_error(), None);
    }

    #[tokio::test]
    async fn test_shutdown_drains_the_wal() {
        let dir = tempfile::tempdir().unwrap();
        let writer = FakeWriter::default();
        let sink = WalSink::new(writer.clone(), spawn_wal(dir.path()), config(100));

        sink.put(records(&[1, 2])).await.unwrap();
        sink.put(records(&[3])).await.unwrap();
        sink.shutdown(Duration::from_secs(5)).await;

        let written: Vec<i64> = writer.written.lock().unwrap().concat();
        assert_eq!(written, vec![1, 2, 3]);
        // Shutdown closed the WAL, so it reopens at once.
        let wal = Wal::open(dir.path()).unwrap();
        assert!(wal.read_oldest(100).unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_drain_retries_a_transient_error() {
        let dir = tempfile::tempdir().unwrap();
        let writer = FakeWriter {
            failures: 1,
            error: Some(|| clickhouse::error::Error::TimedOut),
            ..FakeWriter::default()
        };
        let sink = WalSink::new(writer.clone(), spawn_wal(dir.path()), config(100));

        sink.put(records(&[1])).await.unwrap();
        sink.shutdown(Duration::from_secs(5)).await;

        assert_eq!(*writer.written.lock().unwrap(), vec![vec![1]]);
        let wal = Wal::open(dir.path()).unwrap();
        assert!(wal.read_oldest(100).unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_drain_timeout_leaves_rows_in_the_wal() {
        let dir = tempfile::tempdir().unwrap();
        let writer = FakeWriter {
            failures: usize::MAX,
            error: Some(|| clickhouse::error::Error::TimedOut),
            ..FakeWriter::default()
        };
        let sink = WalSink::new(writer.clone(), spawn_wal(dir.path()), config(1));

        sink.put(records(&[1, 2])).await.unwrap();
        wait_for(|| writer.attempts.load(Ordering::SeqCst) > 0).await;
        sink.shutdown(Duration::from_millis(100)).await;

        let wal = Wal::open(dir.path()).unwrap();
        assert_eq!(wal.read_oldest(100).unwrap().records, records(&[1, 2]));
    }

    #[tokio::test]
    async fn test_drops_bad_data_from_the_wal_and_stays_healthy() {
        let dir = tempfile::tempdir().unwrap();
        let wal = spawn_wal(dir.path());
        let writer = FakeWriter {
            failures: 1,
            error: Some(|| clickhouse::error::Error::Custom("bad row".to_string())),
            ..FakeWriter::default()
        };
        let sink = WalSink::new(writer.clone(), wal.clone(), config(1));

        sink.put(records(&[1])).await.unwrap();

        wait_for(|| writer.attempts.load(Ordering::SeqCst) > 0).await;
        wait_for_empty_wal(&wal).await;
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

        wait_for(|| sink.fatal_error().is_some()).await;
        assert_eq!(wal_timestamps(&wal).await, vec![1]);
    }

    #[tokio::test]
    async fn test_put_fails_when_the_wal_is_full() {
        let dir = tempfile::tempdir().unwrap();
        let wal = WalHandle::spawn(Wal::open(dir.path()).unwrap().with_max_bytes(1));
        let sink = WalSink::new(FakeWriter::default(), wal, config(1));

        let result = sink.put(records(&[1])).await;

        assert!(matches!(
            result,
            Err(SinkError::Wal(e)) if matches!(*e, WalError::Full { .. })
        ));
        assert_eq!(sink.fatal_error(), None);
    }

    #[tokio::test]
    async fn test_put_fails_health_when_the_wal_is_broken() {
        let sink = WalSink::new(FakeWriter::default(), WalHandle::stopped(), config(1));

        assert!(sink.put(records(&[1])).await.is_err());
        assert!(sink.fatal_error().is_some());
    }

    #[tokio::test]
    async fn test_flusher_fails_health_when_the_wal_is_broken() {
        let sink = WalSink::new(FakeWriter::default(), WalHandle::stopped(), config(1));

        wait_for(|| sink.fatal_error().is_some()).await;
    }
}
