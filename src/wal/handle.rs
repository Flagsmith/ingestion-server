use std::sync::Arc;
use std::thread;

use tokio::sync::{mpsc, oneshot};

use super::{Chunk, Wal, WalError};
use crate::models::EventRecord;

/// What the WAL thread needs from the log: `Wal`, or a fake in tests.
pub trait Log: Send + 'static {
    fn append(&mut self, batches: &[Vec<EventRecord>]) -> Result<(), WalError>;
    fn read_oldest(&self, max_events: usize) -> Result<Chunk, WalError>;
    fn remove(&mut self, chunk: Chunk) -> Result<(), WalError>;
    fn reject(&mut self, chunk: Chunk) -> Result<(), WalError>;
}

impl Log for Wal {
    fn append(&mut self, batches: &[Vec<EventRecord>]) -> Result<(), WalError> {
        Wal::append(self, batches)
    }

    fn read_oldest(&self, max_events: usize) -> Result<Chunk, WalError> {
        Wal::read_oldest(self, max_events)
    }

    fn remove(&mut self, chunk: Chunk) -> Result<(), WalError> {
        Wal::remove(self, chunk)
    }

    fn reject(&mut self, chunk: Chunk) -> Result<(), WalError> {
        Wal::reject(self, chunk)
    }
}

/// Errors are shared, since one failed commit fails every append in it.
type Reply<T> = oneshot::Sender<Result<T, Arc<WalError>>>;

enum Request {
    Append(Vec<EventRecord>, Reply<()>),
    ReadOldest(usize, Reply<Chunk>),
    Remove(Chunk, Reply<()>),
    Reject(Chunk, Reply<()>),
}

/// Sends requests to the thread that owns the log. The thread stops once
/// every handle is dropped.
#[derive(Clone)]
pub struct WalHandle {
    requests: mpsc::UnboundedSender<Request>,
}

impl WalHandle {
    /// Runs the log on its own thread, since every call blocks on disk.
    pub fn spawn(log: impl Log) -> Self {
        let (requests, receiver) = mpsc::unbounded_channel();
        thread::Builder::new()
            .name("wal".to_string())
            .spawn(move || run(log, receiver))
            .expect("spawn the WAL thread");
        WalHandle { requests }
    }

    /// Returns once the records are durable.
    pub async fn append(&self, records: Vec<EventRecord>) -> Result<(), Arc<WalError>> {
        self.request(|reply| Request::Append(records, reply)).await
    }

    pub async fn read_oldest(&self, max_events: usize) -> Result<Chunk, Arc<WalError>> {
        self.request(|reply| Request::ReadOldest(max_events, reply))
            .await
    }

    pub async fn remove(&self, chunk: Chunk) -> Result<(), Arc<WalError>> {
        self.request(|reply| Request::Remove(chunk, reply)).await
    }

    pub async fn reject(&self, chunk: Chunk) -> Result<(), Arc<WalError>> {
        self.request(|reply| Request::Reject(chunk, reply)).await
    }

    async fn request<T>(
        &self,
        request: impl FnOnce(Reply<T>) -> Request,
    ) -> Result<T, Arc<WalError>> {
        let (reply, response) = oneshot::channel();
        let stopped = || Arc::new(WalError::Stopped);
        self.requests.send(request(reply)).map_err(|_| stopped())?;
        response.await.map_err(|_| stopped())?
    }
}

/// Takes every queued request at once. Reads and removes run as they come;
/// the appends then share one commit.
fn run(mut log: impl Log, mut requests: mpsc::UnboundedReceiver<Request>) {
    while let Some(first) = requests.blocking_recv() {
        let mut batches = Vec::new();
        let mut replies = Vec::new();
        let queued = std::iter::from_fn(|| requests.try_recv().ok());
        for request in std::iter::once(first).chain(queued) {
            match request {
                Request::Append(records, reply) => {
                    batches.push(records);
                    replies.push(reply);
                }
                Request::ReadOldest(max_events, reply) => {
                    let _ = reply.send(log.read_oldest(max_events).map_err(Arc::new));
                }
                Request::Remove(chunk, reply) => {
                    let _ = reply.send(log.remove(chunk).map_err(Arc::new));
                }
                Request::Reject(chunk, reply) => {
                    let _ = reply.send(log.reject(chunk).map_err(Arc::new));
                }
            }
        }
        if batches.is_empty() {
            continue;
        }
        let result = log.append(&batches).map_err(Arc::new);
        for reply in replies {
            let _ = reply.send(result.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use futures::future::join_all;

    use super::*;
    use crate::wal::tests::records;

    /// Records how many batches each commit held, after a short delay so
    /// that appends queue up behind it.
    #[derive(Default)]
    struct FakeLog {
        fail: bool,
        commits: Arc<Mutex<Vec<usize>>>,
    }

    impl Log for FakeLog {
        fn append(&mut self, batches: &[Vec<EventRecord>]) -> Result<(), WalError> {
            thread::sleep(Duration::from_millis(50));
            self.commits.lock().unwrap().push(batches.len());
            if self.fail {
                let error = std::io::Error::other("disk full");
                return Err(WalError::Io(error));
            }
            Ok(())
        }

        fn read_oldest(&self, _max_events: usize) -> Result<Chunk, WalError> {
            unimplemented!()
        }

        fn remove(&mut self, _chunk: Chunk) -> Result<(), WalError> {
            unimplemented!()
        }

        fn reject(&mut self, _chunk: Chunk) -> Result<(), WalError> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_concurrent_appends_share_commits() {
        let commits = Arc::new(Mutex::new(Vec::new()));
        let handle = WalHandle::spawn(FakeLog {
            commits: commits.clone(),
            ..FakeLog::default()
        });

        let appends = (0..10).map(|i| handle.append(records(&[i])));
        let results = join_all(appends).await;

        assert!(results.iter().all(Result::is_ok));
        let commits = commits.lock().unwrap();
        assert!(commits.len() < 10, "commits: {commits:?}");
        assert_eq!(commits.iter().sum::<usize>(), 10);
    }

    #[tokio::test]
    async fn test_a_failed_commit_fails_every_append_in_it() {
        let handle = WalHandle::spawn(FakeLog {
            fail: true,
            ..FakeLog::default()
        });

        let appends = (0..10).map(|i| handle.append(records(&[i])));
        let results = join_all(appends).await;

        assert!(results
            .iter()
            .all(|result| matches!(result, Err(error) if matches!(**error, WalError::Io(_)))));
    }

    #[tokio::test]
    async fn test_reads_and_removes_through_the_handle() {
        let dir = tempfile::tempdir().unwrap();
        let handle = WalHandle::spawn(Wal::open(dir.path()).unwrap());
        handle.append(records(&[1])).await.unwrap();
        handle.append(records(&[2])).await.unwrap();

        let chunk = handle.read_oldest(100).await.unwrap();
        assert_eq!(chunk.records, records(&[1, 2]));
        handle.remove(chunk).await.unwrap();
        assert!(handle.read_oldest(100).await.unwrap().records.is_empty());
    }
}
