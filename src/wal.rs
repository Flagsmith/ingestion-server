use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};

use crate::models::EventRecord;

mod handle;

pub use handle::WalHandle;

/// One row per appended batch. Keys are microseconds since the epoch, bumped
/// past the last key so they keep increasing even if the clock steps back.
const BATCHES: TableDefinition<u64, &[u8]> = TableDefinition::new("batches");

/// Rows the store rejected or that no longer decode, kept for inspection.
const REJECTED: TableDefinition<u64, &[u8]> = TableDefinition::new("rejected");

/// Durable log of accepted batches, stored in `wal.redb` under a directory.
pub struct Wal {
    db: Database,
    next_key: u64,
    /// Bytes held in the batches table. The file never shrinks, so its size
    /// says nothing about the backlog.
    held_bytes: u64,
    max_bytes: Option<u64>,
}

/// The oldest rows, read for one write. Pass it back to `remove` once
/// written, or to `reject` if the store rejects it.
#[derive(Debug)]
pub struct Chunk {
    pub records: Vec<EventRecord>,
    /// Rows that no longer decode as `EventRecord`s, e.g. after a schema
    /// change. Either way they move to the rejected table.
    pub undecodable_rows: usize,
    keys: Vec<u64>,
    undecodable_keys: Vec<u64>,
}

impl Chunk {
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty() && self.undecodable_keys.is_empty()
    }
}

impl Wal {
    pub fn open(dir: &Path) -> Result<Self, WalError> {
        std::fs::create_dir_all(dir)?;
        let db = Database::create(dir.join("wal.redb")).map_err(redb::Error::from)?;

        let txn = db.begin_write().map_err(redb::Error::from)?;
        let (last, held_bytes) = {
            let table = txn.open_table(BATCHES).map_err(redb::Error::from)?;
            let last = table.last().map_err(redb::Error::from)?;
            let mut held_bytes = 0;
            for row in table.iter().map_err(redb::Error::from)? {
                let (_, value) = row.map_err(redb::Error::from)?;
                held_bytes += value.value().len() as u64;
            }
            (last.map(|(key, _)| key.value()), held_bytes)
        };
        txn.commit().map_err(redb::Error::from)?;

        Ok(Wal {
            db,
            next_key: last.map_or(0, |key| key + 1),
            held_bytes,
            max_bytes: None,
        })
    }

    /// Refuse appends that would take the batches table past `max_bytes`.
    pub fn with_max_bytes(self, max_bytes: u64) -> Self {
        Wal {
            max_bytes: Some(max_bytes),
            ..self
        }
    }

    /// Appends each batch as its own row, all in one durable commit, or none
    /// of them if they would take the WAL past its cap.
    pub fn append(&mut self, batches: &[Vec<EventRecord>]) -> Result<(), WalError> {
        let rows: Vec<Vec<u8>> = batches
            .iter()
            .map(|batch| serde_json::to_vec(batch).expect("EventRecords always serialize"))
            .collect();
        let bytes_after = self.held_bytes + rows.iter().map(|row| row.len() as u64).sum::<u64>();
        if let Some(max_bytes) = self.max_bytes {
            if bytes_after > max_bytes {
                return Err(WalError::Full { max_bytes });
            }
        }
        let start = self.next_key.max(now_micros());

        let mut txn = self.db.begin_write().map_err(redb::Error::from)?;
        txn.set_durability(Durability::Immediate)
            .map_err(redb::Error::from)?;
        {
            let mut table = txn.open_table(BATCHES).map_err(redb::Error::from)?;
            for (key, row) in (start..).zip(&rows) {
                table
                    .insert(key, row.as_slice())
                    .map_err(redb::Error::from)?;
            }
        }
        txn.commit().map_err(redb::Error::from)?;
        self.next_key = start + rows.len() as u64;
        self.held_bytes = bytes_after;
        Ok(())
    }

    /// The oldest rows, up to `max_events` events, but at least one row if
    /// any exist.
    pub fn read_oldest(&self, max_events: usize) -> Result<Chunk, WalError> {
        let txn = self.db.begin_read().map_err(redb::Error::from)?;
        let table = txn.open_table(BATCHES).map_err(redb::Error::from)?;

        let mut chunk = Chunk {
            records: Vec::new(),
            undecodable_rows: 0,
            keys: Vec::new(),
            undecodable_keys: Vec::new(),
        };
        for row in table.iter().map_err(redb::Error::from)? {
            let (key, value) = row.map_err(redb::Error::from)?;
            match serde_json::from_slice::<Vec<EventRecord>>(value.value()) {
                Ok(records) => {
                    let full = chunk.records.len() + records.len() > max_events;
                    if full && !chunk.is_empty() {
                        break;
                    }
                    chunk.records.extend(records);
                    chunk.keys.push(key.value());
                }
                Err(_) => {
                    chunk.undecodable_rows += 1;
                    chunk.undecodable_keys.push(key.value());
                }
            }
        }
        Ok(chunk)
    }

    /// Deletes a written chunk's rows, and moves its undecodable rows to the
    /// rejected table, in one durable commit.
    pub fn remove(&mut self, chunk: Chunk) -> Result<(), WalError> {
        self.remove_rows(&chunk.keys, &chunk.undecodable_keys)
    }

    /// Moves all of a chunk's rows to the rejected table in one durable commit.
    pub fn reject(&mut self, chunk: Chunk) -> Result<(), WalError> {
        self.remove_rows(&[], &[chunk.keys, chunk.undecodable_keys].concat())
    }

    /// Deletes the `delete` rows and moves the `reject` rows to the rejected
    /// table.
    fn remove_rows(&mut self, delete: &[u64], reject: &[u64]) -> Result<(), WalError> {
        let mut txn = self.db.begin_write().map_err(redb::Error::from)?;
        txn.set_durability(Durability::Immediate)
            .map_err(redb::Error::from)?;
        let mut freed_bytes = 0;
        {
            let mut batches = txn.open_table(BATCHES).map_err(redb::Error::from)?;
            let mut rejected = txn.open_table(REJECTED).map_err(redb::Error::from)?;
            for &key in delete {
                if let Some(row) = batches.remove(key).map_err(redb::Error::from)? {
                    freed_bytes += row.value().len() as u64;
                }
            }
            for &key in reject {
                if let Some(row) = batches.remove(key).map_err(redb::Error::from)? {
                    freed_bytes += row.value().len() as u64;
                    rejected
                        .insert(key, row.value())
                        .map_err(redb::Error::from)?;
                }
            }
        }
        txn.commit().map_err(redb::Error::from)?;
        self.held_bytes -= freed_bytes;
        Ok(())
    }
}

fn now_micros() -> u64 {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after 1970");
    since_epoch.as_micros() as u64
}

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("WAL storage error: {0}")]
    Storage(#[from] redb::Error),
    #[error("WAL IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("WAL thread stopped")]
    Stopped,
    #[error("WAL full: appending would exceed {max_bytes} bytes")]
    Full { max_bytes: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn records(timestamps: &[i64]) -> Vec<EventRecord> {
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

    fn insert_raw(wal: &mut Wal, key: u64, row: &[u8]) {
        let txn = wal.db.begin_write().unwrap();
        txn.open_table(BATCHES).unwrap().insert(key, row).unwrap();
        txn.commit().unwrap();
        wal.held_bytes += row.len() as u64;
    }

    fn rejected_rows(wal: &Wal) -> Vec<Vec<u8>> {
        let txn = wal.db.begin_read().unwrap();
        let Ok(table) = txn.open_table(REJECTED) else {
            return Vec::new();
        };
        table
            .iter()
            .unwrap()
            .map(|row| row.unwrap().1.value().to_vec())
            .collect()
    }

    #[test]
    fn test_empty_wal_reads_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::open(dir.path()).unwrap();
        assert!(wal.read_oldest(100).unwrap().is_empty());
    }

    #[test]
    fn test_reads_records_in_append_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path()).unwrap();
        wal.append(&[records(&[1, 2]), records(&[3])]).unwrap();
        wal.append(&[records(&[4])]).unwrap();

        assert_eq!(
            wal.read_oldest(100).unwrap().records,
            records(&[1, 2, 3, 4])
        );
    }

    #[test]
    fn test_read_stops_at_max_events_but_returns_at_least_one_row() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path()).unwrap();
        wal.append(&[records(&[1, 2, 3]), records(&[4]), records(&[5])])
            .unwrap();

        assert_eq!(wal.read_oldest(2).unwrap().records, records(&[1, 2, 3]));
        assert_eq!(wal.read_oldest(4).unwrap().records, records(&[1, 2, 3, 4]));
    }

    #[test]
    fn test_round_trips_every_field() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path()).unwrap();
        let record = EventRecord {
            feature_name: Some("checkout_button".to_string()),
            identifier: Some("user_1".to_string()),
            value: serde_json::json!({"variant": "control"}),
            traits: Some(serde_json::json!({"plan": "pro", "age": 42})),
            metadata: Some(serde_json::json!({"source": "web"})),
            ..records(&[1]).remove(0)
        };
        wal.append(&[vec![record.clone()]]).unwrap();

        assert_eq!(wal.read_oldest(100).unwrap().records, vec![record]);
    }

    #[test]
    fn test_remove_deletes_only_the_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path()).unwrap();
        wal.append(&[records(&[1]), records(&[2])]).unwrap();
        let chunk = wal.read_oldest(1).unwrap();
        wal.append(&[records(&[3])]).unwrap();
        wal.remove(chunk).unwrap();

        assert_eq!(wal.read_oldest(100).unwrap().records, records(&[2, 3]));
    }

    #[test]
    fn test_skips_undecodable_rows_and_remove_rejects_them() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path()).unwrap();
        wal.append(&[records(&[1])]).unwrap();
        let key = wal.next_key;
        insert_raw(&mut wal, key, b"not json");
        wal.next_key += 1;
        wal.append(&[records(&[2])]).unwrap();

        let chunk = wal.read_oldest(100).unwrap();
        assert_eq!(chunk.records, records(&[1, 2]));
        assert_eq!(chunk.undecodable_rows, 1);

        wal.remove(chunk).unwrap();
        assert!(wal.read_oldest(100).unwrap().is_empty());
        assert_eq!(rejected_rows(&wal), vec![b"not json".to_vec()]);
    }

    #[test]
    fn test_reject_moves_the_chunk_to_the_rejected_table() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path()).unwrap();
        wal.append(&[records(&[1]), records(&[2])]).unwrap();
        let chunk = wal.read_oldest(1).unwrap();
        wal.reject(chunk).unwrap();

        assert_eq!(wal.read_oldest(100).unwrap().records, records(&[2]));
        let rejected = serde_json::to_vec(&records(&[1])).unwrap();
        assert_eq!(rejected_rows(&wal), vec![rejected]);
    }

    #[test]
    fn test_reopen_keeps_rows() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path()).unwrap();
        wal.append(&[records(&[1])]).unwrap();
        drop(wal);

        let wal = Wal::open(dir.path()).unwrap();
        assert_eq!(wal.read_oldest(100).unwrap().records, records(&[1]));
    }

    #[test]
    fn test_keys_keep_increasing_after_a_drained_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path()).unwrap();
        wal.append(&[records(&[1])]).unwrap();
        let chunk = wal.read_oldest(100).unwrap();
        let old_key = chunk.keys[0];
        wal.remove(chunk).unwrap();
        drop(wal);

        let mut wal = Wal::open(dir.path()).unwrap();
        wal.append(&[records(&[2])]).unwrap();
        assert!(wal.read_oldest(100).unwrap().keys[0] > old_key);
    }

    #[test]
    fn test_keys_keep_increasing_when_the_clock_steps_back() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path()).unwrap();
        wal.next_key = u64::MAX - 10;
        wal.append(&[records(&[1]), records(&[2])]).unwrap();

        assert_eq!(
            wal.read_oldest(100).unwrap().keys,
            vec![u64::MAX - 10, u64::MAX - 9]
        );
    }

    fn row_bytes(batch: &[EventRecord]) -> u64 {
        serde_json::to_vec(batch).unwrap().len() as u64
    }

    #[test]
    fn test_refuses_appends_past_the_cap_until_rows_are_removed() {
        let dir = tempfile::tempdir().unwrap();
        let row = row_bytes(&records(&[1]));
        let mut wal = Wal::open(dir.path()).unwrap().with_max_bytes(2 * row);
        wal.append(&[records(&[1]), records(&[2])]).unwrap();

        assert!(matches!(
            wal.append(&[records(&[3])]),
            Err(WalError::Full { .. })
        ));
        assert_eq!(wal.read_oldest(100).unwrap().records, records(&[1, 2]));

        let chunk = wal.read_oldest(1).unwrap();
        wal.remove(chunk).unwrap();
        let chunk = wal.read_oldest(1).unwrap();
        wal.reject(chunk).unwrap();
        wal.append(&[records(&[3]), records(&[4])]).unwrap();
    }

    #[test]
    fn test_reopen_counts_the_bytes_held() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path()).unwrap();
        wal.append(&[records(&[1]), records(&[2])]).unwrap();
        drop(wal);

        let wal = Wal::open(dir.path()).unwrap();
        assert_eq!(wal.held_bytes, 2 * row_bytes(&records(&[1])));
    }
}
