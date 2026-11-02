use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};

use crate::models::EventRecord;

/// One row per appended batch. Keys are microseconds since the epoch, bumped
/// past the last key so they keep increasing even if the clock steps back.
const BATCHES: TableDefinition<u64, &[u8]> = TableDefinition::new("batches");

/// Durable log of accepted batches, stored in `wal.redb` under a directory.
pub struct Wal {
    db: Database,
    next: u64,
}

/// The oldest rows, read for one write. Pass it back to `remove` once written.
#[derive(Debug)]
pub struct Chunk {
    pub records: Vec<EventRecord>,
    /// Rows that no longer decode as `EventRecord`s, e.g. after a schema change.
    pub undecodable: Vec<Vec<u8>>,
    keys: Vec<u64>,
}

impl Wal {
    pub fn open(dir: &Path) -> Result<Self, WalError> {
        std::fs::create_dir_all(dir)?;
        let db = Database::create(dir.join("wal.redb")).map_err(redb::Error::from)?;

        let txn = db.begin_write().map_err(redb::Error::from)?;
        let last = {
            let table = txn.open_table(BATCHES).map_err(redb::Error::from)?;
            let last = table.last().map_err(redb::Error::from)?;
            last.map(|(key, _)| key.value())
        };
        txn.commit().map_err(redb::Error::from)?;

        Ok(Wal {
            db,
            next: last.map_or(0, |key| key + 1),
        })
    }

    /// Appends each batch as its own row, all in one durable commit.
    pub fn append(&mut self, batches: &[Vec<EventRecord>]) -> Result<(), WalError> {
        let start = self.next.max(now_micros());

        let mut txn = self.db.begin_write().map_err(redb::Error::from)?;
        txn.set_durability(Durability::Immediate)
            .map_err(redb::Error::from)?;
        {
            let mut table = txn.open_table(BATCHES).map_err(redb::Error::from)?;
            for (key, batch) in (start..).zip(batches) {
                let row = serde_json::to_vec(batch).expect("EventRecords always serialize");
                table
                    .insert(key, row.as_slice())
                    .map_err(redb::Error::from)?;
            }
        }
        txn.commit().map_err(redb::Error::from)?;
        self.next = start + batches.len() as u64;
        Ok(())
    }

    /// The oldest rows, up to `max_events` events, but at least one row if
    /// any exist.
    pub fn read_oldest(&self, max_events: usize) -> Result<Chunk, WalError> {
        let txn = self.db.begin_read().map_err(redb::Error::from)?;
        let table = txn.open_table(BATCHES).map_err(redb::Error::from)?;

        let mut chunk = Chunk {
            records: Vec::new(),
            undecodable: Vec::new(),
            keys: Vec::new(),
        };
        for row in table.iter().map_err(redb::Error::from)? {
            let (key, value) = row.map_err(redb::Error::from)?;
            match serde_json::from_slice::<Vec<EventRecord>>(value.value()) {
                Ok(records) => {
                    let full = chunk.records.len() + records.len() > max_events;
                    if full && !chunk.keys.is_empty() {
                        break;
                    }
                    chunk.records.extend(records);
                }
                Err(_) => chunk.undecodable.push(value.value().to_vec()),
            }
            chunk.keys.push(key.value());
        }
        Ok(chunk)
    }

    /// Deletes a chunk's rows in one durable commit.
    pub fn remove(&mut self, chunk: Chunk) -> Result<(), WalError> {
        let mut txn = self.db.begin_write().map_err(redb::Error::from)?;
        txn.set_durability(Durability::Immediate)
            .map_err(redb::Error::from)?;
        {
            let mut table = txn.open_table(BATCHES).map_err(redb::Error::from)?;
            for key in chunk.keys {
                table.remove(key).map_err(redb::Error::from)?;
            }
        }
        txn.commit().map_err(redb::Error::from)?;
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
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn insert_raw(wal: &Wal, key: u64, row: &[u8]) {
        let txn = wal.db.begin_write().unwrap();
        txn.open_table(BATCHES).unwrap().insert(key, row).unwrap();
        txn.commit().unwrap();
    }

    #[test]
    fn test_empty_wal_reads_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::open(dir.path()).unwrap();
        let chunk = wal.read_oldest(100).unwrap();
        assert!(chunk.records.is_empty());
        assert!(chunk.undecodable.is_empty());
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
    fn test_returns_undecodable_rows_without_blocking_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path()).unwrap();
        wal.append(&[records(&[1])]).unwrap();
        insert_raw(&wal, wal.next, b"not json");
        wal.next += 1;
        wal.append(&[records(&[2])]).unwrap();

        let chunk = wal.read_oldest(100).unwrap();
        assert_eq!(chunk.records, records(&[1, 2]));
        assert_eq!(chunk.undecodable, vec![b"not json".to_vec()]);

        wal.remove(chunk).unwrap();
        let chunk = wal.read_oldest(100).unwrap();
        assert!(chunk.records.is_empty());
        assert!(chunk.undecodable.is_empty());
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
        wal.next = u64::MAX - 10;
        wal.append(&[records(&[1]), records(&[2])]).unwrap();

        assert_eq!(
            wal.read_oldest(100).unwrap().keys,
            vec![u64::MAX - 10, u64::MAX - 9]
        );
    }
}
