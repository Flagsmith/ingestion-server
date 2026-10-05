use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use moka::future::Cache;
use moka::ops::compute::{CompResult, Op};
use moka::{Entry, Expiry};
use sqlx::PgPool;
use tracing::warn;

use crate::lookup::mask_key;

const CACHE_CAPACITY: u64 = 100_000;
const UNKNOWN_KEY_CACHE_TTL: Duration = Duration::from_secs(10);
const FAILED_REFRESH_RETRY_AFTER: Duration = Duration::from_secs(10);
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);

type EnvironmentKeyRow = (String, bool, Option<DateTime<Utc>>);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnvironmentKey {
    pub client_api_key: Arc<str>,
    pub uses_external_warehouse: bool,
    pub expires_at: Option<DateTime<Utc>>,
}

impl EnvironmentKey {
    pub fn is_valid(&self) -> bool {
        self.expires_at
            .is_none_or(|expires_at| expires_at > Utc::now())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LookupError {
    #[error("Postgres is unavailable")]
    Unavailable,
}

#[derive(Clone)]
enum LookupResult {
    Found(EnvironmentKey),
    NotFound,
}

impl LookupResult {
    fn into_valid_key(self) -> Option<EnvironmentKey> {
        match self {
            LookupResult::Found(environment_key) if environment_key.is_valid() => {
                Some(environment_key)
            }
            _ => None,
        }
    }
}

#[derive(Clone)]
struct CachedLookup {
    result: LookupResult,
    refresh_at: Instant,
}

impl CachedLookup {
    fn is_due_for_refresh(&self) -> bool {
        self.refresh_at <= Instant::now()
    }
}

#[derive(Clone)]
pub struct EnvironmentKeys {
    pool: PgPool,
    known_key_refresh_after: Duration,
    cache: Cache<String, CachedLookup>,
}

impl EnvironmentKeys {
    pub fn new(pool: PgPool, known_key_refresh_after: Duration) -> Self {
        let cache = Cache::builder()
            .max_capacity(CACHE_CAPACITY)
            .expire_after(EnvironmentKeysExpiry)
            .build();
        EnvironmentKeys {
            pool,
            known_key_refresh_after,
            cache,
        }
    }

    pub async fn lookup(&self, key: &str) -> Result<Option<EnvironmentKey>, LookupError> {
        let result = match self.cache.get(key).await {
            Some(lookup) => {
                if lookup.is_due_for_refresh() {
                    self.refresh_in_background(key);
                }
                lookup.result
            }
            None => self.fetch_and_remember(key).await?,
        };
        Ok(result.into_valid_key())
    }

    async fn fetch_and_remember(&self, key: &str) -> Result<LookupResult, LookupError> {
        let lookup = self
            .cache
            .try_get_with(key.to_owned(), async {
                let result = self.fetch(key).await?;
                Ok::<_, LookupError>(self.fresh_lookup(result))
            })
            .await
            .map_err(|_| LookupError::Unavailable)?;
        Ok(lookup.result)
    }

    fn refresh_in_background(&self, key: &str) {
        let environment_keys = self.clone();
        let key = key.to_owned();
        tokio::spawn(async move {
            if environment_keys.claim_refresh(&key).await {
                if let Ok(result) = environment_keys.fetch(&key).await {
                    environment_keys.remember(&key, result).await;
                }
            }
        });
    }

    async fn claim_refresh(&self, key: &str) -> bool {
        let computed_cache_entry = self
            .cache
            .entry(key.to_owned())
            .and_compute_with(|existing_cache_entry| async move {
                match existing_cache_entry.map(Entry::into_value) {
                    Some(lookup) if lookup.is_due_for_refresh() => Op::Put(CachedLookup {
                        refresh_at: Instant::now() + FAILED_REFRESH_RETRY_AFTER,
                        ..lookup
                    }),
                    _ => Op::Nop,
                }
            })
            .await;
        matches!(computed_cache_entry, CompResult::ReplacedWith(_))
    }

    async fn remember(&self, key: &str, result: LookupResult) {
        self.cache
            .insert(key.to_owned(), self.fresh_lookup(result))
            .await;
    }

    fn fresh_lookup(&self, result: LookupResult) -> CachedLookup {
        CachedLookup {
            result,
            refresh_at: Instant::now() + self.known_key_refresh_after,
        }
    }

    async fn fetch(&self, key: &str) -> Result<LookupResult, LookupError> {
        Ok(match self.select_environment_key(key).await? {
            Some((client_api_key, uses_external_warehouse, expires_at)) => {
                LookupResult::Found(EnvironmentKey {
                    client_api_key: Arc::from(client_api_key),
                    uses_external_warehouse,
                    expires_at,
                })
            }
            None => LookupResult::NotFound,
        })
    }

    async fn select_environment_key(
        &self,
        key: &str,
    ) -> Result<Option<EnvironmentKeyRow>, LookupError> {
        let query = sqlx::query_as(
            "SELECT client_api_key, uses_external_warehouse, expires_at \
             FROM experimentation_environment_keys WHERE sdk_key = $1",
        )
        .bind(key)
        .fetch_optional(&self.pool);
        tokio::time::timeout(LOOKUP_TIMEOUT, query)
            .await
            .map_err(|_| {
                warn!(key = %mask_key(key), "Environment key lookup timed out");
                LookupError::Unavailable
            })?
            .map_err(|error| {
                warn!(%error, key = %mask_key(key), "Environment key lookup failed");
                LookupError::Unavailable
            })
    }
}

struct EnvironmentKeysExpiry;

impl EnvironmentKeysExpiry {
    fn time_to_live(lookup: &CachedLookup) -> Option<Duration> {
        match lookup.result {
            LookupResult::Found(_) => None,
            LookupResult::NotFound => Some(UNKNOWN_KEY_CACHE_TTL),
        }
    }
}

impl Expiry<String, CachedLookup> for EnvironmentKeysExpiry {
    fn expire_after_create(
        &self,
        _key: &String,
        lookup: &CachedLookup,
        _created_at: Instant,
    ) -> Option<Duration> {
        Self::time_to_live(lookup)
    }

    fn expire_after_update(
        &self,
        _key: &String,
        lookup: &CachedLookup,
        _updated_at: Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        Self::time_to_live(lookup)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::*;
    use crate::test_helpers::{create_environment_keys, insert_environment_key};

    fn environment_key(expires_at: Option<DateTime<Utc>>) -> EnvironmentKey {
        EnvironmentKey {
            client_api_key: Arc::from("client-api-key"),
            uses_external_warehouse: false,
            expires_at,
        }
    }

    #[test]
    fn key_without_expiry_is_valid() {
        // Given
        let key = environment_key(None);

        // When
        let is_valid = key.is_valid();

        // Then
        assert!(is_valid);
    }

    #[test]
    fn key_expiring_later_is_valid() {
        // Given
        let key = environment_key(Some(Utc::now() + TimeDelta::hours(1)));

        // When
        let is_valid = key.is_valid();

        // Then
        assert!(is_valid);
    }

    #[test]
    fn key_past_its_expiry_is_invalid() {
        // Given
        let key = environment_key(Some(Utc::now() - TimeDelta::hours(1)));

        // When
        let is_valid = key.is_valid();

        // Then
        assert!(!is_valid);
    }

    #[sqlx::test]
    async fn lookup_finds_a_stored_key(pool: PgPool) {
        // Given
        create_environment_keys(&pool).await;
        insert_environment_key(&pool, "ser.server-key", true, None).await;
        let environment_keys = EnvironmentKeys::new(pool, Duration::from_secs(300));

        // When
        let found = environment_keys.lookup("ser.server-key").await.unwrap();

        // Then
        assert_eq!(
            found,
            Some(EnvironmentKey {
                client_api_key: Arc::from("client-api-key"),
                uses_external_warehouse: true,
                expires_at: None,
            })
        );
    }

    #[sqlx::test]
    async fn lookup_returns_none_for_an_unknown_key(pool: PgPool) {
        // Given
        create_environment_keys(&pool).await;
        let environment_keys = EnvironmentKeys::new(pool, Duration::from_secs(300));

        // When
        let found = environment_keys.lookup("unknown-key").await.unwrap();

        // Then
        assert_eq!(found, None);
    }

    #[sqlx::test]
    async fn lookup_returns_none_for_an_expired_key(pool: PgPool) {
        // Given
        create_environment_keys(&pool).await;
        insert_environment_key(
            &pool,
            "ser.server-key",
            true,
            Some(Utc::now() - TimeDelta::hours(1)),
        )
        .await;
        let environment_keys = EnvironmentKeys::new(pool, Duration::from_secs(300));

        // When
        let found = environment_keys.lookup("ser.server-key").await.unwrap();

        // Then
        assert_eq!(found, None);
    }

    #[sqlx::test]
    async fn lookup_is_unavailable_when_postgres_is_unreachable(pool: PgPool) {
        // Given
        create_environment_keys(&pool).await;
        pool.close().await;
        let environment_keys = EnvironmentKeys::new(pool, Duration::from_secs(300));

        // When
        let result = environment_keys.lookup("ser.server-key").await;

        // Then
        assert!(matches!(result, Err(LookupError::Unavailable)));
    }

    #[sqlx::test]
    async fn lookup_serves_a_cached_key_after_its_row_is_deleted(pool: PgPool) {
        // Given
        create_environment_keys(&pool).await;
        insert_environment_key(&pool, "ser.server-key", true, None).await;
        let environment_keys = EnvironmentKeys::new(pool.clone(), Duration::from_secs(300));
        environment_keys.lookup("ser.server-key").await.unwrap();
        sqlx::query("DELETE FROM experimentation_environment_keys")
            .execute(&pool)
            .await
            .unwrap();

        // When
        let found = environment_keys.lookup("ser.server-key").await.unwrap();

        // Then
        assert!(found.is_some());
    }

    #[sqlx::test]
    async fn lookup_refetches_a_key_once_it_is_due_for_refresh(pool: PgPool) {
        // Given
        create_environment_keys(&pool).await;
        insert_environment_key(&pool, "ser.server-key", true, None).await;
        let environment_keys = EnvironmentKeys::new(pool.clone(), Duration::from_millis(100));
        environment_keys.lookup("ser.server-key").await.unwrap();
        sqlx::query("DELETE FROM experimentation_environment_keys")
            .execute(&pool)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;

        // When
        let stale = environment_keys.lookup("ser.server-key").await.unwrap();

        // Then
        assert!(stale.is_some());
        tokio::time::timeout(Duration::from_secs(1), async {
            while environment_keys
                .lookup("ser.server-key")
                .await
                .unwrap()
                .is_some()
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the background refresh forgets the deleted key within a second");
    }

    #[sqlx::test]
    async fn lookup_serves_a_due_key_without_waiting_for_postgres(pool: PgPool) {
        // Given
        create_environment_keys(&pool).await;
        insert_environment_key(&pool, "ser.server-key", true, None).await;
        let environment_keys = EnvironmentKeys::new(pool.clone(), Duration::from_millis(100));
        environment_keys.lookup("ser.server-key").await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        let mut table_lock = pool.begin().await.unwrap();
        sqlx::query("LOCK TABLE experimentation_environment_keys IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *table_lock)
            .await
            .unwrap();

        // When
        let found = tokio::time::timeout(
            Duration::from_millis(100),
            environment_keys.lookup("ser.server-key"),
        )
        .await;

        // Then
        assert!(found
            .expect("lookup answered within 100ms")
            .unwrap()
            .is_some());
    }

    #[sqlx::test]
    async fn lookup_remembers_an_unknown_key(pool: PgPool) {
        // Given
        create_environment_keys(&pool).await;
        let environment_keys = EnvironmentKeys::new(pool.clone(), Duration::from_secs(300));
        environment_keys.lookup("ser.server-key").await.unwrap();
        insert_environment_key(&pool, "ser.server-key", true, None).await;

        // When
        let found = environment_keys.lookup("ser.server-key").await.unwrap();

        // Then
        assert_eq!(found, None);
    }

    #[sqlx::test]
    async fn lookup_serves_a_stale_key_while_postgres_is_unreachable(pool: PgPool) {
        // Given
        create_environment_keys(&pool).await;
        insert_environment_key(&pool, "ser.server-key", true, None).await;
        let environment_keys = EnvironmentKeys::new(pool.clone(), Duration::from_millis(100));
        environment_keys.lookup("ser.server-key").await.unwrap();
        pool.close().await;
        tokio::time::sleep(Duration::from_millis(150)).await;

        // When
        let found = environment_keys.lookup("ser.server-key").await.unwrap();

        // Then
        assert!(found.is_some());
    }

    #[sqlx::test]
    async fn lookup_is_unavailable_when_postgres_stops_answering(pool: PgPool) {
        // Given
        create_environment_keys(&pool).await;
        let mut table_lock = pool.begin().await.unwrap();
        sqlx::query("LOCK TABLE experimentation_environment_keys IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *table_lock)
            .await
            .unwrap();
        let environment_keys = EnvironmentKeys::new(pool, Duration::from_secs(300));

        // When
        let result = tokio::time::timeout(
            LOOKUP_TIMEOUT + Duration::from_secs(1),
            environment_keys.lookup("ser.server-key"),
        )
        .await;

        // Then
        assert!(matches!(
            result.expect("lookup gave up within its timeout"),
            Err(LookupError::Unavailable)
        ));
    }

    #[sqlx::test]
    async fn concurrent_lookups_of_a_new_key_share_one_failure(pool: PgPool) {
        // Given Postgres stops answering while five requests carry the same new key
        create_environment_keys(&pool).await;
        let mut table_lock = pool.begin().await.unwrap();
        sqlx::query("LOCK TABLE experimentation_environment_keys IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *table_lock)
            .await
            .unwrap();
        let environment_keys = EnvironmentKeys::new(pool, Duration::from_secs(300));

        // When
        let results = tokio::time::timeout(
            LOOKUP_TIMEOUT + Duration::from_secs(1),
            futures::future::join_all((0..5).map(|_| environment_keys.lookup("ser.server-key"))),
        )
        .await;

        // Then
        let results = results.expect("all five gave up within one timeout");
        assert!(results
            .iter()
            .all(|result| matches!(result, Err(LookupError::Unavailable))));
    }
}
