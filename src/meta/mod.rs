use chrono::{DateTime, Utc};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use sqlx::{AnyPool, Error, Row, Transaction};
use std::convert::TryFrom;
use std::io;
use std::time::Duration;
use tokio::time;
use uuid::Uuid;

use crate::config::DatabaseDriver;
use crate::db::rewrite_placeholders;
use crate::error::ApiError;
use crate::meta;

const FINALIZE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const MAX_SAFE_CACHE_NUMERIC_ID: i64 = 9_007_199_254_740_991;

/// Captures a cache entry row loaded from the metadata database.
///
/// Cache entries record the authoritative state for each uploaded artifact,
/// including ownership, scope, storage identifiers, and access timestamps. The
/// struct mirrors the schema stored in `cache_entries` and enforces invariants
/// such as using [`uuid::Uuid`] identifiers and monotonically increasing access
/// timestamps managed by the database layer.
///
/// # Examples
/// ```
/// use chrono::DateTime;
/// use chrono::Utc;
/// use gha_cache_server::meta::CacheEntry;
/// use uuid::Uuid;
///
/// let timestamp = DateTime::<Utc>::from_timestamp(0, 0).expect("valid timestamp");
/// let entry = CacheEntry {
///     id: Uuid::nil(),
///     org: "octo-org".into(),
///     repo: "gha-cache".into(),
///     key: "ubuntu-latest".into(),
///     version: "v1".into(),
///     scope: "actions".into(),
///     size_bytes: 1024,
///     checksum: Some("abc123".into()),
///     storage_key: "cache/abc123".into(),
///     created_at: timestamp,
///     last_access_at: timestamp,
///     ttl_seconds: 3600,
/// };
/// assert_eq!(entry.repo, "gha-cache");
/// assert_eq!(entry.ttl_seconds, 3600);
/// ```
#[derive(Debug, Serialize, Deserialize)]
pub struct CacheEntry {
    pub id: Uuid,
    pub org: String,
    pub repo: String,
    pub key: String,
    pub version: String,
    pub scope: String,
    pub size_bytes: i64,
    pub checksum: Option<String>,
    pub storage_key: String,
    pub created_at: DateTime<Utc>,
    pub last_access_at: DateTime<Utc>,
    pub ttl_seconds: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UploadRow {
    pub id: Uuid,
    pub entry_id: Option<Uuid>,
    pub upload_id: String,
    pub state: String,
    pub active_part_count: i64,
    pub pending_finalize: bool,
}

#[derive(Clone, Debug)]
pub struct UploadStatus {
    pub state: String,
    pub active_part_count: i64,
    pub pending_finalize: bool,
}

#[derive(Clone, Debug)]
pub struct UploadPartRecord {
    pub part_index: i32,
    pub part_number: i32,
    pub offset: i64,
    pub size: i64,
    pub etag: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CacheGeneration {
    pub previous: i64,
    pub current: i64,
}

fn parse_uuid(value: String) -> sqlx::Result<Uuid> {
    Uuid::parse_str(&value).map_err(|err| sqlx::Error::Decode(Box::new(err)))
}

fn parse_uuid_opt(value: Option<String>) -> sqlx::Result<Option<Uuid>> {
    value.map(parse_uuid).transpose()
}

fn timestamp_to_datetime(ts: i64) -> sqlx::Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(ts, 0).ok_or_else(|| {
        sqlx::Error::Decode(Box::new(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid timestamp: {ts}"),
        )))
    })
}

fn generate_cache_numeric_id() -> i64 {
    let mut rng = rand::rng();
    rng.random_range(1..=MAX_SAFE_CACHE_NUMERIC_ID)
}

async fn insert_cache_numeric_id(
    tx: &mut Transaction<'_, sqlx::Any>,
    driver: DatabaseDriver,
    entry_id: Uuid,
) -> Result<i64, sqlx::Error> {
    let entry_str = entry_id.to_string();
    let insert_sql = rewrite_placeholders(
        "INSERT INTO cache_entry_ids (entry_id, numeric_id) VALUES (?, ?)",
        driver,
    );
    let fetch_sql = rewrite_placeholders(
        "SELECT numeric_id FROM cache_entry_ids WHERE entry_id = ? LIMIT 1",
        driver,
    );

    loop {
        let candidate = generate_cache_numeric_id();
        let result = sqlx::query(&insert_sql)
            .bind(&entry_str)
            .bind(candidate)
            .execute(tx.as_mut())
            .await;

        match result {
            Ok(_) => return Ok(candidate),
            Err(sqlx::Error::Database(db_err)) if db_err.is_unique_violation() => {
                if let Some(existing) = sqlx::query_scalar::<_, i64>(&fetch_sql)
                    .bind(&entry_str)
                    .fetch_optional(tx.as_mut())
                    .await?
                {
                    return Ok(existing);
                }
                continue;
            }
            Err(err) => return Err(err),
        }
    }
}

fn map_cache_entry(row: sqlx::any::AnyRow) -> Result<CacheEntry, sqlx::Error> {
    let id = parse_uuid(row.try_get::<String, _>("id")?)?;
    let created_at = timestamp_to_datetime(row.try_get::<i64, _>("created_at")?)?;
    let last_access_at = timestamp_to_datetime(row.try_get::<i64, _>("last_access_at")?)?;
    Ok(CacheEntry {
        id,
        org: row.try_get("org")?,
        repo: row.try_get("repo")?,
        key: row.try_get("cache_key")?,
        version: row.try_get("cache_version")?,
        scope: row.try_get("scope")?,
        size_bytes: row.try_get("size_bytes")?,
        checksum: row.try_get("checksum")?,
        storage_key: row.try_get("storage_key")?,
        created_at,
        last_access_at,
        ttl_seconds: row.try_get("ttl_seconds")?,
    })
}

fn map_upload_row(row: sqlx::any::AnyRow) -> Result<UploadRow, sqlx::Error> {
    let id = parse_uuid(row.try_get::<String, _>("id")?)?;
    let entry_id = parse_uuid_opt(row.try_get("entry_id")?)?;

    Ok(UploadRow {
        id,
        entry_id,
        upload_id: row.try_get("upload_id")?,
        state: row.try_get("state")?,
        active_part_count: row.try_get("active_part_count")?,
        pending_finalize: try_get_bool(&row, "pending_finalize")?,
    })
}

async fn fetch_upload(
    pool: &AnyPool,
    driver: DatabaseDriver,
    upload_id: &str,
) -> Result<UploadRow, sqlx::Error> {
    let query = rewrite_placeholders(
        "SELECT id, entry_id, upload_id, state, active_part_count, pending_finalize FROM cache_uploads WHERE upload_id = ?",
        driver,
    );
    let row = sqlx::query(&query).bind(upload_id).fetch_one(pool).await?;
    map_upload_row(row)
}

pub async fn get_upload_status(
    pool: &AnyPool,
    driver: DatabaseDriver,
    upload_id: &str,
) -> Result<UploadStatus, sqlx::Error> {
    let query = rewrite_placeholders(
        "SELECT state, active_part_count, pending_finalize FROM cache_uploads WHERE upload_id = ?",
        driver,
    );
    let row = sqlx::query(&query).bind(upload_id).fetch_one(pool).await?;
    Ok(UploadStatus {
        state: row.try_get("state")?,
        active_part_count: row.try_get("active_part_count")?,
        pending_finalize: try_get_bool(&row, "pending_finalize")?,
    })
}

pub async fn wait_for_no_active_parts(
    pool: &AnyPool,
    driver: DatabaseDriver,
    upload_id: &str,
) -> Result<(), sqlx::Error> {
    loop {
        let status = get_upload_status(pool, driver, upload_id).await?;
        if status.active_part_count == 0 {
            break;
        }
        time::sleep(FINALIZE_POLL_INTERVAL).await;
    }
    Ok(())
}

async fn increment_active_part_count(
    pool: &AnyPool,
    driver: DatabaseDriver,
    upload_id: &str,
) -> Result<(), sqlx::Error> {
    let now = Utc::now().timestamp();
    let query = rewrite_placeholders(
        "UPDATE cache_uploads SET active_part_count = active_part_count + 1, updated_at = ? WHERE upload_id = ?",
        driver,
    );
    let result = sqlx::query(&query)
        .bind(now)
        .bind(upload_id)
        .execute(pool)
        .await?;
    if result.rows_affected() == 0 {
        return Err(sqlx::Error::RowNotFound);
    }
    Ok(())
}

async fn decrement_active_part_count(
    pool: &AnyPool,
    driver: DatabaseDriver,
    upload_id: &str,
) -> Result<i64, sqlx::Error> {
    let mut tx: Transaction<'_, sqlx::Any> = pool.begin().await?;
    let now = Utc::now().timestamp();

    let update_query = rewrite_placeholders(
        "UPDATE cache_uploads SET active_part_count = CASE WHEN active_part_count > 0 THEN active_part_count - 1 ELSE 0 END, updated_at = ? WHERE upload_id = ?",
        driver,
    );
    let result = sqlx::query(&update_query)
        .bind(now)
        .bind(upload_id)
        .execute(&mut *tx)
        .await?;
    if result.rows_affected() == 0 {
        tx.rollback().await?;
        return Err(sqlx::Error::RowNotFound);
    }

    let select_query = rewrite_placeholders(
        "SELECT active_part_count FROM cache_uploads WHERE upload_id = ?",
        driver,
    );
    let row = sqlx::query(&select_query)
        .bind(upload_id)
        .fetch_one(&mut *tx)
        .await?;
    let new_value: i64 = row.try_get("active_part_count")?;

    tx.commit().await?;
    Ok(new_value)
}

pub async fn begin_part_upload(
    pool: &AnyPool,
    driver: DatabaseDriver,
    upload_id: &str,
) -> Result<(), sqlx::Error> {
    increment_active_part_count(pool, driver, upload_id).await
}

pub async fn finish_part_upload(
    pool: &AnyPool,
    driver: DatabaseDriver,
    upload_id: &str,
) -> Result<i64, sqlx::Error> {
    decrement_active_part_count(pool, driver, upload_id).await
}

pub async fn set_pending_finalize(
    pool: &AnyPool,
    driver: DatabaseDriver,
    upload_id: &str,
    pending: bool,
) -> Result<(), sqlx::Error> {
    let now = Utc::now().timestamp();
    let query = rewrite_placeholders(
        "UPDATE cache_uploads SET pending_finalize = ?, updated_at = ? WHERE upload_id = ?",
        driver,
    );
    let result = sqlx::query(&query)
        .bind(pending)
        .bind(now)
        .bind(upload_id)
        .execute(pool)
        .await?;
    if result.rows_affected() == 0 {
        return Err(sqlx::Error::RowNotFound);
    }
    Ok(())
}

fn try_get_bool(row: &sqlx::any::AnyRow, column: &str) -> Result<bool, Error> {
    match row.try_get::<bool, _>(column) {
        Ok(value) => Ok(value),
        Err(Error::ColumnDecode { .. }) => {
            let numeric: i64 = row.try_get(column)?;
            Ok(numeric != 0)
        }
        Err(other) => Err(other),
    }
}

async fn fetch_entry(
    pool: &AnyPool,
    driver: DatabaseDriver,
    id: Uuid,
) -> Result<CacheEntry, sqlx::Error> {
    let query = rewrite_placeholders(
        "SELECT id, org, repo, cache_key, cache_version, scope, size_bytes, checksum, storage_key, created_at, last_access_at, ttl_seconds FROM cache_entries WHERE id = ?",
        driver,
    );
    let row = sqlx::query(&query)
        .bind(id.to_string())
        .fetch_one(pool)
        .await?;
    map_cache_entry(row)
}

pub async fn touch_entry(
    pool: &AnyPool,
    driver: DatabaseDriver,
    id: Uuid,
) -> Result<(), sqlx::Error> {
    let now = Utc::now().timestamp();
    let query = rewrite_placeholders(
        "UPDATE cache_entries SET last_access_at = ? WHERE id = ?",
        driver,
    );
    sqlx::query(&query)
        .bind(now)
        .bind(id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_entry(
    pool: &AnyPool,
    driver: DatabaseDriver,
    id: Uuid,
) -> Result<(), sqlx::Error> {
    let query = rewrite_placeholders("DELETE FROM cache_entries WHERE id = ?", driver);
    sqlx::query(&query)
        .bind(id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn expired_entries(
    pool: &AnyPool,
    driver: DatabaseDriver,
    now: DateTime<Utc>,
    max_entry_age: Option<Duration>,
) -> Result<Vec<CacheEntry>, sqlx::Error> {
    let ts = now.timestamp();

    let rows = if let Some(limit) = max_entry_age {
        let secs = i64::try_from(limit.as_secs()).unwrap_or(i64::MAX);
        let query = rewrite_placeholders(
            "SELECT id, org, repo, cache_key, cache_version, scope, size_bytes, checksum, storage_key, created_at, last_access_at, ttl_seconds \
FROM cache_entries WHERE last_access_at + CASE WHEN ttl_seconds > ? THEN ? ELSE ttl_seconds END < ? ORDER BY last_access_at ASC",
            driver,
        );
        sqlx::query(&query)
            .bind(secs)
            .bind(secs)
            .bind(ts)
            .fetch_all(pool)
            .await?
    } else {
        let query = rewrite_placeholders(
            "SELECT id, org, repo, cache_key, cache_version, scope, size_bytes, checksum, storage_key, created_at, last_access_at, ttl_seconds \
FROM cache_entries WHERE last_access_at + ttl_seconds < ? ORDER BY last_access_at ASC",
            driver,
        );
        sqlx::query(&query).bind(ts).fetch_all(pool).await?
    };

    rows.into_iter().map(map_cache_entry).collect()
}

pub async fn total_occupancy(pool: &AnyPool, driver: DatabaseDriver) -> Result<i64, sqlx::Error> {
    let query = rewrite_placeholders(
        "SELECT COALESCE(SUM(size_bytes), 0) FROM cache_entries",
        driver,
    );
    let total = sqlx::query_scalar::<_, i64>(&query).fetch_one(pool).await?;
    Ok(total)
}

pub async fn list_entries_ordered(
    pool: &AnyPool,
    driver: DatabaseDriver,
    limit: Option<i64>,
) -> Result<Vec<CacheEntry>, sqlx::Error> {
    if let Some(limit) = limit {
        let query = rewrite_placeholders(
            "SELECT id, org, repo, cache_key, cache_version, scope, size_bytes, checksum, storage_key, created_at, last_access_at, ttl_seconds FROM cache_entries ORDER BY last_access_at ASC LIMIT ?",
            driver,
        );
        let rows = sqlx::query(&query).bind(limit).fetch_all(pool).await?;

        rows.into_iter().map(map_cache_entry).collect()
    } else {
        let query = rewrite_placeholders(
            "SELECT id, org, repo, cache_key, cache_version, scope, size_bytes, checksum, storage_key, created_at, last_access_at, ttl_seconds FROM cache_entries ORDER BY last_access_at ASC",
            driver,
        );
        let rows = sqlx::query(&query).fetch_all(pool).await?;

        rows.into_iter().map(map_cache_entry).collect()
    }
}

pub async fn current_generation(
    pool: &AnyPool,
    driver: DatabaseDriver,
) -> Result<i64, sqlx::Error> {
    let query = rewrite_placeholders(
        "SELECT current_generation FROM cache_state WHERE singleton = ? LIMIT 1",
        driver,
    );
    sqlx::query_scalar::<_, i64>(&query)
        .bind(1_i32)
        .fetch_one(pool)
        .await
}

pub async fn rotate_generation_and_clear_entries(
    pool: &AnyPool,
    driver: DatabaseDriver,
) -> Result<CacheGeneration, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let select_query = rewrite_placeholders(
        "SELECT current_generation FROM cache_state WHERE singleton = ? LIMIT 1",
        driver,
    );
    let previous = sqlx::query_scalar::<_, i64>(&select_query)
        .bind(1_i32)
        .fetch_one(&mut *tx)
        .await?;
    let current = previous + 1;

    let update_query = rewrite_placeholders(
        "UPDATE cache_state SET current_generation = ? WHERE singleton = ?",
        driver,
    );
    sqlx::query(&update_query)
        .bind(current)
        .bind(1_i32)
        .execute(&mut *tx)
        .await?;

    let delete_uploads_query = rewrite_placeholders("DELETE FROM cache_uploads", driver);
    sqlx::query(&delete_uploads_query).execute(&mut *tx).await?;

    let delete_entries_query = rewrite_placeholders("DELETE FROM cache_entries", driver);
    sqlx::query(&delete_entries_query).execute(&mut *tx).await?;

    tx.commit().await?;

    Ok(CacheGeneration { previous, current })
}

#[expect(
    clippy::too_many_arguments,
    reason = "Inserts need all cache entry fields"
)]
pub async fn create_entry(
    pool: &AnyPool,
    driver: DatabaseDriver,
    org: &str,
    repo: &str,
    key: &str,
    version: &str,
    scope: &str,
    storage_key: &str,
) -> Result<CacheEntry, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let id = Uuid::new_v4();
    let query = rewrite_placeholders(
        "INSERT INTO cache_entries (id, org, repo, cache_key, cache_version, scope, storage_key) VALUES (?, ?, ?, ?, ?, ?, ?)",
        driver,
    );
    sqlx::query(&query)
        .bind(id.to_string())
        .bind(org)
        .bind(repo)
        .bind(key)
        .bind(version)
        .bind(scope)
        .bind(storage_key)
        .execute(&mut *tx)
        .await?;

    insert_cache_numeric_id(&mut tx, driver, id).await?;

    tx.commit().await?;

    fetch_entry(pool, driver, id).await
}

pub async fn get_cache_numeric_id(
    pool: &AnyPool,
    driver: DatabaseDriver,
    entry_id: Uuid,
) -> Result<Option<i64>, sqlx::Error> {
    let query = rewrite_placeholders(
        "SELECT numeric_id FROM cache_entry_ids WHERE entry_id = ? LIMIT 1",
        driver,
    );
    sqlx::query_scalar::<_, i64>(&query)
        .bind(entry_id.to_string())
        .fetch_optional(pool)
        .await
}

pub async fn find_entry_id_by_numeric(
    pool: &AnyPool,
    driver: DatabaseDriver,
    numeric_id: i64,
) -> Result<Option<Uuid>, sqlx::Error> {
    let query = rewrite_placeholders(
        "SELECT entry_id FROM cache_entry_ids WHERE numeric_id = ? LIMIT 1",
        driver,
    );
    let maybe = sqlx::query(&query)
        .bind(numeric_id)
        .fetch_optional(pool)
        .await?;

    if let Some(row) = maybe {
        let entry_id: String = row.try_get("entry_id")?;
        Ok(Some(parse_uuid(entry_id)?))
    } else {
        Ok(None)
    }
}

pub async fn find_entry_by_key_version(
    pool: &AnyPool,
    driver: DatabaseDriver,
    key: &str,
    version: &str,
) -> Result<Option<CacheEntry>, sqlx::Error> {
    let query = rewrite_placeholders(
        "SELECT id, org, repo, cache_key, cache_version, scope, size_bytes, checksum, storage_key, created_at, last_access_at, ttl_seconds FROM cache_entries WHERE cache_key = ? AND cache_version = ? ORDER BY created_at DESC LIMIT 1",
        driver,
    );
    let maybe_row = sqlx::query(&query)
        .bind(key)
        .bind(version)
        .fetch_optional(pool)
        .await?;

    if let Some(row) = maybe_row {
        Ok(Some(map_cache_entry(row)?))
    } else {
        Ok(None)
    }
}

pub async fn upsert_upload(
    pool: &AnyPool,
    driver: DatabaseDriver,
    entry_id: Uuid,
    upload_id: &str,
    state: &str,
) -> Result<UploadRow, sqlx::Error> {
    let id = Uuid::new_v4();
    let entry = entry_id.to_string();

    let insert_query = rewrite_placeholders(
        "INSERT INTO cache_uploads (id, entry_id, upload_id, state) VALUES (?, ?, ?, ?)",
        driver,
    );
    let insert = sqlx::query(&insert_query)
        .bind(id.to_string())
        .bind(entry.clone())
        .bind(upload_id)
        .bind(state)
        .execute(pool)
        .await;

    if let Err(err) = insert {
        if let sqlx::Error::Database(db_err) = &err {
            if db_err.is_unique_violation() {
                let now = Utc::now().timestamp();
                let update_query = rewrite_placeholders(
                    "UPDATE cache_uploads SET entry_id = ?, state = ?, updated_at = ? WHERE upload_id = ?",
                    driver,
                );
                sqlx::query(&update_query)
                    .bind(entry)
                    .bind(state)
                    .bind(now)
                    .bind(upload_id)
                    .execute(pool)
                    .await?;
            } else {
                return Err(err);
            }
        } else {
            return Err(err);
        }
    }

    fetch_upload(pool, driver, upload_id).await
}

pub async fn reserve_part(
    pool: &AnyPool,
    driver: DatabaseDriver,
    upload_id: &str,
    part_index: i32,
    offset: Option<i64>,
    size: i64,
) -> Result<(), sqlx::Error> {
    let now = Utc::now().timestamp();
    let mut tx = pool.begin().await?;

    let insert_query = rewrite_placeholders(
        "INSERT INTO cache_upload_parts (upload_id, part_index, part_number, part_offset, size, state, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        driver,
    );
    let part_number = i64::from(part_index) + 1;
    let insert = sqlx::query(&insert_query)
        .bind(upload_id)
        .bind(part_index)
        .bind(part_number)
        .bind(offset)
        .bind(size)
        .bind("pending")
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await;

    match insert {
        Ok(_) => {
            tx.commit().await?;
            Ok(())
        }
        Err(err) => {
            if let sqlx::Error::Database(db_err) = &err {
                if db_err.is_unique_violation() {
                    let update_query = rewrite_placeholders(
                        "UPDATE cache_upload_parts SET part_offset = ?, size = ?, state = ?, etag = NULL, updated_at = ? WHERE upload_id = ? AND part_index = ?",
                        driver,
                    );
                    sqlx::query(&update_query)
                        .bind(offset)
                        .bind(size)
                        .bind("pending")
                        .bind(now)
                        .bind(upload_id)
                        .bind(part_index)
                        .execute(&mut *tx)
                        .await?;
                    tx.commit().await?;
                    Ok(())
                } else {
                    tx.rollback().await.ok();
                    Err(err)
                }
            } else {
                tx.rollback().await.ok();
                Err(err)
            }
        }
    }
}

pub async fn complete_part(
    pool: &AnyPool,
    driver: DatabaseDriver,
    upload_id: &str,
    part_index: i32,
    provided_offset: Option<i64>,
    etag: &str,
) -> Result<(), sqlx::Error> {
    let mut tx: Transaction<'_, sqlx::Any> = pool.begin().await?;
    let fetch_query = rewrite_placeholders(
        "SELECT size, part_offset FROM cache_upload_parts WHERE upload_id = ? AND part_index = ?",
        driver,
    );
    let maybe_row = sqlx::query(&fetch_query)
        .bind(upload_id)
        .bind(part_index)
        .fetch_optional(&mut *tx)
        .await?;

    let row = if let Some(row) = maybe_row {
        row
    } else {
        tx.rollback().await.ok();
        return Err(sqlx::Error::RowNotFound);
    };

    let size: i64 = row.try_get("size")?;
    let existing_offset: Option<i64> = row.try_get("part_offset")?;

    let mut expected_offset = provided_offset;
    if expected_offset.is_none() {
        let sum_sql = if driver == DatabaseDriver::Postgres {
            "SELECT COALESCE(SUM(size), 0)::bigint AS total FROM cache_upload_parts WHERE upload_id = ? AND part_index < ?"
        } else {
            "SELECT COALESCE(SUM(size), 0) AS total FROM cache_upload_parts WHERE upload_id = ? AND part_index < ?"
        };
        let sum_query = rewrite_placeholders(sum_sql, driver);
        let total: i64 = sqlx::query(&sum_query)
            .bind(upload_id)
            .bind(part_index)
            .fetch_one(&mut *tx)
            .await?
            .try_get("total")?;
        expected_offset = Some(total);
    }

    if let (Some(current), Some(existing)) = (expected_offset, existing_offset)
        && existing != current
    {
        tx.rollback().await.ok();
        return Err(sqlx::Error::Protocol("part offset mismatch".into()));
    }

    let offset_to_store = if let Some(offset) = expected_offset.or(existing_offset) {
        offset
    } else {
        tx.rollback().await.ok();
        return Err(sqlx::Error::Protocol(
            "missing offset for upload part".into(),
        ));
    };

    let now = Utc::now().timestamp();
    let update_query = rewrite_placeholders(
        "UPDATE cache_upload_parts SET part_offset = ?, etag = ?, state = ?, updated_at = ?, size = ? WHERE upload_id = ? AND part_index = ?",
        driver,
    );
    sqlx::query(&update_query)
        .bind(offset_to_store)
        .bind(etag)
        .bind("completed")
        .bind(now)
        .bind(size)
        .bind(upload_id)
        .bind(part_index)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(())
}

pub async fn get_completed_parts(
    pool: &AnyPool,
    driver: DatabaseDriver,
    upload_id: &str,
) -> Result<Vec<UploadPartRecord>, sqlx::Error> {
    let query = rewrite_placeholders(
        "SELECT part_index, part_number, part_offset, size, etag FROM cache_upload_parts WHERE upload_id = ? AND state = ? ORDER BY part_index ASC",
        driver,
    );
    let rows = sqlx::query(&query)
        .bind(upload_id)
        .bind("completed")
        .fetch_all(pool)
        .await?;

    rows.into_iter()
        .map(|row| {
            let offset: Option<i64> = row.try_get("part_offset")?;
            let etag: Option<String> = row.try_get("etag")?;
            let part_index: i32 = row.try_get("part_index")?;
            let part_number: i32 = row.try_get("part_number")?;
            let size: i64 = row.try_get("size")?;
            let offset = offset.ok_or_else(|| {
                sqlx::Error::Decode(Box::new(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "upload part missing offset",
                )))
            })?;
            let etag = etag.ok_or_else(|| {
                sqlx::Error::Decode(Box::new(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "upload part missing etag",
                )))
            })?;
            Ok(UploadPartRecord {
                part_index,
                part_number,
                offset,
                size,
                etag,
            })
        })
        .collect()
}

pub async fn get_completed_part_count(
    pool: &AnyPool,
    driver: DatabaseDriver,
    upload_id: &str,
) -> Result<i64, sqlx::Error> {
    let query = rewrite_placeholders(
        "SELECT COUNT(*) AS count FROM cache_upload_parts WHERE upload_id = ? AND state = ?",
        driver,
    );
    let count = sqlx::query_scalar::<_, i64>(&query)
        .bind(upload_id)
        .bind("completed")
        .fetch_one(pool)
        .await?;
    Ok(count)
}

pub async fn transition_to_uploading(
    pool: &AnyPool,
    database_driver: DatabaseDriver,
    upload_id: &str,
    status: &mut UploadStatus,
) -> Result<(), ApiError> {
    let ready = meta::transition_upload_state(
        pool,
        database_driver,
        upload_id,
        &["reserved", "ready", "uploading"],
        "uploading",
    )
    .await?;

    if !ready {
        *status = get_upload_status(pool, database_driver, upload_id).await?;
        if status.pending_finalize || status.state != "uploading" {
            return Err(ApiError::BadRequest(
                "upload is not ready to accept more parts".into(),
            ));
        }
    }

    Ok(())
}

pub async fn transition_upload_state(
    pool: &AnyPool,
    driver: DatabaseDriver,
    upload_id: &str,
    allowed: &[&str],
    next: &str,
) -> Result<bool, sqlx::Error> {
    let upload = fetch_upload(pool, driver, upload_id).await?;
    if !allowed.iter().any(|state| *state == upload.state) {
        return Ok(false);
    }

    let now = Utc::now().timestamp();
    let query = rewrite_placeholders(
        "UPDATE cache_uploads SET state = ?, updated_at = ? WHERE upload_id = ? AND state = ?",
        driver,
    );
    let updated = sqlx::query(&query)
        .bind(next)
        .bind(now)
        .bind(upload_id)
        .bind(upload.state)
        .execute(pool)
        .await?;

    Ok(updated.rows_affected() == 1)
}
