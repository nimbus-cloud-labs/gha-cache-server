use chrono::{DateTime, Utc};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use sqlx::{AnyPool, Error, Row, Transaction};
use std::collections::HashSet;
use std::convert::TryFrom;
use std::io;
use std::time::Duration;
use tokio::time;
use uuid::Uuid;

use crate::config::DatabaseDriver;
use crate::db::{rewrite_placeholders, safe_sql};
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

/// Captures an artifact entry row loaded from the metadata database.
///
/// Artifact entries model the GitHub Actions results service artifact metadata
/// independently from cache entries. They are addressed by workflow run/job
/// backend identifiers plus an artifact name and point to a ZIP payload stored
/// in the configured blob backend.
///
/// # Examples
/// ```
/// use chrono::{DateTime, Utc};
/// use gha_cache_server::meta::ArtifactEntry;
/// use uuid::Uuid;
///
/// let timestamp = DateTime::<Utc>::from_timestamp(0, 0).expect("valid timestamp");
/// let entry = ArtifactEntry {
///     id: Uuid::nil(),
///     numeric_id: 1,
///     workflow_run_backend_id: "run".into(),
///     workflow_job_run_backend_id: "job".into(),
///     name: "logs".into(),
///     version: 4,
///     size_bytes: 128,
///     hash: Some("sha256:abc".into()),
///     storage_key: "artifacts/run/job/logs.zip".into(),
///     state: "finalized".into(),
///     expires_at: None,
///     created_at: timestamp,
///     updated_at: timestamp,
/// };
/// assert_eq!(entry.name, "logs");
/// ```
#[derive(Debug, Serialize, Deserialize)]
pub struct ArtifactEntry {
    pub id: Uuid,
    pub numeric_id: i64,
    pub workflow_run_backend_id: String,
    pub workflow_job_run_backend_id: String,
    pub name: String,
    pub version: i64,
    pub size_bytes: i64,
    pub hash: Option<String>,
    pub storage_key: String,
    pub state: String,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Captures an artifact upload row loaded from the metadata database.
///
/// The upload row links one results-service artifact to the backend multipart
/// upload session used to receive the ZIP payload.
///
/// # Examples
/// ```
/// use gha_cache_server::meta::ArtifactUploadRow;
/// use uuid::Uuid;
///
/// let row = ArtifactUploadRow {
///     id: Uuid::nil(),
///     artifact_id: Uuid::nil(),
///     upload_id: "upload".into(),
///     state: "reserved".into(),
///     etag: None,
///     size_bytes: 0,
/// };
/// assert_eq!(row.upload_id, "upload");
/// ```
#[derive(Debug, Serialize, Deserialize)]
pub struct ArtifactUploadRow {
    pub id: Uuid,
    pub artifact_id: Uuid,
    pub upload_id: String,
    pub state: String,
    pub etag: Option<String>,
    pub size_bytes: i64,
}

/// Describes one uploaded artifact block.
///
/// Artifact block records preserve the Azure block identifier supplied by the
/// client and point to the temporary blob that stores the block payload until
/// the block list defines the final artifact order.
///
/// # Examples
/// ```
/// use gha_cache_server::meta::ArtifactUploadPartRecord;
///
/// let part = ArtifactUploadPartRecord {
///     block_id: "block".into(),
///     part_number: 1,
///     size: 1024,
///     etag: "etag".into(),
///     storage_key: Some("artifact-blocks/block".into()),
/// };
/// assert_eq!(part.part_number, 1);
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArtifactUploadPartRecord {
    pub block_id: String,
    pub part_number: i32,
    pub size: i64,
    pub etag: String,
    pub storage_key: Option<String>,
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
        let result = sqlx::query(safe_sql(&insert_sql))
            .bind(&entry_str)
            .bind(candidate)
            .execute(tx.as_mut())
            .await;

        match result {
            Ok(_) => return Ok(candidate),
            Err(sqlx::Error::Database(db_err)) if db_err.is_unique_violation() => {
                if let Some(existing) = sqlx::query_scalar::<_, i64>(safe_sql(&fetch_sql))
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

fn map_artifact_entry(row: sqlx::any::AnyRow) -> Result<ArtifactEntry, sqlx::Error> {
    let id = parse_uuid(row.try_get::<String, _>("id")?)?;
    let created_at = timestamp_to_datetime(row.try_get::<i64, _>("created_at")?)?;
    let updated_at = timestamp_to_datetime(row.try_get::<i64, _>("updated_at")?)?;
    let expires_at = row
        .try_get::<Option<i64>, _>("expires_at")?
        .map(timestamp_to_datetime)
        .transpose()?;

    Ok(ArtifactEntry {
        id,
        numeric_id: row.try_get("numeric_id")?,
        workflow_run_backend_id: row.try_get("workflow_run_backend_id")?,
        workflow_job_run_backend_id: row.try_get("workflow_job_run_backend_id")?,
        name: row.try_get("name")?,
        version: row.try_get("version")?,
        size_bytes: row.try_get("size_bytes")?,
        hash: row.try_get("hash")?,
        storage_key: row.try_get("storage_key")?,
        state: row.try_get("state")?,
        expires_at,
        created_at,
        updated_at,
    })
}

fn map_artifact_upload_row(row: sqlx::any::AnyRow) -> Result<ArtifactUploadRow, sqlx::Error> {
    Ok(ArtifactUploadRow {
        id: parse_uuid(row.try_get::<String, _>("id")?)?,
        artifact_id: parse_uuid(row.try_get::<String, _>("artifact_id")?)?,
        upload_id: row.try_get("upload_id")?,
        state: row.try_get("state")?,
        etag: row.try_get("etag")?,
        size_bytes: row.try_get("size_bytes")?,
    })
}

fn map_artifact_upload_part(
    row: sqlx::any::AnyRow,
) -> Result<ArtifactUploadPartRecord, sqlx::Error> {
    Ok(ArtifactUploadPartRecord {
        block_id: row.try_get("block_id")?,
        part_number: i32::try_from(row.try_get::<i64, _>("part_number")?).map_err(|err| {
            sqlx::Error::Decode(Box::new(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid artifact part number: {err}"),
            )))
        })?,
        size: row.try_get("size")?,
        etag: row.try_get("etag")?,
        storage_key: row.try_get("storage_key")?,
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
    let row = sqlx::query(safe_sql(&query))
        .bind(upload_id)
        .fetch_one(pool)
        .await?;
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
    let row = sqlx::query(safe_sql(&query))
        .bind(upload_id)
        .fetch_one(pool)
        .await?;
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
    let result = sqlx::query(safe_sql(&query))
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
    let result = sqlx::query(safe_sql(&update_query))
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
    let row = sqlx::query(safe_sql(&select_query))
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
    let result = sqlx::query(safe_sql(&query))
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
    let row = sqlx::query(safe_sql(&query))
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
    sqlx::query(safe_sql(&query))
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
    sqlx::query(safe_sql(&query))
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
        sqlx::query(safe_sql(&query))
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
        sqlx::query(safe_sql(&query))
            .bind(ts)
            .fetch_all(pool)
            .await?
    };

    rows.into_iter().map(map_cache_entry).collect()
}

pub async fn total_occupancy(pool: &AnyPool, driver: DatabaseDriver) -> Result<i64, sqlx::Error> {
    let query = rewrite_placeholders(
        "SELECT COALESCE(SUM(size_bytes), 0) FROM cache_entries",
        driver,
    );
    let total = sqlx::query_scalar::<_, i64>(safe_sql(&query))
        .fetch_one(pool)
        .await?;
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
        let rows = sqlx::query(safe_sql(&query))
            .bind(limit)
            .fetch_all(pool)
            .await?;

        rows.into_iter().map(map_cache_entry).collect()
    } else {
        let query = rewrite_placeholders(
            "SELECT id, org, repo, cache_key, cache_version, scope, size_bytes, checksum, storage_key, created_at, last_access_at, ttl_seconds FROM cache_entries ORDER BY last_access_at ASC",
            driver,
        );
        let rows = sqlx::query(safe_sql(&query)).fetch_all(pool).await?;

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
    sqlx::query_scalar::<_, i64>(safe_sql(&query))
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
    let previous = sqlx::query_scalar::<_, i64>(safe_sql(&select_query))
        .bind(1_i32)
        .fetch_one(&mut *tx)
        .await?;
    let current = previous + 1;

    let update_query = rewrite_placeholders(
        "UPDATE cache_state SET current_generation = ? WHERE singleton = ?",
        driver,
    );
    sqlx::query(safe_sql(&update_query))
        .bind(current)
        .bind(1_i32)
        .execute(&mut *tx)
        .await?;

    let delete_uploads_query = rewrite_placeholders("DELETE FROM cache_uploads", driver);
    sqlx::query(safe_sql(&delete_uploads_query))
        .execute(&mut *tx)
        .await?;

    let delete_entries_query = rewrite_placeholders("DELETE FROM cache_entries", driver);
    sqlx::query(safe_sql(&delete_entries_query))
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;

    Ok(CacheGeneration { previous, current })
}

#[expect(
    clippy::too_many_arguments,
    reason = "Artifact creation receives all results-service identity fields"
)]
pub async fn create_artifact_entry(
    pool: &AnyPool,
    driver: DatabaseDriver,
    workflow_run_backend_id: &str,
    workflow_job_run_backend_id: &str,
    name: &str,
    version: i64,
    storage_key: &str,
    expires_at: Option<DateTime<Utc>>,
) -> Result<ArtifactEntry, sqlx::Error> {
    let id = Uuid::new_v4();
    let expires_at = expires_at.map(|value| value.timestamp());
    let now = Utc::now().timestamp();
    let insert_query = rewrite_placeholders(
        "INSERT INTO artifact_entries (id, numeric_id, workflow_run_backend_id, workflow_job_run_backend_id, name, version, storage_key, state, expires_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        driver,
    );

    loop {
        let numeric_id = generate_cache_numeric_id();
        let result = sqlx::query(safe_sql(&insert_query))
            .bind(id.to_string())
            .bind(numeric_id)
            .bind(workflow_run_backend_id)
            .bind(workflow_job_run_backend_id)
            .bind(name)
            .bind(version)
            .bind(storage_key)
            .bind("created")
            .bind(expires_at)
            .bind(now)
            .bind(now)
            .execute(pool)
            .await;

        match result {
            Ok(_) => return fetch_artifact_entry(pool, driver, id).await,
            Err(sqlx::Error::Database(db_err)) if db_err.is_unique_violation() => continue,
            Err(err) => return Err(err),
        }
    }
}

pub async fn fetch_artifact_entry(
    pool: &AnyPool,
    driver: DatabaseDriver,
    id: Uuid,
) -> Result<ArtifactEntry, sqlx::Error> {
    let query = rewrite_placeholders(
        "SELECT id, numeric_id, workflow_run_backend_id, workflow_job_run_backend_id, name, version, size_bytes, hash, storage_key, state, expires_at, created_at, updated_at FROM artifact_entries WHERE id = ?",
        driver,
    );
    let row = sqlx::query(safe_sql(&query))
        .bind(id.to_string())
        .fetch_one(pool)
        .await?;
    map_artifact_entry(row)
}

pub async fn find_active_artifact_by_name(
    pool: &AnyPool,
    driver: DatabaseDriver,
    workflow_run_backend_id: &str,
    name: &str,
) -> Result<Option<ArtifactEntry>, sqlx::Error> {
    let query = rewrite_placeholders(
        "SELECT id, numeric_id, workflow_run_backend_id, workflow_job_run_backend_id, name, version, size_bytes, hash, storage_key, state, expires_at, created_at, updated_at FROM artifact_entries WHERE workflow_run_backend_id = ? AND name = ? AND state != ? ORDER BY created_at DESC LIMIT 1",
        driver,
    );
    let maybe = sqlx::query(safe_sql(&query))
        .bind(workflow_run_backend_id)
        .bind(name)
        .bind("deleted")
        .fetch_optional(pool)
        .await?;
    maybe.map(map_artifact_entry).transpose()
}

pub async fn find_finalized_artifact_by_name(
    pool: &AnyPool,
    driver: DatabaseDriver,
    workflow_run_backend_id: &str,
    name: &str,
) -> Result<Option<ArtifactEntry>, sqlx::Error> {
    let query = rewrite_placeholders(
        "SELECT id, numeric_id, workflow_run_backend_id, workflow_job_run_backend_id, name, version, size_bytes, hash, storage_key, state, expires_at, created_at, updated_at FROM artifact_entries WHERE workflow_run_backend_id = ? AND name = ? AND state = ? ORDER BY created_at DESC LIMIT 1",
        driver,
    );
    let maybe = sqlx::query(safe_sql(&query))
        .bind(workflow_run_backend_id)
        .bind(name)
        .bind("finalized")
        .fetch_optional(pool)
        .await?;
    maybe.map(map_artifact_entry).transpose()
}

pub async fn find_latest_finalized_artifact_by_name(
    pool: &AnyPool,
    driver: DatabaseDriver,
    name: &str,
) -> Result<Option<ArtifactEntry>, sqlx::Error> {
    let query = rewrite_placeholders(
        "SELECT id, numeric_id, workflow_run_backend_id, workflow_job_run_backend_id, name, version, size_bytes, hash, storage_key, state, expires_at, created_at, updated_at FROM artifact_entries WHERE name = ? AND state = ? ORDER BY created_at DESC LIMIT 1",
        driver,
    );
    let maybe = sqlx::query(safe_sql(&query))
        .bind(name)
        .bind("finalized")
        .fetch_optional(pool)
        .await?;
    maybe.map(map_artifact_entry).transpose()
}

/// Lists the newest finalized artifact entry for each artifact name.
///
/// This fallback supports retry jobs whose GitHub artifact backend identifier no
/// longer matches the backend identifier that uploaded the original artifacts.
///
/// # Errors
///
/// Returns an error when the metadata query fails or a row cannot be decoded.
pub async fn list_latest_finalized_artifact_entries_by_name(
    pool: &AnyPool,
    driver: DatabaseDriver,
) -> Result<Vec<ArtifactEntry>, sqlx::Error> {
    let query = rewrite_placeholders(
        "SELECT id, numeric_id, workflow_run_backend_id, workflow_job_run_backend_id, name, version, size_bytes, hash, storage_key, state, expires_at, created_at, updated_at FROM artifact_entries WHERE state = ? ORDER BY created_at DESC",
        driver,
    );
    let rows = sqlx::query(safe_sql(&query))
        .bind("finalized")
        .fetch_all(pool)
        .await?;
    let mut seen = HashSet::new();
    rows.into_iter()
        .map(map_artifact_entry)
        .filter_map(|entry| match entry {
            Ok(entry) if seen.insert(entry.name.clone()) => Some(Ok(entry)),
            Ok(_) => None,
            Err(err) => Some(Err(err)),
        })
        .collect()
}

pub async fn list_artifact_entries(
    pool: &AnyPool,
    driver: DatabaseDriver,
    workflow_run_backend_id: &str,
    name_filter: Option<&str>,
    id_filter: Option<i64>,
) -> Result<Vec<ArtifactEntry>, sqlx::Error> {
    let base = "SELECT id, numeric_id, workflow_run_backend_id, workflow_job_run_backend_id, name, version, size_bytes, hash, storage_key, state, expires_at, created_at, updated_at FROM artifact_entries WHERE workflow_run_backend_id = ? AND state = ?";
    let sql = match (name_filter, id_filter) {
        (Some(_), Some(_)) => {
            format!("{base} AND name = ? AND numeric_id = ? ORDER BY created_at DESC")
        }
        (Some(_), None) => format!("{base} AND name = ? ORDER BY created_at DESC"),
        (None, Some(_)) => format!("{base} AND numeric_id = ? ORDER BY created_at DESC"),
        (None, None) => format!("{base} ORDER BY created_at DESC"),
    };
    let query = rewrite_placeholders(&sql, driver);
    let mut query = sqlx::query(safe_sql(&query))
        .bind(workflow_run_backend_id)
        .bind("finalized");
    if let Some(name_filter) = name_filter {
        query = query.bind(name_filter);
    }
    if let Some(id_filter) = id_filter {
        query = query.bind(id_filter);
    }

    let mut rows = query.fetch_all(pool).await?;
    if rows.is_empty() && (name_filter.is_some() || id_filter.is_some()) {
        let base = "SELECT id, numeric_id, workflow_run_backend_id, workflow_job_run_backend_id, name, version, size_bytes, hash, storage_key, state, expires_at, created_at, updated_at FROM artifact_entries WHERE state = ?";
        let sql = match (name_filter, id_filter) {
            (Some(_), Some(_)) => {
                format!("{base} AND name = ? AND numeric_id = ? ORDER BY created_at DESC LIMIT 1")
            }
            (Some(_), None) => format!("{base} AND name = ? ORDER BY created_at DESC LIMIT 1"),
            (None, Some(_)) => {
                format!("{base} AND numeric_id = ? ORDER BY created_at DESC LIMIT 1")
            }
            (None, None) => unreachable!("filtered artifact fallback requires a filter"),
        };
        let query = rewrite_placeholders(&sql, driver);
        let mut query = sqlx::query(safe_sql(&query)).bind("finalized");
        if let Some(name_filter) = name_filter {
            query = query.bind(name_filter);
        }
        if let Some(id_filter) = id_filter {
            query = query.bind(id_filter);
        }
        rows = query.fetch_all(pool).await?;
    }
    let entries: Vec<_> = rows
        .into_iter()
        .map(map_artifact_entry)
        .collect::<Result<_, _>>()?;
    if name_filter.is_none() && id_filter.is_none() {
        let mut entries = entries;
        let mut seen = entries
            .iter()
            .map(|entry| entry.name.clone())
            .collect::<HashSet<_>>();
        entries.extend(
            list_latest_finalized_artifact_entries_by_name(pool, driver)
                .await?
                .into_iter()
                .filter(|entry| seen.insert(entry.name.clone())),
        );
        return Ok(entries);
    }
    Ok(entries)
}

pub async fn expired_artifact_entries(
    pool: &AnyPool,
    driver: DatabaseDriver,
    now: DateTime<Utc>,
) -> Result<Vec<ArtifactEntry>, sqlx::Error> {
    let query = rewrite_placeholders(
        "SELECT id, numeric_id, workflow_run_backend_id, workflow_job_run_backend_id, name, version, size_bytes, hash, storage_key, state, expires_at, created_at, updated_at FROM artifact_entries WHERE expires_at IS NOT NULL AND expires_at < ? AND state != ? ORDER BY expires_at ASC",
        driver,
    );
    let rows = sqlx::query(safe_sql(&query))
        .bind(now.timestamp())
        .bind("deleted")
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(map_artifact_entry).collect()
}

pub async fn remove_artifact_entry(
    pool: &AnyPool,
    driver: DatabaseDriver,
    artifact_id: Uuid,
) -> Result<(), sqlx::Error> {
    let query = rewrite_placeholders("DELETE FROM artifact_entries WHERE id = ?", driver);
    sqlx::query(safe_sql(&query))
        .bind(artifact_id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn upsert_artifact_upload(
    pool: &AnyPool,
    driver: DatabaseDriver,
    artifact_id: Uuid,
    upload_id: &str,
) -> Result<ArtifactUploadRow, sqlx::Error> {
    let id = Uuid::new_v4();
    let insert_query = rewrite_placeholders(
        "INSERT INTO artifact_uploads (id, artifact_id, upload_id, state) VALUES (?, ?, ?, ?)",
        driver,
    );
    sqlx::query(safe_sql(&insert_query))
        .bind(id.to_string())
        .bind(artifact_id.to_string())
        .bind(upload_id)
        .bind("reserved")
        .execute(pool)
        .await?;

    fetch_artifact_upload(pool, driver, artifact_id).await
}

pub async fn fetch_artifact_upload(
    pool: &AnyPool,
    driver: DatabaseDriver,
    artifact_id: Uuid,
) -> Result<ArtifactUploadRow, sqlx::Error> {
    let query = rewrite_placeholders(
        "SELECT id, artifact_id, upload_id, state, etag, size_bytes FROM artifact_uploads WHERE artifact_id = ?",
        driver,
    );
    let row = sqlx::query(safe_sql(&query))
        .bind(artifact_id.to_string())
        .fetch_one(pool)
        .await?;
    map_artifact_upload_row(row)
}

pub async fn next_artifact_part_number(
    pool: &AnyPool,
    driver: DatabaseDriver,
    upload_id: &str,
    block_id: &str,
) -> Result<i32, sqlx::Error> {
    let existing_query = rewrite_placeholders(
        "SELECT part_number FROM artifact_upload_parts WHERE upload_id = ? AND block_id = ?",
        driver,
    );
    if let Some(existing) = sqlx::query_scalar::<_, i64>(safe_sql(&existing_query))
        .bind(upload_id)
        .bind(block_id)
        .fetch_optional(pool)
        .await?
    {
        return i32::try_from(existing).map_err(|err| {
            sqlx::Error::Decode(Box::new(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid artifact part number: {err}"),
            )))
        });
    }

    let max_query = rewrite_placeholders(
        "SELECT COALESCE(MAX(part_number), 0) FROM artifact_upload_parts WHERE upload_id = ?",
        driver,
    );
    let max = sqlx::query_scalar::<_, i64>(safe_sql(&max_query))
        .bind(upload_id)
        .fetch_one(pool)
        .await?;
    i32::try_from(max + 1).map_err(|err| {
        sqlx::Error::Decode(Box::new(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid artifact part number: {err}"),
        )))
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "Artifact part records are persisted from upload request fields"
)]
pub async fn record_artifact_part(
    pool: &AnyPool,
    driver: DatabaseDriver,
    upload_id: &str,
    block_id: &str,
    part_number: i32,
    size: i64,
    etag: &str,
    storage_key: &str,
) -> Result<(), sqlx::Error> {
    let now = Utc::now().timestamp();
    let insert_query = rewrite_placeholders(
        "INSERT INTO artifact_upload_parts (upload_id, block_id, part_number, size, etag, storage_key, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        driver,
    );
    let insert = sqlx::query(safe_sql(&insert_query))
        .bind(upload_id)
        .bind(block_id)
        .bind(i64::from(part_number))
        .bind(size)
        .bind(etag)
        .bind(storage_key)
        .bind(now)
        .bind(now)
        .execute(pool)
        .await;

    match insert {
        Ok(_) => Ok(()),
        Err(sqlx::Error::Database(db_err)) if db_err.is_unique_violation() => {
            let update_query = rewrite_placeholders(
                "UPDATE artifact_upload_parts SET part_number = ?, size = ?, etag = ?, storage_key = ?, updated_at = ? WHERE upload_id = ? AND block_id = ?",
                driver,
            );
            sqlx::query(safe_sql(&update_query))
                .bind(i64::from(part_number))
                .bind(size)
                .bind(etag)
                .bind(storage_key)
                .bind(now)
                .bind(upload_id)
                .bind(block_id)
                .execute(pool)
                .await?;
            Ok(())
        }
        Err(err) => Err(err),
    }
}

pub async fn artifact_parts_by_block_ids(
    pool: &AnyPool,
    driver: DatabaseDriver,
    upload_id: &str,
    block_ids: &[String],
) -> Result<Vec<ArtifactUploadPartRecord>, sqlx::Error> {
    let query = rewrite_placeholders(
        "SELECT block_id, part_number, size, etag, storage_key FROM artifact_upload_parts WHERE upload_id = ? AND block_id = ?",
        driver,
    );
    let mut parts = Vec::with_capacity(block_ids.len());
    for block_id in block_ids {
        let row = sqlx::query(safe_sql(&query))
            .bind(upload_id)
            .bind(block_id)
            .fetch_one(pool)
            .await?;
        parts.push(map_artifact_upload_part(row)?);
    }
    Ok(parts)
}

pub async fn mark_artifact_uploaded(
    pool: &AnyPool,
    driver: DatabaseDriver,
    artifact_id: Uuid,
    etag: &str,
    size_bytes: i64,
) -> Result<(), sqlx::Error> {
    let now = Utc::now().timestamp();
    let update_upload = rewrite_placeholders(
        "UPDATE artifact_uploads SET state = ?, etag = ?, size_bytes = ?, updated_at = ? WHERE artifact_id = ?",
        driver,
    );
    sqlx::query(safe_sql(&update_upload))
        .bind("uploaded")
        .bind(etag)
        .bind(size_bytes)
        .bind(now)
        .bind(artifact_id.to_string())
        .execute(pool)
        .await?;

    let update_entry = rewrite_placeholders(
        "UPDATE artifact_entries SET state = ?, size_bytes = ?, updated_at = ? WHERE id = ?",
        driver,
    );
    sqlx::query(safe_sql(&update_entry))
        .bind("uploading")
        .bind(size_bytes)
        .bind(now)
        .bind(artifact_id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn mark_artifact_multipart_committed(
    pool: &AnyPool,
    driver: DatabaseDriver,
    artifact_id: Uuid,
    size_bytes: i64,
) -> Result<(), sqlx::Error> {
    let now = Utc::now().timestamp();
    let update_upload = rewrite_placeholders(
        "UPDATE artifact_uploads SET state = ?, size_bytes = ?, updated_at = ? WHERE artifact_id = ?",
        driver,
    );
    sqlx::query(safe_sql(&update_upload))
        .bind("completed")
        .bind(size_bytes)
        .bind(now)
        .bind(artifact_id.to_string())
        .execute(pool)
        .await?;

    let update_entry = rewrite_placeholders(
        "UPDATE artifact_entries SET state = ?, size_bytes = ?, updated_at = ? WHERE id = ?",
        driver,
    );
    sqlx::query(safe_sql(&update_entry))
        .bind("uploading")
        .bind(size_bytes)
        .bind(now)
        .bind(artifact_id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn finalize_artifact_entry(
    pool: &AnyPool,
    driver: DatabaseDriver,
    artifact_id: Uuid,
    size_bytes: i64,
    hash: Option<&str>,
) -> Result<(), sqlx::Error> {
    let now = Utc::now().timestamp();
    let update_entry = rewrite_placeholders(
        "UPDATE artifact_entries SET state = ?, size_bytes = ?, hash = ?, updated_at = ? WHERE id = ?",
        driver,
    );
    sqlx::query(safe_sql(&update_entry))
        .bind("finalized")
        .bind(size_bytes)
        .bind(hash)
        .bind(now)
        .bind(artifact_id.to_string())
        .execute(pool)
        .await?;

    let update_upload = rewrite_placeholders(
        "UPDATE artifact_uploads SET state = ?, updated_at = ? WHERE artifact_id = ?",
        driver,
    );
    sqlx::query(safe_sql(&update_upload))
        .bind("completed")
        .bind(now)
        .bind(artifact_id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_artifact_entry(
    pool: &AnyPool,
    driver: DatabaseDriver,
    artifact_id: Uuid,
) -> Result<(), sqlx::Error> {
    let now = Utc::now().timestamp();
    let query = rewrite_placeholders(
        "UPDATE artifact_entries SET state = ?, updated_at = ? WHERE id = ?",
        driver,
    );
    sqlx::query(safe_sql(&query))
        .bind("deleted")
        .bind(now)
        .bind(artifact_id.to_string())
        .execute(pool)
        .await?;
    Ok(())
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
    sqlx::query(safe_sql(&query))
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
    sqlx::query_scalar::<_, i64>(safe_sql(&query))
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
    let maybe = sqlx::query(safe_sql(&query))
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
    let maybe_row = sqlx::query(safe_sql(&query))
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
    let insert = sqlx::query(safe_sql(&insert_query))
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
                sqlx::query(safe_sql(&update_query))
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
    let insert = sqlx::query(safe_sql(&insert_query))
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
                    sqlx::query(safe_sql(&update_query))
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
    let maybe_row = sqlx::query(safe_sql(&fetch_query))
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
        let total: i64 = sqlx::query(safe_sql(&sum_query))
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
    sqlx::query(safe_sql(&update_query))
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
    let rows = sqlx::query(safe_sql(&query))
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
    let count = sqlx::query_scalar::<_, i64>(safe_sql(&query))
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
    let updated = sqlx::query(safe_sql(&query))
        .bind(next)
        .bind(now)
        .bind(upload_id)
        .bind(upload.state)
        .execute(pool)
        .await?;

    Ok(updated.rows_affected() == 1)
}
