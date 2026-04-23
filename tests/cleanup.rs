#[cfg(not(any(test, feature = "test-util")))]
compile_error!("Enable the `test-util` feature to run these tests outside `cargo test`.");

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use gha_cache_server::cleanup;
use gha_cache_server::config::{CleanupSettings, DatabaseDriver};
use gha_cache_server::meta::{self, CacheEntry};
use gha_cache_server::storage::fs::FsStore;
use gha_cache_server::storage::{BlobStore, generation_prefix};
use sqlx::AnyPool;
use sqlx::any::AnyPoolOptions;
use tempfile::TempDir;
use tokio::fs;
use tokio::task::yield_now;

const TEST_VERSION: &str = "v1";

async fn setup_pool() -> AnyPool {
    sqlx::any::install_default_drivers();
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:?cache=shared")
        .await
        .expect("connect sqlite");
    sqlx::migrate!("./migrations/sqlite")
        .run(&pool)
        .await
        .expect("run migrations");
    pool
}

async fn create_entry_with_file(
    pool: &AnyPool,
    store_root: &Path,
    key: &str,
    last_access: i64,
    ttl: i64,
    contents: &[u8],
) -> CacheEntry {
    let entry = meta::create_entry(
        pool,
        DatabaseDriver::Sqlite,
        "org",
        "repo",
        key,
        TEST_VERSION,
        "_",
        key,
    )
    .await
    .expect("create entry");

    let path = store_root.join(&entry.storage_key);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .expect("create storage directories");
    }
    fs::write(&path, contents)
        .await
        .expect("write entry contents");

    sqlx::query(
        "UPDATE cache_entries SET last_access_at = ?, ttl_seconds = ?, size_bytes = ? WHERE id = ?",
    )
    .bind(last_access)
    .bind(ttl)
    .bind(contents.len() as i64)
    .bind(entry.id.to_string())
    .execute(pool)
    .await
    .expect("update entry fields");

    entry
}

#[tokio::test]
async fn cleanup_removes_expired_entries_and_files() {
    let pool = setup_pool().await;
    let temp_dir = TempDir::new().expect("temp dir");
    let store = Arc::new(
        FsStore::new(temp_dir.path().to_path_buf(), None, None, None)
            .await
            .expect("create store"),
    );

    let now = chrono::Utc::now().timestamp();
    let _expired =
        create_entry_with_file(&pool, temp_dir.path(), "expired", now - 100, 10, b"payload").await;

    let settings = CleanupSettings {
        interval: Duration::from_millis(50),
        max_entry_age: None,
        max_total_bytes: None,
    };

    let cleanup_pool = pool.clone();
    let cleanup_store: Arc<dyn BlobStore> = store.clone();
    let handle = tokio::spawn(async move {
        cleanup::run_cleanup_loop(
            cleanup_pool,
            cleanup_store,
            settings,
            DatabaseDriver::Sqlite,
        )
        .await;
    });

    tokio::time::sleep(Duration::from_millis(200)).await;
    yield_now().await;

    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(1) FROM cache_entries")
        .fetch_one(&pool)
        .await
        .expect("count entries");
    assert_eq!(remaining, 0);

    let path = temp_dir.path().join("expired");
    assert!(fs::metadata(&path).await.is_err(), "file should be removed");

    handle.abort();
    let _ = handle.await;
}

#[tokio::test]
async fn cleanup_enforces_size_limit() {
    let pool = setup_pool().await;
    let temp_dir = TempDir::new().expect("temp dir");
    let store = Arc::new(
        FsStore::new(temp_dir.path().to_path_buf(), None, None, None)
            .await
            .expect("create store"),
    );

    let base = chrono::Utc::now().timestamp();
    let _older = create_entry_with_file(
        &pool,
        temp_dir.path(),
        "older",
        base - 200,
        1_000,
        b"abcdefgh",
    )
    .await;
    let newer = create_entry_with_file(
        &pool,
        temp_dir.path(),
        "newer",
        base - 100,
        1_000,
        b"ijklmnop",
    )
    .await;

    let settings = CleanupSettings {
        interval: Duration::from_millis(50),
        max_entry_age: None,
        max_total_bytes: Some(8),
    };

    let cleanup_pool = pool.clone();
    let cleanup_store: Arc<dyn BlobStore> = store.clone();
    let handle = tokio::spawn(async move {
        cleanup::run_cleanup_loop(
            cleanup_pool,
            cleanup_store,
            settings,
            DatabaseDriver::Sqlite,
        )
        .await;
    });

    tokio::time::sleep(Duration::from_millis(200)).await;
    yield_now().await;

    let remaining_ids: Vec<String> =
        sqlx::query_scalar("SELECT id FROM cache_entries ORDER BY cache_key")
            .fetch_all(&pool)
            .await
            .expect("fetch remaining ids");
    assert_eq!(remaining_ids.len(), 1);
    assert_eq!(remaining_ids[0], newer.id.to_string());

    assert!(fs::metadata(temp_dir.path().join("older")).await.is_err());
    assert!(fs::metadata(temp_dir.path().join("newer")).await.is_ok());

    handle.abort();
    let _ = handle.await;
}

#[tokio::test]
async fn delete_all_caches_rotates_generation_and_removes_retired_prefix() {
    let pool = setup_pool().await;
    let temp_dir = TempDir::new().expect("temp dir");
    let store = Arc::new(
        FsStore::new(temp_dir.path().to_path_buf(), None, None, None)
            .await
            .expect("create store"),
    );

    assert_eq!(
        meta::current_generation(&pool, DatabaseDriver::Sqlite)
            .await
            .expect("read initial generation"),
        1
    );

    let retired_prefix = generation_prefix(1);
    let storage_key = format!("{retired_prefix}/ac/test/cache.tzst");
    let _entry = meta::create_entry(
        &pool,
        DatabaseDriver::Sqlite,
        "org",
        "repo",
        "primary",
        TEST_VERSION,
        "_",
        &storage_key,
    )
    .await
    .expect("create entry");

    let path = temp_dir.path().join(&storage_key);
    fs::create_dir_all(path.parent().expect("storage parent"))
        .await
        .expect("create storage directories");
    fs::write(&path, b"payload").await.expect("write payload");

    let deleted = cleanup::delete_all_caches(&pool, DatabaseDriver::Sqlite, store.clone())
        .await
        .expect("delete all caches");
    assert_eq!(deleted, 1);

    assert_eq!(
        meta::current_generation(&pool, DatabaseDriver::Sqlite)
            .await
            .expect("read rotated generation"),
        2
    );
    assert!(
        fs::metadata(temp_dir.path().join(retired_prefix))
            .await
            .is_err(),
        "retired generation prefix should be removed"
    );

    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(1) FROM cache_entries")
        .fetch_one(&pool)
        .await
        .expect("count entries");
    assert_eq!(remaining, 0);

    let uploads: i64 = sqlx::query_scalar("SELECT COUNT(1) FROM cache_uploads")
        .fetch_one(&pool)
        .await
        .expect("count uploads");
    assert_eq!(uploads, 0);
}
