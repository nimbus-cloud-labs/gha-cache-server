use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use chrono::Utc;
use sqlx::AnyPool;
use tokio::time::{MissedTickBehavior, interval};
use tracing::{debug, error, info, warn};

use crate::config::{CleanupSettings, DatabaseDriver};
use crate::meta::{self, ArtifactEntry, CacheEntry};
use crate::storage::{BlobStore, generation_prefix};

pub async fn run_cleanup_loop(
    pool: AnyPool,
    store: Arc<dyn BlobStore>,
    settings: CleanupSettings,
    driver: DatabaseDriver,
) {
    let mut ticker = interval(settings.interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;

        if let Err(err) = run_cleanup_once(&pool, store.clone(), &settings, driver).await {
            error!(?err, "cleanup iteration failed");
        }
    }
}

pub async fn run_cleanup_once(
    pool: &AnyPool,
    store: Arc<dyn BlobStore>,
    settings: &CleanupSettings,
    driver: DatabaseDriver,
) -> anyhow::Result<()> {
    let now = Utc::now();

    let expired = meta::expired_entries(pool, driver, now, settings.max_entry_age).await?;
    if !expired.is_empty() {
        info!(count = expired.len(), "removing expired cache entries");
    }
    for entry in expired {
        match purge_entry(&store, pool, driver, &entry).await {
            Ok(()) => {
                debug!(entry_id = %entry.id, "deleted expired cache entry");
            }
            Err(err) => {
                error!(
                    entry_id = %entry.id,
                    storage_key = %entry.storage_key,
                    ?err,
                    "failed to delete expired cache entry"
                );
            }
        }
    }

    let expired_artifacts = meta::expired_artifact_entries(pool, driver, now).await?;
    if !expired_artifacts.is_empty() {
        info!(
            count = expired_artifacts.len(),
            "removing expired artifact entries"
        );
    }
    for entry in expired_artifacts {
        match purge_artifact(&store, pool, driver, &entry).await {
            Ok(()) => {
                debug!(artifact_id = %entry.id, "deleted expired artifact entry");
            }
            Err(err) => {
                error!(
                    artifact_id = %entry.id,
                    storage_key = %entry.storage_key,
                    ?err,
                    "failed to delete expired artifact entry"
                );
            }
        }
    }

    if let Some(limit) = settings.max_total_bytes {
        let mut usage = meta::total_occupancy(pool, driver).await?.max(0) as u64;
        if usage > limit {
            info!(current = usage, limit, "cache usage exceeds threshold");
            let entries = meta::list_entries_ordered(pool, driver, None).await?;
            for entry in entries {
                if usage <= limit {
                    break;
                }

                match purge_entry(&store, pool, driver, &entry).await {
                    Ok(()) => {
                        let size = clamp_size(entry.size_bytes);
                        usage = usage.saturating_sub(size);
                        debug!(
                            entry_id = %entry.id,
                            size,
                            usage,
                            limit,
                            "deleted entry to reclaim space"
                        );
                    }
                    Err(err) => {
                        error!(
                            entry_id = %entry.id,
                            storage_key = %entry.storage_key,
                            ?err,
                            "failed to delete cache entry during cleanup"
                        );
                    }
                }
            }

            if usage > limit {
                warn!(
                    current = usage,
                    limit, "cleanup loop could not reduce usage below threshold"
                );
            }
        }
    }

    if let Some(min_available) = settings.min_available_bytes {
        if let Some(path) = settings.filesystem_path.as_deref() {
            match available_bytes(path).await {
                Ok(available) => {
                    enforce_available_space(
                        pool,
                        store.clone(),
                        settings,
                        driver,
                        min_available,
                        available,
                    )
                    .await?;
                }
                Err(err) => {
                    warn!(
                        path = %path.display(),
                        ?err,
                        "failed to read filesystem free space for cleanup"
                    );
                }
            }
        } else {
            debug!(
                min_available,
                "filesystem free-space cleanup is configured but unavailable for this storage backend"
            );
        }
    }

    Ok(())
}

async fn enforce_available_space(
    pool: &AnyPool,
    store: Arc<dyn BlobStore>,
    settings: &CleanupSettings,
    driver: DatabaseDriver,
    min_available: u64,
    mut available: u64,
) -> anyhow::Result<()> {
    if available >= min_available {
        return Ok(());
    }

    let target = settings
        .target_available_bytes
        .unwrap_or(min_available)
        .max(min_available);
    info!(
        available,
        min_available, target, "filesystem free space is below threshold"
    );

    let entries = meta::list_entries_ordered(pool, driver, None).await?;
    for entry in entries {
        if available >= target {
            break;
        }

        match purge_entry(&store, pool, driver, &entry).await {
            Ok(()) => {
                let size = clamp_size(entry.size_bytes);
                available = available.saturating_add(size);
                debug!(
                    entry_id = %entry.id,
                    size,
                    available,
                    target,
                    "deleted entry to reclaim filesystem space"
                );
            }
            Err(err) => {
                error!(
                    entry_id = %entry.id,
                    storage_key = %entry.storage_key,
                    ?err,
                    "failed to delete cache entry during filesystem cleanup"
                );
            }
        }
    }

    if available < target {
        warn!(
            available,
            target, "cleanup loop could not restore filesystem free space to target"
        );
    }

    Ok(())
}

async fn available_bytes(path: &Path) -> anyhow::Result<u64> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || available_bytes_blocking(&path))
        .await
        .context("filesystem free-space check panicked")?
}

#[cfg(unix)]
fn available_bytes_blocking(path: &Path) -> anyhow::Result<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes())
        .with_context(|| format!("path contains an interior NUL byte: {}", path.display()))?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `c_path` is a valid, NUL-terminated filesystem path and `stat`
    // points to writable memory for `statvfs` to initialize on success.
    let result = unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) };
    if result != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("failed to stat filesystem for {}", path.display()));
    }

    // SAFETY: `statvfs` returned success, so it initialized `stat`.
    let stat = unsafe { stat.assume_init() };
    Ok(stat.f_bavail.saturating_mul(stat.f_frsize))
}

#[cfg(not(unix))]
fn available_bytes_blocking(path: &Path) -> anyhow::Result<u64> {
    let _ = path;
    anyhow::bail!("filesystem free-space cleanup is not supported on this platform")
}

async fn purge_entry(
    store: &Arc<dyn BlobStore>,
    pool: &AnyPool,
    driver: DatabaseDriver,
    entry: &CacheEntry,
) -> anyhow::Result<()> {
    store
        .delete(&entry.storage_key)
        .await
        .with_context(|| format!("failed to delete blob {}", entry.storage_key))?;
    meta::delete_entry(pool, driver, entry.id)
        .await
        .with_context(|| format!("failed to delete metadata for entry {}", entry.id))?;
    Ok(())
}

async fn purge_artifact(
    store: &Arc<dyn BlobStore>,
    pool: &AnyPool,
    driver: DatabaseDriver,
    entry: &ArtifactEntry,
) -> anyhow::Result<()> {
    store
        .delete(&entry.storage_key)
        .await
        .with_context(|| format!("failed to delete blob {}", entry.storage_key))?;
    meta::remove_artifact_entry(pool, driver, entry.id)
        .await
        .with_context(|| format!("failed to delete metadata for artifact {}", entry.id))?;
    Ok(())
}

fn clamp_size(value: i64) -> u64 {
    if value < 0 { 0 } else { value as u64 }
}

pub async fn delete_all_caches(
    pool: &AnyPool,
    driver: DatabaseDriver,
    store: Arc<dyn BlobStore>,
) -> anyhow::Result<usize> {
    let deleted = meta::list_entries_ordered(pool, driver, None).await?.len();
    let generation = meta::rotate_generation_and_clear_entries(pool, driver).await?;
    let retired_prefix = generation_prefix(generation.previous);

    if let Err(err) = store.delete_prefix(&retired_prefix).await {
        warn!(
            prefix = %retired_prefix,
            ?err,
            "failed to delete retired cache generation from blob storage"
        );
    }

    Ok(deleted)
}
