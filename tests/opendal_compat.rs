#[cfg(not(any(test, feature = "test-util")))]
compile_error!("Enable the `test-util` feature to run these tests outside `cargo test`.");

use std::ffi::{OsStr, OsString};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::Router;
use once_cell::sync::Lazy;
use opendal::services::Ghac;
use opendal::{ErrorKind, Operator};
use reqwest::Client;
use serde_json::json;
use sqlx::AnyPool;
use sqlx::any::AnyPoolOptions;
use tempfile::TempDir;
use tokio::sync::{Mutex, oneshot};
use tokio::time::sleep;

use gha_cache_server::config::{
    BlobStoreSelector, CleanupSettings, Config, DatabaseDriver, FsConfig,
};
use gha_cache_server::http;
use gha_cache_server::storage::{BlobStore, fs::FsStore};

struct EnvVarGuard {
    key: &'static str,
    previous: Option<OsString>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: impl AsRef<OsStr>) -> Self {
        let previous = std::env::var_os(key);
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, previous }
    }

    fn remove(key: &'static str) -> Self {
        let previous = std::env::var_os(key);
        unsafe {
            std::env::remove_var(key);
        }
        Self { key, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        if let Some(previous) = &self.previous {
            unsafe {
                std::env::set_var(self.key, previous);
            }
        } else {
            unsafe {
                std::env::remove_var(self.key);
            }
        }
    }
}

static ENV_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
static RUSTLS_PROVIDER: Lazy<()> = Lazy::new(|| {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("install rustls provider");
});

struct TestServer {
    base_url: String,
    shutdown: Option<oneshot::Sender<()>>,
    handle: tokio::task::JoinHandle<Result<()>>,
    _tempdir: TempDir,
}

impl TestServer {
    async fn start() -> Result<Self> {
        Lazy::force(&RUSTLS_PROVIDER);
        let tempdir = TempDir::new()?;
        let root_path = tempdir.path().to_path_buf();

        sqlx::any::install_default_drivers();
        let pool: AnyPool = AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:?cache=shared")
            .await?;
        sqlx::migrate!("./migrations/sqlite").run(&pool).await?;

        let store = FsStore::new(root_path.clone(), None, None, None).await?;
        let store: Arc<dyn BlobStore> = Arc::new(store);

        let cfg = Config {
            port: 0,
            enable_direct_downloads: false,
            defer_finalize_in_background: true,
            request_timeout: Duration::from_secs(30),
            max_concurrency: 16,
            database_url: "sqlite::memory:?cache=shared".into(),
            database_driver: DatabaseDriver::Sqlite,
            blob_store: BlobStoreSelector::Fs,
            s3: None,
            fs: Some(FsConfig {
                root: root_path,
                uploads_root: None,
                file_mode: None,
                dir_mode: None,
            }),
            gcs: None,
            cleanup: CleanupSettings {
                interval: Duration::from_secs(3600),
                max_entry_age: None,
                max_total_bytes: None,
                min_available_bytes: None,
                target_available_bytes: None,
                filesystem_path: None,
            },
        };

        let app: Router = http::build_router(pool, store, &cfg);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let base_url = format!("http://{addr}/");

        let (tx, rx) = oneshot::channel();
        let server = axum::serve(listener, app).with_graceful_shutdown(async move {
            let _ = rx.await;
        });

        let handle = tokio::spawn(async move { server.await.map_err(anyhow::Error::from) });

        Ok(Self {
            base_url,
            shutdown: Some(tx),
            handle,
            _tempdir: tempdir,
        })
    }

    async fn stop(mut self) -> Result<()> {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }

        self.handle.abort();
        match self.handle.await {
            Ok(result) => result,
            Err(err) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
            Err(_) => Ok(()),
        }
    }

    fn operator(&self) -> Result<Operator> {
        let builder = Ghac::default()
            .endpoint(&self.base_url)
            .runtime_token("test-token")
            .version("gha-cache-server-tests");
        Ok(Operator::new(builder)?.finish())
    }
}

#[tokio::test]
async fn opendal_read_on_empty_cache_returns_not_found() -> Result<()> {
    let _guard = ENV_MUTEX.lock().await;

    let _v2 = EnvVarGuard::set("ACTIONS_CACHE_SERVICE_V2", "enabled");
    let _github_url = EnvVarGuard::remove("GITHUB_SERVER_URL");

    let server = TestServer::start().await?;
    let operator = server.operator()?;

    let err = operator
        .read("missing-entry")
        .await
        .expect_err("read should return an error for missing entries");
    assert_eq!(err.kind(), ErrorKind::NotFound);

    server.stop().await?;

    Ok(())
}

#[tokio::test]
async fn opendal_write_and_read_roundtrip() -> Result<()> {
    let _guard = ENV_MUTEX.lock().await;

    let _v2 = EnvVarGuard::remove("ACTIONS_CACHE_SERVICE_V2");
    let _github_url = EnvVarGuard::remove("GITHUB_SERVER_URL");

    let server = TestServer::start().await?;
    let operator = server.operator()?;

    let key = "examples/cache-key";
    let payload = b"hello from opendal";

    operator.write(key, payload.as_ref()).await?;
    let mut attempts = 0;
    let read_back = loop {
        match operator.read(key).await {
            Ok(bytes) => break bytes,
            Err(err) if err.kind() == ErrorKind::NotFound && attempts < 50 => {
                attempts += 1;
                sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(err) => return Err(err.into()),
        }
    };
    assert_eq!(read_back.to_vec(), payload);

    server.stop().await?;

    Ok(())
}

#[tokio::test]
async fn rest_reserve_cache_returns_safe_numeric_identifier() -> Result<()> {
    let _guard = ENV_MUTEX.lock().await;

    let _v2 = EnvVarGuard::remove("ACTIONS_CACHE_SERVICE_V2");
    let _github_url = EnvVarGuard::remove("GITHUB_SERVER_URL");

    let server = TestServer::start().await?;

    let client = Client::new();
    let url = format!("{}{}_apis/artifactcache/caches", server.base_url, "");
    let response = client
        .post(url)
        .header("accept", "application/json;api-version=6.0-preview.1")
        .json(&json!({ "key": "rest/string-id", "version": "v1" }))
        .send()
        .await?;
    assert!(response.status().is_success());

    let body: serde_json::Value = response.json().await?;
    let cache_id = body
        .get("cacheId")
        .and_then(|value| value.as_i64())
        .ok_or_else(|| anyhow::anyhow!("cacheId is missing"))?;
    const MAX_SAFE: i64 = 9_007_199_254_740_991;
    assert!((1..=MAX_SAFE).contains(&cache_id));

    server.stop().await?;

    Ok(())
}
