#[cfg(not(any(test, feature = "test-util")))]
compile_error!("Enable the `test-util` feature to run these tests outside `cargo test`.");

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::Router;
use base64::{Engine as _, engine::general_purpose};
use once_cell::sync::Lazy;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use sqlx::AnyPool;
use sqlx::any::AnyPoolOptions;
use tempfile::TempDir;
use tokio::sync::oneshot;

use gha_cache_server::config::{
    BlobStoreSelector, CleanupSettings, Config, DatabaseDriver, FsConfig,
};
use gha_cache_server::http;
use gha_cache_server::storage::{BlobStore, fs::FsStore};

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
            },
        };

        let app: Router = http::build_router(pool, store, &cfg);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let base_url = format!("http://{addr}");

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

    fn artifact_endpoint(&self, method: &str) -> String {
        format!(
            "{}/twirp/github.actions.results.api.v1.ArtifactService/{method}",
            self.base_url
        )
    }
}

#[derive(Deserialize)]
struct CreateArtifactResponse {
    #[serde(rename = "signedUploadUrl")]
    signed_upload_url: String,
}

#[derive(Deserialize)]
struct FinalizeArtifactResponse {
    ok: bool,
    #[serde(rename = "artifactId")]
    artifact_id: i64,
}

#[derive(Deserialize)]
struct ListArtifactsResponse {
    artifacts: Vec<ListArtifact>,
}

#[derive(Deserialize)]
struct ListArtifact {
    #[serde(rename = "databaseId")]
    database_id: i64,
    digest: String,
    name: String,
    size: i64,
}

#[derive(Deserialize)]
struct GetSignedArtifactUrlResponse {
    #[serde(rename = "signedUrl")]
    signed_url: String,
}

#[tokio::test]
async fn artifact_twirp_roundtrip_uses_local_storage() -> Result<()> {
    let server = TestServer::start().await?;
    let client = Client::new();

    let create: CreateArtifactResponse = client
        .post(server.artifact_endpoint("CreateArtifact"))
        .header("content-type", "application/json")
        .json(&json!({
            "workflowRunBackendId": "run-1",
            "workflowJobRunBackendId": "job-1",
            "name": "logs",
            "version": 4
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let retry_create: CreateArtifactResponse = client
        .post(server.artifact_endpoint("CreateArtifact"))
        .header("content-type", "application/json")
        .json(&json!({
            "workflowRunBackendId": "run-1",
            "workflowJobRunBackendId": "job-1",
            "name": "logs",
            "version": 4
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(retry_create.signed_upload_url, create.signed_upload_url);

    let payload = b"zip bytes for tests".to_vec();
    client
        .put(&create.signed_upload_url)
        .header("content-type", "application/zip")
        .body(payload.clone())
        .send()
        .await?
        .error_for_status()?;

    let finalized: FinalizeArtifactResponse = client
        .post(server.artifact_endpoint("FinalizeArtifact"))
        .header("content-type", "application/json")
        .json(&json!({
            "workflowRunBackendId": "run-1",
            "workflowJobRunBackendId": "job-1",
            "name": "logs",
            "size": payload.len(),
            "hash": "sha256:artifact-test-digest"
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(finalized.ok);
    assert!(finalized.artifact_id > 0);

    let listed: ListArtifactsResponse = client
        .post(server.artifact_endpoint("ListArtifacts"))
        .header("content-type", "application/json")
        .json(&json!({
            "workflowRunBackendId": "run-1-retry",
            "nameFilter": "logs"
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(listed.artifacts.len(), 1);
    assert_eq!(listed.artifacts[0].database_id, finalized.artifact_id);
    assert_eq!(listed.artifacts[0].digest, "sha256:artifact-test-digest");
    assert_eq!(listed.artifacts[0].name, "logs");
    assert_eq!(listed.artifacts[0].size, payload.len() as i64);

    let signed: GetSignedArtifactUrlResponse = client
        .post(server.artifact_endpoint("GetSignedArtifactURL"))
        .header("content-type", "application/json")
        .json(&json!({
            "workflowRunBackendId": "run-1-retry",
            "workflowJobRunBackendId": "job-downloader",
            "name": "logs"
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let downloaded = client
        .get(signed.signed_url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    assert_eq!(downloaded.as_ref(), payload.as_slice());

    server.stop().await
}

#[tokio::test]
async fn artifact_block_blob_upload_commits_block_list_order() -> Result<()> {
    let server = TestServer::start().await?;
    let client = Client::new();

    let create: CreateArtifactResponse = client
        .post(server.artifact_endpoint("CreateArtifact"))
        .header("content-type", "application/json")
        .json(&json!({
            "workflowRunBackendId": "run-2",
            "workflowJobRunBackendId": "job-2",
            "name": "chunks",
            "version": 4
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let first_id = general_purpose::STANDARD.encode("block-000001");
    let second_id = general_purpose::STANDARD.encode("block-000002");
    client
        .put(format!(
            "{}?comp=block&blockid={}",
            create.signed_upload_url, second_id
        ))
        .body("world".as_bytes().to_vec())
        .send()
        .await?
        .error_for_status()?;
    client
        .put(format!(
            "{}?comp=block&blockid={}",
            create.signed_upload_url, first_id
        ))
        .body("hello ".as_bytes().to_vec())
        .send()
        .await?
        .error_for_status()?;

    let block_list = format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList><Latest>{}</Latest><Latest>{}</Latest></BlockList>",
        first_id, second_id
    );
    client
        .put(format!("{}?comp=blocklist", create.signed_upload_url))
        .header("content-type", "application/xml")
        .body(block_list)
        .send()
        .await?
        .error_for_status()?;

    let finalized: FinalizeArtifactResponse = client
        .post(server.artifact_endpoint("FinalizeArtifact"))
        .header("content-type", "application/json")
        .json(&json!({
            "workflowRunBackendId": "run-2",
            "workflowJobRunBackendId": "job-2",
            "name": "chunks",
            "size": 11
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(finalized.ok);

    let signed: GetSignedArtifactUrlResponse = client
        .post(server.artifact_endpoint("GetSignedArtifactURL"))
        .header("content-type", "application/json")
        .json(&json!({
            "workflowRunBackendId": "run-2",
            "workflowJobRunBackendId": "job-2",
            "name": "chunks"
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let downloaded = client
        .get(signed.signed_url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    assert_eq!(downloaded, "hello world");

    server.stop().await
}

#[tokio::test]
async fn artifact_block_upload_accepts_large_numeric_block_suffix() -> Result<()> {
    let server = TestServer::start().await?;
    let client = Client::new();

    let create: CreateArtifactResponse = client
        .post(server.artifact_endpoint("CreateArtifact"))
        .header("content-type", "application/json")
        .json(&json!({
            "workflowRunBackendId": "run-3",
            "workflowJobRunBackendId": "job-3",
            "name": "large-suffix",
            "version": 4
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let block_id = general_purpose::STANDARD.encode("block-999999999999999999999999999999");
    client
        .put(format!(
            "{}?comp=block&blockid={}",
            create.signed_upload_url, block_id
        ))
        .body("payload".as_bytes().to_vec())
        .send()
        .await?
        .error_for_status()?;

    let block_list = format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList><Latest>{}</Latest></BlockList>",
        block_id
    );
    client
        .put(format!("{}?comp=blocklist", create.signed_upload_url))
        .header("content-type", "application/xml")
        .body(block_list)
        .send()
        .await?
        .error_for_status()?;

    server.stop().await
}
