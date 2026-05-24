use crate::api::{artifact, download, proxy, twirp, upload, upload_compat};
use crate::config::{Config, DatabaseDriver};
use crate::storage::BlobStore;
use axum::{
    Router,
    error_handling::HandleErrorLayer,
    http::StatusCode,
    routing::{get, patch, post, put},
};
use sqlx::AnyPool;
use std::convert::Infallible;
use std::sync::Arc;
use tower::{
    ServiceBuilder,
    limit::ConcurrencyLimitLayer,
    timeout::{TimeoutLayer, error::Elapsed},
};
use tower_http::trace::TraceLayer;

#[derive(Clone)]
pub struct AppState {
    pub pool: AnyPool,
    pub store: Arc<dyn BlobStore>,
    pub enable_direct: bool,
    pub defer_finalize_in_background: bool,
    pub proxy_client: Arc<dyn proxy::ProxyHttpClient>,
    pub database_driver: DatabaseDriver,
}

pub fn build_router(pool: AnyPool, store: Arc<dyn BlobStore>, cfg: &Config) -> Router {
    let proxy_client: Arc<dyn proxy::ProxyHttpClient> = Arc::new(proxy::HyperProxyClient::new());

    build_router_with_proxy(pool, store, cfg, proxy_client)
}

pub(crate) fn build_router_with_proxy(
    pool: AnyPool,
    store: Arc<dyn BlobStore>,
    cfg: &Config,
    proxy_client: Arc<dyn proxy::ProxyHttpClient>,
) -> Router {
    let app_state = AppState {
        pool: pool.clone(),
        store,
        enable_direct: cfg.enable_direct_downloads,
        defer_finalize_in_background: cfg.defer_finalize_in_background,
        proxy_client,
        database_driver: cfg.database_driver,
    };

    Router::new()
        .without_v07_checks()
        .route("/", get(|| async { "OK" }))
        .route("/healthz", get(|| async { "ok" }))
        // ===== Official actions/cache style =====
        // GET lookup
        .route("/_apis/artifactcache/cache", get(upload::get_cache_entry))
        // POST reserve
        .route(
            "/_apis/artifactcache/caches",
            get(upload::list_caches).post(upload::reserve_cache),
        )
        // PATCH chunks
        .route(
            "/_apis/artifactcache/caches/{id}",
            patch(upload::upload_chunk),
        )
        .route(
            "/_apis/artifactcache/caches/{id}",
            post(upload::commit_cache),
        )
        .route(
            "/download/{cache_key}/{filename}",
            get(download::download_proxy),
        )
        // TWIRP endpoints
        .route(
            "/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry",
            post(twirp::create_cache_entry),
        )
        .route(
            "/twirp/github.actions.results.api.v1.CacheService/FinalizeCacheEntryUpload",
            post(twirp::finalize_cache_entry_upload),
        )
        .route(
            "/twirp/github.actions.results.api.v1.CacheService/GetCacheEntryDownloadURL",
            post(twirp::get_cache_entry_download_url),
        )
        // Artifact service endpoints used by actions/upload-artifact v4+.
        .route(
            "/twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact",
            post(artifact::create_artifact),
        )
        .route(
            "/twirp/github.actions.results.api.v1.ArtifactService/FinalizeArtifact",
            post(artifact::finalize_artifact),
        )
        .route(
            "/twirp/github.actions.results.api.v1.ArtifactService/ListArtifacts",
            post(artifact::list_artifacts),
        )
        .route(
            "/twirp/github.actions.results.api.v1.ArtifactService/GetSignedArtifactURL",
            post(artifact::get_signed_artifact_url),
        )
        .route(
            "/twirp/github.actions.results.api.v1.ArtifactService/DeleteArtifact",
            post(artifact::delete_artifact),
        )
        .route(
            "/artifact-upload/{artifact_id}",
            put(artifact::put_artifact),
        )
        .route(
            "/artifact-download/{artifact_id}/{filename}",
            get(artifact::download_artifact),
        )
        // 3) PUT /upload/{cache-id}
        .route("/upload/{cache_id}", put(upload_compat::put_upload))
        .fallback(proxy::proxy_unknown)
        .with_state(app_state)
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(ConcurrencyLimitLayer::new(cfg.max_concurrency))
                .layer(HandleErrorLayer::new(|error: axum::BoxError| async move {
                    if error.is::<Elapsed>() {
                        tracing::warn!("request timed out");
                        Ok::<_, Infallible>((StatusCode::REQUEST_TIMEOUT, "request timed out"))
                    } else {
                        tracing::error!(%error, "unhandled middleware error");
                        Ok::<_, Infallible>((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "unhandled internal error",
                        ))
                    }
                }))
                .layer(TimeoutLayer::new(cfg.request_timeout))
                .into_inner(),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::{
        body::Body,
        http::{HeaderMap, Method, Request, StatusCode},
        response::Response,
    };
    use bytes::Bytes;
    use http::Uri;
    use http_body_util::BodyExt;
    use std::{sync::Arc, time::Duration};
    use tokio::sync::Mutex;
    use tower::ServiceExt;

    use crate::api::proxy::{self, ProxyHttpClient, RESULTS_RECEIVER_ORIGIN};
    use crate::config::{
        BlobStoreSelector, CleanupSettings, Config, DatabaseDriver, S3Config, S3TlsConfig,
    };
    use crate::storage::{BlobDownloadStream, BlobStore, PresignedUrl};

    #[derive(Clone, Debug)]
    struct RecordedRequest {
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: Bytes,
    }

    #[derive(Clone)]
    struct MockProxyClient {
        calls: Arc<Mutex<Vec<RecordedRequest>>>,
        responder: Arc<dyn Fn() -> Response + Send + Sync>,
    }

    impl MockProxyClient {
        fn with_response<F>(responder: F) -> Self
        where
            F: Fn() -> Response + Send + Sync + 'static,
        {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                responder: Arc::new(responder),
            }
        }

        fn with_body(status: StatusCode, body: &'static str) -> Self {
            Self::with_response(move || {
                Response::builder()
                    .status(status)
                    .body(Body::from(body))
                    .expect("proxy response")
            })
        }

        async fn take_calls(&self) -> Vec<RecordedRequest> {
            let mut calls = self.calls.lock().await;
            let recorded = calls.clone();
            calls.clear();
            recorded
        }
    }

    #[async_trait]
    impl ProxyHttpClient for MockProxyClient {
        async fn execute(
            &self,
            request: Request<Body>,
        ) -> std::result::Result<Response, axum::BoxError> {
            let (parts, body) = request.into_parts();
            let collected = body
                .collect()
                .await
                .map_err(|err| -> axum::BoxError { Box::new(err) })?;

            let record = RecordedRequest {
                method: parts.method,
                uri: parts.uri,
                headers: parts.headers,
                body: collected.to_bytes(),
            };

            self.calls.lock().await.push(record);

            Ok((self.responder)())
        }
    }

    #[derive(Clone)]
    struct NoopStore;

    #[async_trait]
    impl BlobStore for NoopStore {
        async fn create_multipart(&self, _key: &str) -> anyhow::Result<String> {
            unimplemented!("not required for tests")
        }

        async fn upload_part(
            &self,
            _key: &str,
            _upload_id: &str,
            _part_number: i32,
            _body: crate::storage::BlobUploadPayload,
        ) -> anyhow::Result<String> {
            unimplemented!("not required for tests")
        }

        async fn complete_multipart(
            &self,
            _key: &str,
            _upload_id: &str,
            _parts: Vec<(i32, String)>,
        ) -> anyhow::Result<()> {
            unimplemented!("not required for tests")
        }

        async fn presign_get(
            &self,
            _key: &str,
            _ttl: Duration,
        ) -> anyhow::Result<Option<PresignedUrl>> {
            Ok(None)
        }

        async fn get(&self, _key: &str) -> anyhow::Result<Option<BlobDownloadStream>> {
            Ok(None)
        }

        async fn delete(&self, _key: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn test_config() -> Config {
        Config {
            port: 8080,
            enable_direct_downloads: true,
            defer_finalize_in_background: true,
            request_timeout: Duration::from_secs(30),
            max_concurrency: 16,
            database_url: "postgres://localhost/test".into(),
            database_driver: DatabaseDriver::Postgres,
            blob_store: BlobStoreSelector::S3,
            s3: Some(S3Config {
                bucket: "bucket".into(),
                region: "us-east-1".into(),
                endpoint_url: None,
                force_path_style: true,
                tls: S3TlsConfig {
                    accept_invalid_certs: false,
                    custom_ca_bundle: None,
                },
            }),
            fs: None,
            gcs: None,
            cleanup: CleanupSettings {
                interval: Duration::from_secs(300),
                max_entry_age: None,
                max_total_bytes: None,
            },
        }
    }

    #[tokio::test]
    async fn proxies_unknown_paths_via_results_receiver() {
        let mock = MockProxyClient::with_body(StatusCode::ACCEPTED, "proxied body");
        let proxy_arc: Arc<dyn proxy::ProxyHttpClient> = Arc::new(mock.clone());
        let pool = AnyPool::connect_lazy("sqlite::memory:").expect("lazy pool");
        let store: Arc<dyn BlobStore> = Arc::new(NoopStore);
        let cfg = test_config();

        let router = build_router_with_proxy(pool, store, &cfg, proxy_arc);

        let request = Request::builder()
            .method(Method::POST)
            .uri("/unknown/path?foo=bar")
            .header("x-sample", "demo")
            .body(Body::from("payload"))
            .expect("request");

        let response = router.oneshot(request).await.expect("router response");

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body_bytes = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        assert_eq!(body_bytes, Bytes::from_static(b"proxied body"));

        let calls = mock.take_calls().await;
        assert_eq!(calls.len(), 1, "expected exactly one proxied request");
        let recorded = &calls[0];

        assert_eq!(recorded.method, Method::POST);
        assert_eq!(
            recorded.uri,
            format!("{RESULTS_RECEIVER_ORIGIN}/unknown/path?foo=bar")
                .parse::<Uri>()
                .unwrap()
        );
        assert_eq!(recorded.body, Bytes::from_static(b"payload"));

        let host_header = recorded
            .headers
            .get(axum::http::header::HOST)
            .and_then(|v| v.to_str().ok());
        assert_eq!(host_header, Some(proxy::RESULTS_RECEIVER_HOST));
        assert_eq!(
            recorded
                .headers
                .get("x-sample")
                .and_then(|v| v.to_str().ok()),
            Some("demo")
        );
    }

    #[tokio::test]
    async fn known_routes_bypass_proxy() {
        let mock = MockProxyClient::with_body(StatusCode::IM_A_TEAPOT, "unused");
        let proxy_arc: Arc<dyn proxy::ProxyHttpClient> = Arc::new(mock.clone());
        let pool = AnyPool::connect_lazy("sqlite::memory:").expect("lazy pool");
        let store: Arc<dyn BlobStore> = Arc::new(NoopStore);
        let cfg = test_config();

        let router = build_router_with_proxy(pool, store, &cfg, proxy_arc);

        let request = Request::builder()
            .uri("/healthz")
            .body(Body::empty())
            .expect("request");

        let response = router.oneshot(request).await.expect("router response");

        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        assert_eq!(body_bytes, Bytes::from_static(b"ok"));

        let calls = mock.take_calls().await;
        assert!(
            calls.is_empty(),
            "proxy should not be invoked for known routes"
        );
    }

    #[tokio::test]
    async fn root_route_returns_ok() {
        let mock = MockProxyClient::with_body(StatusCode::IM_A_TEAPOT, "unused");
        let proxy_arc: Arc<dyn proxy::ProxyHttpClient> = Arc::new(mock.clone());
        let pool = AnyPool::connect_lazy("sqlite::memory:").expect("lazy pool");
        let store: Arc<dyn BlobStore> = Arc::new(NoopStore);
        let cfg = test_config();

        let router = build_router_with_proxy(pool, store, &cfg, proxy_arc);

        let request = Request::builder()
            .uri("/")
            .body(Body::empty())
            .expect("request");

        let response = router.oneshot(request).await.expect("router response");

        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        assert_eq!(body_bytes, Bytes::from_static(b"OK"));

        let calls = mock.take_calls().await;
        assert!(
            calls.is_empty(),
            "proxy should not be invoked for known routes"
        );
    }
}
