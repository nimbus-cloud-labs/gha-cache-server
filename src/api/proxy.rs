use async_trait::async_trait;
use axum::{
    BoxError,
    body::Body,
    extract::State,
    http::{HeaderValue, Request, header::HOST},
    response::Response,
};
use http::{Uri, uri::InvalidUri};
use http_body_util::BodyExt;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use std::fmt;

use crate::error::{ApiError, Result};
use crate::http::AppState;

pub const RESULTS_RECEIVER_HOST: &str = "results-receiver.actions.githubusercontent.com";
pub const RESULTS_RECEIVER_ORIGIN: &str = "https://results-receiver.actions.githubusercontent.com";

#[async_trait]
pub trait ProxyHttpClient: Send + Sync + 'static {
    async fn execute(&self, request: Request<Body>) -> std::result::Result<Response, BoxError>;
}

#[derive(Clone)]
pub struct HyperProxyClient {
    inner: Client<hyper_rustls::HttpsConnector<HttpConnector>, Body>,
}

impl Default for HyperProxyClient {
    fn default() -> Self {
        Self::new()
    }
}

impl HyperProxyClient {
    pub fn new() -> Self {
        let https = HttpsConnectorBuilder::new()
            .with_native_roots()
            .expect("failed to load native root certificates")
            .https_only()
            .enable_http1()
            .build();

        let client = Client::builder(TokioExecutor::new()).build(https);

        Self { inner: client }
    }
}

#[async_trait]
impl ProxyHttpClient for HyperProxyClient {
    async fn execute(&self, request: Request<Body>) -> std::result::Result<Response, BoxError> {
        let response = self
            .inner
            .request(request)
            .await
            .map_err(|err| -> BoxError { Box::new(err) })?;

        let (parts, body) = response.into_parts();
        let stream = body.into_data_stream();
        let body = Body::from_stream(stream);

        Ok(Response::from_parts(parts, body))
    }
}

fn build_target_uri(path_and_query: &str) -> std::result::Result<Uri, InvalidUri> {
    format!("{RESULTS_RECEIVER_ORIGIN}{path_and_query}").parse()
}

fn format_invalid_uri_error(err: InvalidUri) -> ApiError {
    ApiError::Internal(format!("invalid proxy uri: {err}"))
}

fn host_header() -> HeaderValue {
    HeaderValue::from_static(RESULTS_RECEIVER_HOST)
}

pub async fn proxy_unknown(
    State(state): State<AppState>,
    request: Request<Body>,
) -> Result<Response> {
    proxy_results(&state, request).await
}

pub(crate) async fn proxy_results(state: &AppState, request: Request<Body>) -> Result<Response> {
    let original = request.uri().clone();
    let path_and_query = original
        .path_and_query()
        .map(|pq| pq.as_str().to_owned())
        .unwrap_or_else(|| original.path().to_string());

    let target_uri = build_target_uri(&path_and_query).map_err(format_invalid_uri_error)?;

    let (mut parts, body) = request.into_parts();
    let method = parts.method.clone();
    parts.uri = target_uri.clone();
    parts.headers.insert(HOST, host_header());

    let proxied = Request::from_parts(parts, body);

    tracing::info!(
        method = %method,
        path = %path_and_query,
        target = %target_uri,
        "proxying request to results receiver",
    );

    let response = state
        .proxy_client
        .execute(proxied)
        .await
        .map_err(|err| ApiError::Internal(format!("proxy request failed: {err}")))?;

    let (parts, body) = response.into_parts();
    let status = parts.status;

    if status.is_client_error() || status.is_server_error() {
        let collected = body
            .collect()
            .await
            .map_err(|err| ApiError::Internal(format!("failed to read proxied response: {err}")))?;

        let body_bytes = collected.to_bytes();
        let body_text = String::from_utf8_lossy(&body_bytes);

        tracing::warn!(
            method = %method,
            path = %path_and_query,
            status = %status,
            body = %body_text,
            "received proxied error response",
        );

        let body = Body::from(body_bytes);

        Ok(Response::from_parts(parts, body))
    } else {
        tracing::info!(
            method = %method,
            path = %path_and_query,
            status = %status,
            "received proxied response",
        );

        Ok(Response::from_parts(parts, body))
    }
}

impl fmt::Debug for HyperProxyClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HyperProxyClient").finish_non_exhaustive()
    }
}
