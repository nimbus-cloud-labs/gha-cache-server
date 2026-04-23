use anyhow::{Context, bail};
use aws_config::BehaviorVersion;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{
    Client,
    config::{Builder as S3ConfigBuilder, Region},
    types::{CompletedMultipartUpload, CompletedPart},
};
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::http::{
    HttpConnector, HttpConnectorFuture, SharedHttpClient, SharedHttpConnector, http_client_fn,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_types::body::{Error as SdkBodyError, SdkBody};
use futures::{StreamExt, TryStreamExt};
use http_body::Frame;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::{self as hyper_legacy, Client as HyperClient};
use hyper_util::rt::TokioExecutor;
use pin_project_lite::pin_project;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use rustls_native_certs::CertificateResult;
use rustls_pemfile;
use std::{fmt, io::Cursor, sync::Arc, time::Duration};
use sync_wrapper::SyncWrapper;
use tokio::fs;
use tokio_util::io::ReaderStream;
use tower::Service;
use tracing::warn;

#[cfg(test)]
use axum::body::Body;

use crate::config::S3TlsConfig;
use crate::storage::{BlobDownloadStream, BlobStore, BlobUploadPayload, PresignedUrl};

#[derive(Clone)]
pub struct S3Store {
    client: Client,
    bucket: String,
}

impl S3Store {
    pub async fn new(
        bucket: String,
        region: String,
        endpoint: Option<String>,
        force_path_style: bool,
        tls: S3TlsConfig,
    ) -> anyhow::Result<Self> {
        let mut loader =
            aws_config::defaults(BehaviorVersion::latest()).region(Region::new(region));
        if let Some(ep) = &endpoint {
            loader = loader.endpoint_url(ep);
        }
        if let Some(http_client) = build_custom_http_client(&tls).await? {
            loader = loader.http_client(http_client);
        }
        let shared = loader.load().await;
        let mut b = S3ConfigBuilder::from(&shared);
        if force_path_style {
            b = b.force_path_style(true);
        }
        let cfg = b.build();
        let client = Client::from_conf(cfg);
        Ok(Self { client, bucket })
    }

    #[cfg(test)]
    pub fn bytestream_from_reader(r: Body) -> ByteStream {
        let stream = r
            .into_data_stream()
            .map_err(|err| -> SdkBodyError { err.into() });
        Self::bytestream_from_stream(stream)
    }

    fn bytestream_from_stream<S, E>(stream: S) -> ByteStream
    where
        S: futures::Stream<Item = Result<bytes::Bytes, E>> + Send + 'static,
        E: Into<SdkBodyError> + 'static,
    {
        ByteStream::from_body_1_x(SyncDataBody::new(stream))
    }
}

async fn build_custom_http_client(tls: &S3TlsConfig) -> anyhow::Result<Option<SharedHttpClient>> {
    if !tls.accept_invalid_certs && tls.custom_ca_bundle.is_none() {
        return Ok(None);
    }

    let custom_ca =
        if let Some(path) = &tls.custom_ca_bundle {
            Some(fs::read(path).await.with_context(|| {
                format!("failed to read AWS_TLS_CA_BUNDLE at {}", path.display())
            })?)
        } else {
            None
        };

    let config = build_rustls_client_config(tls.accept_invalid_certs, custom_ca.as_deref())?;
    let https_connector = HttpsConnectorBuilder::new()
        .with_tls_config(config)
        .https_or_http()
        .enable_http1()
        .build();

    let client = HyperClient::builder(TokioExecutor::new()).build::<_, SdkBody>(https_connector);
    let connector = Arc::new(SimpleHttpConnector { client });

    let http_client = http_client_fn(move |_, _| {
        let connector = connector.clone();
        SharedHttpConnector::new(connector.as_ref().clone())
    });

    Ok(Some(http_client))
}

fn build_rustls_client_config(
    accept_invalid_certs: bool,
    custom_ca_bundle: Option<&[u8]>,
) -> anyhow::Result<ClientConfig> {
    let roots = load_root_store(custom_ca_bundle)?;
    build_rustls_client_config_from_roots(accept_invalid_certs, roots)
}

fn build_rustls_client_config_from_roots(
    accept_invalid_certs: bool,
    roots: RootCertStore,
) -> anyhow::Result<ClientConfig> {
    if roots.is_empty() && !accept_invalid_certs {
        bail!("no TLS root certificates found; set AWS_TLS_CA_BUNDLE or enable AWS_TLS_INSECURE");
    }

    let mut config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    if accept_invalid_certs {
        config
            .dangerous()
            .set_certificate_verifier(Arc::new(AcceptAllVerifier));
    }

    if !config
        .alpn_protocols
        .iter()
        .any(|proto| proto == b"http/1.1")
    {
        config.alpn_protocols.push(b"http/1.1".to_vec());
    }

    Ok(config)
}

fn load_root_store(custom_ca_bundle: Option<&[u8]>) -> anyhow::Result<RootCertStore> {
    load_root_store_with(custom_ca_bundle, rustls_native_certs::load_native_certs)
}

fn load_root_store_with<F>(
    custom_ca_bundle: Option<&[u8]>,
    load_native_certs: F,
) -> anyhow::Result<RootCertStore>
where
    F: FnOnce() -> CertificateResult,
{
    let mut roots = RootCertStore::empty();
    let CertificateResult { certs, errors, .. } = load_native_certs();
    if !errors.is_empty() {
        warn!(?errors, "failed to load some native TLS certificates");
    }

    for cert in certs {
        if let Err(err) = roots.add(cert) {
            warn!(?err, "failed to add native TLS certificate to trust store");
        }
    }

    if let Some(pem_bytes) = custom_ca_bundle {
        let mut reader = Cursor::new(pem_bytes);
        let certs = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .context("AWS_TLS_CA_BUNDLE must contain PEM encoded certificates")?;
        if certs.is_empty() {
            bail!("AWS_TLS_CA_BUNDLE does not contain any certificates");
        }
        for cert in certs {
            roots
                .add(cert)
                .context("failed to add certificate from AWS_TLS_CA_BUNDLE")?;
        }
    }

    Ok(roots)
}

#[derive(Clone)]
struct SimpleHttpConnector {
    client:
        HyperClient<hyper_rustls::HttpsConnector<hyper_legacy::connect::HttpConnector>, SdkBody>,
}

impl fmt::Debug for SimpleHttpConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SimpleHttpConnector")
    }
}

impl HttpConnector for SimpleHttpConnector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let request = match request.try_into_http1x() {
            Ok(request) => request,
            Err(err) => {
                return HttpConnectorFuture::ready(Err(ConnectorError::user(BoxError::from(err))));
            }
        };

        let mut client = self.client.clone();
        let fut = Service::call(&mut client, request);
        HttpConnectorFuture::new(async move {
            let response = fut
                .await
                .map_err(|err| ConnectorError::other(BoxError::from(err), None))?
                .map(SdkBody::from_body_1_x);
            HttpResponse::try_from(response)
                .map_err(|err| ConnectorError::other(BoxError::from(err), None))
        })
    }
}

#[derive(Debug)]
struct AcceptAllVerifier;

impl ServerCertVerifier for AcceptAllVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
        ]
    }
}

pin_project! {
    struct SyncDataBody<S> {
        #[pin]
        stream: SyncWrapper<S>,
    }
}

impl<S> SyncDataBody<S> {
    fn new(stream: S) -> Self {
        Self {
            stream: SyncWrapper::new(stream),
        }
    }
}

impl<S, E> http_body::Body for SyncDataBody<S>
where
    S: futures::Stream<Item = Result<bytes::Bytes, E>> + Send,
    E: Into<SdkBodyError> + 'static,
{
    type Data = bytes::Bytes;
    type Error = E;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.project().stream.get_pin_mut().poll_next(cx) {
            std::task::Poll::Ready(Some(Ok(bytes))) => {
                std::task::Poll::Ready(Some(Ok(Frame::data(bytes))))
            }
            std::task::Poll::Ready(Some(Err(err))) => std::task::Poll::Ready(Some(Err(err))),
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

#[async_trait::async_trait]
impl BlobStore for S3Store {
    async fn create_multipart(&self, key: &str) -> anyhow::Result<String> {
        let out = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;
        Ok(out.upload_id().unwrap_or_default().to_string())
    }

    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: BlobUploadPayload,
    ) -> anyhow::Result<String> {
        let body = Self::bytestream_from_stream(body.map_err(|err| -> SdkBodyError { err.into() }));
        let out = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(body)
            .send()
            .await?;
        Ok(out
            .e_tag()
            .unwrap_or_default()
            .trim_matches('"')
            .to_string())
    }

    async fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: Vec<(i32, String)>,
    ) -> anyhow::Result<()> {
        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(
                parts
                    .into_iter()
                    .map(|(n, etag)| CompletedPart::builder().part_number(n).e_tag(etag).build())
                    .collect(),
            ))
            .build();
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(completed)
            .send()
            .await?;
        Ok(())
    }

    async fn presign_get(&self, key: &str, ttl: Duration) -> anyhow::Result<Option<PresignedUrl>> {
        use aws_sdk_s3::presigning::PresigningConfig;
        let req = self.client.get_object().bucket(&self.bucket).key(key);
        let presigned = req.presigned(PresigningConfig::expires_in(ttl)?).await?;
        let url: url::Url = presigned.uri().to_string().parse()?;
        Ok(Some(PresignedUrl { url }))
    }

    async fn get(&self, key: &str) -> anyhow::Result<Option<BlobDownloadStream>> {
        match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => {
                let reader = output.body.into_async_read();
                let stream =
                    ReaderStream::new(reader).map(|chunk| chunk.map_err(anyhow::Error::from));
                Ok(Some(Box::pin(stream)))
            }
            Err(SdkError::ServiceError(err)) => {
                if err.err().is_no_such_key() {
                    return Ok(None);
                }
                Err(anyhow::Error::msg(format!("get object failed: {err:?}")))
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;
        Ok(())
    }

    async fn delete_prefix(&self, prefix: &str) -> anyhow::Result<()> {
        anyhow::bail!(
            "prefix deletion is not implemented for S3 bucket {} and prefix {}",
            self.bucket,
            prefix
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{S3Store, build_rustls_client_config_from_roots, load_root_store_with};
    use axum::body::Body;
    use bytes::Bytes;
    use futures::stream;
    use rustls::{RootCertStore, crypto::aws_lc_rs};
    use rustls_native_certs::CertificateResult;
    use std::convert::Infallible;
    use std::error::Error as _;
    use std::io::{self, Cursor};

    #[tokio::test]
    async fn bytestream_from_reader_streams_large_payloads() {
        let chunk = Bytes::from(vec![42u8; 128 * 1024]);
        let chunk_len = chunk.len();
        let chunks = 64;
        let chunk_for_stream = chunk.clone();
        let body = Body::from_stream(stream::iter(
            (0..chunks).map(move |_| Ok::<_, Infallible>(chunk_for_stream.clone())),
        ));

        let collected = S3Store::bytestream_from_reader(body)
            .collect()
            .await
            .expect("collect succeeds")
            .into_bytes();

        assert_eq!(collected.len(), chunk_len * chunks);
        assert!(collected.iter().all(|&b| b == 42));
    }

    #[tokio::test]
    async fn bytestream_from_reader_propagates_stream_errors() {
        let body = Body::from_stream(stream::iter([
            Ok::<_, io::Error>(Bytes::from_static(b"ok")),
            Err(io::Error::other("boom")),
        ]));

        let err = S3Store::bytestream_from_reader(body)
            .collect()
            .await
            .expect_err("collect should report stream error");

        let source = err.source().expect("streaming error should expose source");
        assert!(source.to_string().contains("boom"));
    }

    const TEST_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nMIIDDTCCAfWgAwIBAgIUcrFbOhTXqNbnLChrWTZWc02qTjYwDQYJKoZIhvcNAQEL\nBQAwFjEUMBIGA1UEAwwLZXhhbXBsZS5jb20wHhcNMjUxMDE1MDA0MjQxWhcNMjUx\nMDE2MDA0MjQxWjAWMRQwEgYDVQQDDAtleGFtcGxlLmNvbTCCASIwDQYJKoZIhvcN\nAQEBBQADggEPADCCAQoCggEBALFvAk/YTtbxDp2B1Hep+60sBVf7kXePf6NwRV7D\n+rGsSH8qNfnQnxxZKM0QYrDytzBb2BYhsoyjm49x6Elf1idXerXIaRtrBUpPZju4\nNS2XDB7DZb+PJxsOX9WD3AGqo1MTcN0sIkQJ1c1cGyC3GOEGa52Jm8QhdicNu9Nk\nO1NwHOTlH0wahsTxYQqCyr2omdRboMRd9SstqBYNJ8oMkUrzQH43gSY6fn9ANVoP\n3aHRgp0/Ww+8vhRRBSD02xw8bRLSQYv2EwyynLzixDl6sOBDNmP+LV6yfxMYmWvD\nHoOW4x1pdSXviM03SO5z2vZvHexE9ku+qenGFc+qA8xFfIECAwEAAaNTMFEwHQYD\nVR0OBBYEFHOq6UoYi/SXJsCiXorQqomfUeoEMB8GA1UdIwQYMBaAFHOq6UoYi/SX\nJsCiXorQqomfUeoEMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEB\nAKG+PpXSk4aH8Mv4ENFsyj/JIhs6T1bqnU7QerIO4Lcn85y40fBC1M8pPaSS2iqm\n9uST25KQhlHs7IPEEaPDKK3OtDd69AKJnUWeSJ7y8O8FrLCL9iq1b0OkVK8mO9Xe\ndE8SiIMbGm76+8VDZup3s/kx43UR2NH/6zuyo5geRwrFEGtRwBiXPLxVF55/RHXH\nyKhjYensfuShnphQL+E0sI2aPjvs8zQQ0u6HLfECWx91RgrMpekKgA/oOIJaWSjx\n+sVMNUxI6oSEKdT62w66a9/B1YwkdKGWr6jbChuB2xDxC0tmccN3kwg0/BPbes/O\n7DCTTX1gbiuU9pyLmVLTonY=\n-----END CERTIFICATE-----\n";

    fn parse_test_certificates() -> Vec<rustls::pki_types::CertificateDer<'static>> {
        let mut reader = Cursor::new(TEST_CERT_PEM.as_bytes());
        rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .expect("test certificate is valid")
    }

    #[test]
    fn load_root_store_with_custom_bundle_adds_certificates() {
        let roots =
            load_root_store_with(Some(TEST_CERT_PEM.as_bytes()), CertificateResult::default)
                .expect("loading custom certificates succeeds");
        assert!(!roots.is_empty());
    }

    #[test]
    fn load_root_store_with_reports_invalid_pem() {
        let err = load_root_store_with(
            Some(b"-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----\n" as &[u8]),
            CertificateResult::default,
        )
        .expect_err("invalid PEM should be rejected");
        let message = err.to_string();
        assert!(
            message.contains("AWS_TLS_CA_BUNDLE must contain PEM encoded certificates")
                || message.contains("failed to add certificate from AWS_TLS_CA_BUNDLE"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn load_root_store_with_rejects_empty_bundle() {
        let err = load_root_store_with(Some(b"\n"), CertificateResult::default)
            .expect_err("empty PEM bundle should be rejected");
        assert!(
            err.to_string()
                .contains("AWS_TLS_CA_BUNDLE does not contain any certificates")
        );
    }

    #[test]
    fn load_root_store_with_combines_native_and_custom_certificates() {
        let native_certs = parse_test_certificates();
        let roots = load_root_store_with(Some(TEST_CERT_PEM.as_bytes()), || {
            let mut result = CertificateResult::default();
            result.certs = native_certs.clone();
            result
        })
        .expect("loading certificates succeeds");
        assert_eq!(roots.len(), native_certs.len() + 1);
    }

    #[test]
    fn build_rustls_config_requires_roots_when_not_insecure() {
        let err = build_rustls_client_config_from_roots(false, RootCertStore::empty())
            .expect_err("missing roots should error when not insecure");
        assert!(err.to_string().contains("no TLS root certificates found"));
    }

    #[test]
    fn build_rustls_config_allows_insecure_without_roots() {
        let _ = aws_lc_rs::default_provider().install_default();
        let config = build_rustls_client_config_from_roots(true, RootCertStore::empty())
            .expect("insecure mode accepts empty roots");
        assert!(
            config
                .alpn_protocols
                .iter()
                .any(|proto| proto.as_slice() == b"http/1.1")
        );
    }
}
