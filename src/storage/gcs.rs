use std::io;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{StreamExt, stream};
use google_cloud_auth::credentials::Credentials;
use google_cloud_auth::credentials::service_account;
use google_cloud_gax::error::rpc::Code;
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model::{Object, compose_object_request::SourceObject};
use google_cloud_storage::streaming_source::{Payload, SizeHint, StreamingSource};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_encode};
use rsa::RsaPrivateKey;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::sha2::Sha256 as RsaSha256;
use rsa::signature::{SignatureEncoding, Signer};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use url::Url;

#[cfg(test)]
use once_cell::sync::Lazy;
#[cfg(test)]
use std::alloc::{GlobalAlloc, Layout, System};
#[cfg(test)]
use std::cell::Cell;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(test)]
use std::sync::{Mutex as StdMutex, MutexGuard as StdMutexGuard};

use crate::config::{GcsConfig, ServiceAccountKeyConfig};
use crate::storage::{BlobDownloadStream, BlobStore, BlobUploadPayload, PresignedUrl};

const UPLOAD_PREFIX: &str = ".gha-cache/uploads";
const MAX_COMPOSE_COMPONENTS: usize = 32;

const PATH_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~')
    .remove(b'/');
const QUERY_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

fn make_part_object_name(upload_id: &str, part_number: i32) -> String {
    format!(
        "{UPLOAD_PREFIX}/{upload_id}/part-{part_number:05}",
        upload_id = upload_id,
        part_number = part_number
    )
}

fn make_stage_object_name(upload_id: &str, stage: usize, index: usize) -> String {
    format!(
        "{UPLOAD_PREFIX}/{upload_id}/stage-{stage:04}-{index:04}",
        upload_id = upload_id,
        stage = stage,
        index = index
    )
}

#[derive(Debug, PartialEq)]
enum ComposeAction {
    Compose {
        sources: Vec<String>,
        destination: String,
    },
    Delete(String),
}

#[derive(Clone)]
pub struct GcsStore {
    storage: Storage,
    control: StorageControl,
    bucket_resource: String,
    bucket_name: String,
    base_url: Url,
    signer: GcsSigner,
}

impl GcsStore {
    pub async fn new(config: GcsConfig) -> Result<Self> {
        let credentials = build_credentials(&config.credentials.raw_json)?;
        let mut storage_builder = Storage::builder().with_credentials(credentials.clone());
        let mut control_builder = StorageControl::builder().with_credentials(credentials.clone());
        if let Some(endpoint) = &config.endpoint {
            storage_builder = storage_builder.with_endpoint(endpoint.clone());
            control_builder = control_builder.with_endpoint(endpoint.clone());
        }
        let storage = storage_builder.build().await?;
        let control = control_builder.build().await?;

        let base_url = if let Some(endpoint) = &config.endpoint {
            Url::parse(endpoint).context("invalid GCS endpoint URL")?
        } else {
            Url::parse("https://storage.googleapis.com")?
        };
        if base_url.path() != "/" {
            bail!("GCS endpoint URL must not include a path component");
        }

        let signer = GcsSigner::new(&config.credentials.service_account)?;

        Ok(Self {
            storage,
            control,
            bucket_resource: format!("projects/_/buckets/{}", config.bucket),
            bucket_name: config.bucket,
            base_url,
            signer,
        })
    }

    fn part_object_name(&self, upload_id: &str, part_number: i32) -> String {
        make_part_object_name(upload_id, part_number)
    }

    async fn compose_into(&self, sources: &[String], destination: &str) -> Result<()> {
        let dest = Object::new()
            .set_bucket(self.bucket_resource.clone())
            .set_name(destination.to_string());
        let request = self
            .control
            .compose_object()
            .set_destination(dest)
            .set_source_objects(
                sources
                    .iter()
                    .map(|name| SourceObject::new().set_name(name.clone())),
            );
        request.send().await?;
        Ok(())
    }

    async fn delete_object(&self, name: &str) {
        let _ = self
            .control
            .delete_object()
            .set_bucket(self.bucket_resource.clone())
            .set_object(name.to_string())
            .send()
            .await;
    }

    async fn compose_parts(&self, upload_id: &str, key: &str, sources: Vec<String>) -> Result<()> {
        let actions = build_compose_plan(upload_id, key, sources)?;
        for action in actions {
            match action {
                ComposeAction::Compose {
                    sources,
                    destination,
                } => {
                    self.compose_into(&sources, &destination).await?;
                }
                ComposeAction::Delete(name) => {
                    self.delete_object(&name).await;
                }
            }
        }
        Ok(())
    }
}

fn build_credentials(raw_json: &serde_json::Value) -> Result<Credentials> {
    service_account::Builder::new(raw_json.clone())
        .build()
        .map_err(|e| e.into())
}

fn build_compose_plan(
    upload_id: &str,
    key: &str,
    mut sources: Vec<String>,
) -> Result<Vec<ComposeAction>> {
    if sources.is_empty() {
        bail!("multipart upload must include at least one part");
    }
    let mut actions = Vec::new();
    let mut stage = 0usize;
    while sources.len() > MAX_COMPOSE_COMPONENTS {
        let mut next = Vec::with_capacity(sources.len().div_ceil(MAX_COMPOSE_COMPONENTS));
        for (idx, chunk) in sources.chunks(MAX_COMPOSE_COMPONENTS).enumerate() {
            if chunk.len() == 1 {
                next.push(chunk[0].clone());
                continue;
            }
            let temp_name = make_stage_object_name(upload_id, stage, idx);
            actions.push(ComposeAction::Compose {
                sources: chunk.to_vec(),
                destination: temp_name.clone(),
            });
            for name in chunk {
                actions.push(ComposeAction::Delete(name.clone()));
            }
            next.push(temp_name);
        }
        sources = next;
        stage += 1;
    }

    actions.push(ComposeAction::Compose {
        sources: sources.clone(),
        destination: key.to_string(),
    });
    for name in sources {
        actions.push(ComposeAction::Delete(name));
    }
    Ok(actions)
}

#[async_trait]
impl BlobStore for GcsStore {
    async fn create_multipart(&self, _key: &str) -> Result<String> {
        Ok(uuid::Uuid::new_v4().to_string())
    }

    async fn upload_part(
        &self,
        _key: &str,
        upload_id: &str,
        part_number: i32,
        body: BlobUploadPayload,
    ) -> Result<String> {
        let object_name = self.part_object_name(upload_id, part_number);
        let payload: Payload<GcsStreamSource> = Payload::from_stream(GcsStreamSource::new(body));
        let object = self
            .storage
            .write_object::<_, _, _, GcsStreamSource>(
                self.bucket_resource.clone(),
                object_name,
                payload,
            )
            .with_resumable_upload_threshold(0usize)
            .send_buffered()
            .await?;
        if object.etag.is_empty() {
            Ok(object.generation.to_string())
        } else {
            Ok(object.etag)
        }
    }

    async fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: Vec<(i32, String)>,
    ) -> Result<()> {
        let numbers: Vec<i32> = parts.into_iter().map(|(n, _)| n).collect();
        if numbers.is_empty() {
            bail!("multipart upload must include at least one part");
        }
        let names = numbers
            .into_iter()
            .map(|n| self.part_object_name(upload_id, n))
            .collect();
        self.compose_parts(upload_id, key, names).await
    }

    async fn presign_get(&self, key: &str, ttl: Duration) -> Result<Option<PresignedUrl>> {
        let url = self
            .signer
            .sign(&self.base_url, &self.bucket_name, key, ttl)?;
        Ok(Some(PresignedUrl { url }))
    }

    async fn get(&self, key: &str) -> Result<Option<BlobDownloadStream>> {
        match self
            .storage
            .read_object(self.bucket_resource.clone(), key.to_string())
            .send()
            .await
        {
            Ok(response) => {
                let stream = stream::unfold(Some(response), |state| async move {
                    let mut resp = match state {
                        Some(resp) => resp,
                        None => return None,
                    };
                    match resp.next().await {
                        Some(Ok(chunk)) => Some((Ok(chunk), Some(resp))),
                        Some(Err(err)) => Some((Err(err.into()), None)),
                        None => None,
                    }
                });
                Ok(Some(Box::pin(stream)))
            }
            Err(err) => {
                if err.http_status_code() == Some(404) {
                    return Ok(None);
                }
                Err(err.into())
            }
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        match self
            .control
            .delete_object()
            .set_bucket(self.bucket_resource.clone())
            .set_object(key.to_string())
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => {
                if err.status().map(|s| s.code) == Some(Code::NotFound) {
                    return Ok(());
                }
                if err.http_status_code() == Some(404) {
                    return Ok(());
                }
                Err(anyhow!(
                    "failed to delete object {key} from bucket {}: {err}",
                    self.bucket_name
                ))
            }
        }
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<()> {
        anyhow::bail!(
            "prefix deletion is not implemented for GCS bucket {} and prefix {}",
            self.bucket_name,
            prefix
        );
    }
}

#[derive(Clone)]
struct GcsSigner {
    email: String,
    private_key: Arc<RsaPrivateKey>,
    clock: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
}

impl GcsSigner {
    fn new(config: &ServiceAccountKeyConfig) -> Result<Self> {
        Self::from_parts(config, Arc::new(Utc::now))
    }

    fn from_parts(
        config: &ServiceAccountKeyConfig,
        clock: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
    ) -> Result<Self> {
        let _ = &config.private_key_id;
        let pem = config.private_key.replace("\\n", "\n");
        let private_key = RsaPrivateKey::from_pkcs8_pem(&pem)
            .or_else(|_| RsaPrivateKey::from_pkcs1_pem(&pem))
            .map_err(|err| anyhow!("failed to parse service account private key: {err}"))?;
        Ok(Self {
            email: config.client_email.clone(),
            private_key: Arc::new(private_key),
            clock,
        })
    }

    #[cfg(test)]
    fn with_clock(
        config: &ServiceAccountKeyConfig,
        clock: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
    ) -> Result<Self> {
        Self::from_parts(config, clock)
    }

    fn sign(&self, base_url: &Url, bucket: &str, object: &str, ttl: Duration) -> Result<Url> {
        let expires = ttl.as_secs();
        if expires == 0 {
            bail!("signed URL TTL must be at least one second");
        }
        let expires = expires.min(604800);

        let host = base_url
            .host_str()
            .ok_or_else(|| anyhow!("GCS endpoint URL must include a host"))?;
        let mut header_host = host.to_string();
        if let Some(port) = base_url.port() {
            let default_port = match base_url.scheme() {
                "http" => 80,
                "https" => 443,
                _ => port,
            };
            if port != default_port {
                header_host = format!("{host}:{port}");
            }
        }

        let now = (self.clock)();
        let timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date = now.format("%Y%m%d").to_string();
        let scope = format!("{date}/auto/storage/goog4_request");
        let credential = format!("{}/{scope}", self.email);

        let canonical_uri = format!(
            "/{}/{}",
            percent_encode(bucket.as_bytes(), PATH_ENCODE_SET),
            percent_encode(object.as_bytes(), PATH_ENCODE_SET)
        );

        let mut params = vec![
            (
                "X-Goog-Algorithm".to_string(),
                "GOOG4-RSA-SHA256".to_string(),
            ),
            ("X-Goog-Credential".to_string(), credential.clone()),
            ("X-Goog-Date".to_string(), timestamp.clone()),
            ("X-Goog-Expires".to_string(), expires.to_string()),
            ("X-Goog-SignedHeaders".to_string(), "host".to_string()),
        ];
        let canonical_query = encode_query(&params);

        let canonical_headers = format!("host:{}\n", header_host);
        let signed_headers = "host";
        let canonical_request = format!(
            "GET\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\nUNSIGNED-PAYLOAD"
        );
        let hashed = Sha256::digest(canonical_request.as_bytes());
        let hashed_hex = to_hex(&hashed);

        let string_to_sign = format!("GOOG4-RSA-SHA256\n{timestamp}\n{scope}\n{hashed_hex}");

        let signing_key = SigningKey::<RsaSha256>::new((*self.private_key).clone());
        let signature = signing_key.sign(string_to_sign.as_bytes()).to_vec();
        let signature_hex = to_hex(&signature);

        params.push(("X-Goog-Signature".to_string(), signature_hex));
        let query = encode_query(&params);
        let final_url = format!(
            "{}://{}{}?{}",
            base_url.scheme(),
            header_host,
            canonical_uri,
            query
        );
        Url::parse(&final_url).context("failed to build signed URL")
    }
}

struct GcsStreamSource {
    inner: Arc<Mutex<BlobUploadPayload>>,
}

impl GcsStreamSource {
    fn new(inner: BlobUploadPayload) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

impl StreamingSource for GcsStreamSource {
    type Error = io::Error;

    fn next(
        &mut self,
    ) -> impl std::future::Future<Output = Option<Result<bytes::Bytes, io::Error>>> + Send {
        let inner = Arc::clone(&self.inner);
        async move {
            let mut guard = inner.lock().await;
            match guard.next().await {
                Some(Ok(bytes)) => Some(Ok(bytes)),
                Some(Err(err)) => Some(Err(io::Error::other(err.to_string()))),
                None => None,
            }
        }
    }

    fn size_hint(&self) -> impl std::future::Future<Output = Result<SizeHint, io::Error>> + Send {
        let inner = Arc::clone(&self.inner);
        async move {
            let guard = inner.lock().await;
            let (lower, upper) = guard.size_hint();
            Ok(size_hint_from_bounds(lower, upper))
        }
    }
}

fn size_hint_from_bounds(lower: usize, upper: Option<usize>) -> SizeHint {
    let mut hint = SizeHint::new();
    hint.set_lower(lower as u64);
    if let Some(upper) = upper {
        hint.set_upper(upper as u64);
    }
    hint
}

fn encode_query(params: &[(String, String)]) -> String {
    let mut encoded: Vec<String> = params
        .iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                percent_encode(k.as_bytes(), QUERY_ENCODE_SET),
                percent_encode(v.as_bytes(), QUERY_ENCODE_SET)
            )
        })
        .collect();
    encoded.sort();
    encoded.join("&")
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
static HEAP_MEASUREMENT_LOCK: Lazy<StdMutex<()>> = Lazy::new(|| StdMutex::new(()));

#[cfg(test)]
std::thread_local! {
    static THREAD_MEASURING: Cell<bool> = const { Cell::new(false) };
}

#[cfg(test)]
struct CountingAllocator {
    system: System,
    current: AtomicUsize,
    peak: AtomicUsize,
}

#[cfg(test)]
impl CountingAllocator {
    const fn new() -> Self {
        Self {
            system: System,
            current: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    fn system_alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: `GlobalAlloc::alloc` only receives well-formed layouts, so forwarding
        // the layout to the system allocator upholds its requirements.
        unsafe { self.system.alloc(layout) }
    }

    fn system_alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: `GlobalAlloc::alloc_zeroed` has the same layout guarantees as
        // `alloc`, so the system allocator may zero-initialize the requested block.
        unsafe { self.system.alloc_zeroed(layout) }
    }

    fn system_dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: The pointer and layout are supplied by `GlobalAlloc::dealloc` and
        // therefore describe a block that originated from this allocator with the
        // matching layout; the measurement guard only affects bookkeeping outside of
        // the deallocation call.
        unsafe { self.system.dealloc(ptr, layout) }
    }

    fn system_realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: The pointer and layout identify an allocation provided by this
        // allocator, and `new_size` comes from the caller of `GlobalAlloc::realloc`,
        // satisfying the system allocator's contract for resizing the block.
        unsafe { self.system.realloc(ptr, layout, new_size) }
    }

    fn scoped(&'static self) -> MeasurementGuard {
        let lock = HEAP_MEASUREMENT_LOCK
            .lock()
            .expect("measurement mutex poisoned");
        self.current.store(0, Ordering::SeqCst);
        self.peak.store(0, Ordering::SeqCst);
        THREAD_MEASURING.with(|flag| flag.set(true));
        MeasurementGuard {
            allocator: self,
            _lock: lock,
        }
    }

    fn stop(&self) {
        THREAD_MEASURING.with(|flag| flag.set(false));
        self.current.store(0, Ordering::SeqCst);
    }

    fn peak_usage(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    fn should_track(&self) -> bool {
        THREAD_MEASURING.with(|flag| flag.get())
    }

    fn add_allocation(&self, size: usize) {
        if size == 0 {
            return;
        }
        let new = self.current.fetch_add(size, Ordering::SeqCst) + size;
        let mut observed = self.peak.load(Ordering::SeqCst);
        while new > observed {
            match self
                .peak
                .compare_exchange(observed, new, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(previous) => observed = previous,
            }
        }
    }

    fn remove_allocation(&self, size: usize) {
        if size == 0 {
            return;
        }
        self.current.fetch_sub(size, Ordering::SeqCst);
    }
}

#[cfg(test)]
impl Default for CountingAllocator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = self.system_alloc(layout);
        if !ptr.is_null() && self.should_track() {
            self.add_allocation(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = self.system_alloc_zeroed(layout);
        if !ptr.is_null() && self.should_track() {
            self.add_allocation(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if self.should_track() {
            self.remove_allocation(layout.size());
        }
        self.system_dealloc(ptr, layout);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = self.system_realloc(ptr, layout, new_size);
        if !new_ptr.is_null() && self.should_track() {
            let old_size = layout.size();
            if new_size >= old_size {
                self.add_allocation(new_size - old_size);
            } else {
                self.remove_allocation(old_size - new_size);
            }
        }
        new_ptr
    }
}

#[cfg(test)]
#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator::new();

#[cfg(test)]
pub(super) struct MeasurementGuard {
    allocator: &'static CountingAllocator,
    _lock: StdMutexGuard<'static, ()>,
}

#[cfg(test)]
impl MeasurementGuard {
    pub(super) fn peak(&self) -> usize {
        self.allocator.peak_usage()
    }
}

#[cfg(test)]
impl Drop for MeasurementGuard {
    fn drop(&mut self) {
        self.allocator.stop();
    }
}

#[cfg(test)]
pub(super) fn start_heap_measurement() -> MeasurementGuard {
    GLOBAL_ALLOCATOR.scoped()
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{StreamExt, stream};
    use std::collections::HashSet;

    const TEST_PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC8TUWONZPa4/cv\nNtKsIYPzx4MD/xkOZZPjtZuIK9P2zoI77aAeD8df8Jx0kFkYQpgZDq+7S37FaNm9\n/sATTYrHs7j1kVwjajy7x+ulAhy0Wyu3g7zoOMtLvzEoAqUmYgUNLxAH+C+ArlX2\nWwviT0CMqcOHwHv6oANzdImWensJcSoxeE+EKKLhfoPdRRllLjvN7Q1c8Pd3ixgn\nm0ZhRGErhphGbwV5OgGjFUShfmIC9cubKTpqrxzVR+UQtqZ/hcOjxS8gzQpe+DNV\nM/7nD1DME0Des0DLOwDb4+4BzetHFhSIe7QB/Wnh1Wp1eYuyvLUz057HDAfwjI2h\nxrDe/HtfAgMBAAECggEADSv5Pe22Llfx9jCHOMPjag2+YWzaa4JkKkfi3aSgiswp\nRK3mRxQNe0LmftTgwUIEnQQaUNICx9ECIjiMEQ2Z3pxOHNy9LZEEjJIl2VYLfKY/\n/vH30zqzJdT1naSJuY9JESywmg4crItat1qTuzyV+auft4LvE+SrjnZMGuWYblwy\n+3iKgACsnfU9OcrnfXe6I66sVUeeqAjDsqW/vnN5Hdpo057yQlygRs6rIgCS5H68\nXaO+YsLQeaxY5/gJ4n3itGOCwV5sZZ+HLiVIx6cAg0S6rLMFQek0B23Jon+/mKsX\nB4HQgElQe3R2h3vwLvKjQ6dh+NQxhG6wShhMkSsxSQKBgQDvKfYLS4p1Pv/VxNVI\nSOCoy0MVi+7SBXsxXHNTvMig2GcA2QHSk2TXS6HI4TPJUFFOSlr1GTMhLgwZYDvP\nvkdINejOYxEA/ZCt98minaPnmwzNgc9nelmtarUo2fOQ/76+qLe8Z4eqYJCk5kLm\nHwiu2aLUcH6kxbaAR5tw/52SHQKBgQDJjrYSOwJmOT1yJ/2tiWzse/bJoy7cZ4pN\n6VK9CpHaDMBS3rTG3OK8llBmLVlrUBB1jRwAfgkL1sPs9CnzOTTPeaZx2rAnwZGx\nYwxYQEYLC3eNnz9uyojzD2TzyWIssCM21GHjkaWsecBoV8XrbGs2kLGe+VeXgzRZ\nJHP03mXKqwKBgCvH1aeZq33tC245ewWheabMlroyBITjxfpyPxZcH6n6E1j/YKsI\nmlQjHzmjqBQ5JLkdOWtWsppnUIWwrSJJZckdPUHStsEkqcB+9KVVEDUMmBpiofIC\nXro1J3aT91daybMjNYdCuH4C8VeOYz62/aLsajdTZIuLOe5frV/RGyotAoGAEUf2\nHlwG2aLgvM/m9SEKQMBkKWefVfBesE1n9aNZW/up5bEIiOBZZFfy7r/GoefMcXe2\nxegIeIZiaAeLLTpjZ8KDXdGlNtNm3XGjllF0b+/8wRy9QI+G7GgOfMRwcWpsqn/N\nIMjVDpOlxox4ALZb/uKrB/lS5D+wllAEzSLgUV8CgYBHjsa2qZUi8zkkSgbA2ueU\n0ClzugJc/sLJ7ePwNEATfIvGOq7ZGYmx/wd0ghy/FwUcA0sK2Tj5O/KvT6iX9m5s\n3o12N+x90tMTyXBJd5M6wDTZlOpiFcqovOOi2kVe4kb13IHgMFOfmN5wvCcL1qhd\nL9k+Zdlx7/HuONEsjRUn+w==\n-----END PRIVATE KEY-----\n";

    fn sample_key() -> ServiceAccountKeyConfig {
        ServiceAccountKeyConfig {
            client_email: "test-sa@example.iam.gserviceaccount.com".into(),
            private_key_id: "test-key".into(),
            private_key: TEST_PRIVATE_KEY.into(),
        }
    }

    #[test]
    fn part_object_name_is_stable() {
        assert_eq!(
            make_part_object_name("upload", 7),
            ".gha-cache/uploads/upload/part-00007"
        );
        assert_eq!(
            make_part_object_name("upload", 12345),
            ".gha-cache/uploads/upload/part-12345"
        );
    }

    #[test]
    fn compose_plan_for_single_part() {
        let actions = build_compose_plan("upload", "final", vec!["part-a".into()]).unwrap();
        assert_eq!(
            actions,
            vec![
                ComposeAction::Compose {
                    sources: vec!["part-a".into()],
                    destination: "final".into(),
                },
                ComposeAction::Delete("part-a".into()),
            ]
        );
    }

    #[test]
    fn compose_plan_chunks_large_uploads() {
        let part_names: Vec<String> = (1..=65)
            .map(|idx| make_part_object_name("upload", idx))
            .collect();
        let actions = build_compose_plan("upload", "final", part_names.clone()).unwrap();

        let mut compose_destinations = Vec::new();
        let mut deletes = HashSet::new();
        for action in &actions {
            match action {
                ComposeAction::Compose {
                    sources,
                    destination,
                } => {
                    assert!(sources.len() <= MAX_COMPOSE_COMPONENTS);
                    compose_destinations.push((destination.clone(), sources.clone()));
                }
                ComposeAction::Delete(name) => {
                    deletes.insert(name.clone());
                }
            }
        }

        assert_eq!(compose_destinations.len(), 3);
        let final_compose = compose_destinations
            .iter()
            .find(|(dest, _)| dest == "final")
            .expect("final compose step present");
        assert_eq!(final_compose.1.len(), 3);
        for name in final_compose.1.iter() {
            if name.ends_with("stage-0000-0000") || name.ends_with("stage-0000-0001") {
                assert!(deletes.contains(name));
            }
        }
        for name in &part_names {
            assert!(deletes.contains(name), "missing delete for {name}");
        }
    }

    #[test]
    fn signer_generates_expected_url() -> Result<()> {
        let clock = Arc::new(|| {
            DateTime::parse_from_rfc3339("2024-01-02T03:04:05Z")
                .unwrap()
                .with_timezone(&Utc)
        });
        let signer = GcsSigner::with_clock(&sample_key(), clock)?;
        let base = Url::parse("https://storage.googleapis.com")?;
        let url = signer.sign(
            &base,
            "example-bucket",
            "path/to/object.tar",
            Duration::from_secs(600),
        )?;

        assert_eq!(url.scheme(), "https");
        assert_eq!(url.host_str(), Some("storage.googleapis.com"));
        assert_eq!(url.path(), "/example-bucket/path/to/object.tar");
        assert_eq!(
            url.to_string(),
            "https://storage.googleapis.com/example-bucket/path/to/object.tar?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=test-sa%40example.iam.gserviceaccount.com%2F20240102%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240102T030405Z&X-Goog-Expires=600&X-Goog-Signature=2429d09964a6ffcaa1d1a615c516ed63ba360159f3eed90370e2d7ac76352b985c0a6594beab49ff9b027354414fa4e642c8c420135b60a5496e437be98e60e617528117145e2ab5da875e4c40f8042d0de409ca6241103374711ecf8185027554bdf077d657774f979b1fe4a4f91ec62d2a3486f76914912eecf3228663a0d76b33fb94e2b2678bc889bff1746c33acc73d5180dc51961bf43a4c1c82c2c2860a2a026c0dc723ca21ef63360d19e196bc25e67d93a257ea96ab7142e880f2a2dd2ced99d67d8d7b2c2e581569e216a4c0ff3665ae7553379b8ec3384b793f9b604ac54e07e3b1d7152704c73392648cb1f8c8f9a3111748882548ab9effabda&X-Goog-SignedHeaders=host"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gcs_stream_source_streams_without_buffering_entire_payload() {
        let measurement = super::start_heap_measurement();
        let chunk_size = 1usize << 20;
        let chunk_count = 16usize;
        let payload: BlobUploadPayload = stream::unfold(0usize, move |index| {
            let chunk_size = chunk_size;
            let chunk_count = chunk_count;
            async move {
                if index >= chunk_count {
                    None
                } else {
                    let data = vec![0u8; chunk_size];
                    let bytes = bytes::Bytes::from(data);
                    Some((Ok(bytes), index + 1))
                }
            }
        })
        .boxed();

        let mut source = GcsStreamSource::new(payload);
        let mut total = 0usize;
        while let Some(chunk) = source.next().await {
            let bytes = chunk.expect("stream chunk should succeed");
            assert_eq!(bytes.len(), chunk_size);
            total += bytes.len();
        }

        assert_eq!(total, chunk_size * chunk_count);

        let peak = measurement.peak();
        drop(measurement);

        let allowed_peak = chunk_size * 2;
        assert!(
            peak <= allowed_peak,
            "expected peak heap usage to stay below {allowed_peak} bytes, observed {peak}",
        );
    }
}
