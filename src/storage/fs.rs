use std::ffi::OsString;
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use sha2::{Digest, Sha256};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{self, AsyncWriteExt};
use tokio::runtime::Handle;
use tokio::task;
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::storage::{BlobDownloadStream, BlobStore, BlobUploadPayload, PresignedUrl};

#[derive(Clone)]
pub struct FsStore {
    root: PathBuf,
    uploads_root: PathBuf,
    file_mode: Option<u32>,
    dir_mode: Option<u32>,
}

impl FsStore {
    pub async fn new(
        root: PathBuf,
        uploads_root: Option<PathBuf>,
        file_mode: Option<u32>,
        dir_mode: Option<u32>,
    ) -> Result<Self> {
        if !root.as_path().exists() {
            fs::create_dir_all(&root)
                .await
                .with_context(|| format!("failed to create storage root at {}", root.display()))?;
        }

        let canonical_root = fs::canonicalize(&root).await.with_context(|| {
            format!(
                "failed to resolve absolute path for storage root at {}",
                root.display()
            )
        })?;
        let uploads_root = Self::resolve_uploads_root(&canonical_root, uploads_root).await?;

        let store = Self {
            root: canonical_root,
            uploads_root,
            file_mode,
            dir_mode,
        };

        store
            .ensure_dir_mode(&store.root)
            .await
            .context("applying permissions to storage root")?;

        store
            .ensure_dir_mode(&store.uploads_root)
            .await
            .context("applying permissions to uploads directory")?;

        Ok(store)
    }

    async fn resolve_uploads_root(
        canonical_root: &Path,
        configured: Option<PathBuf>,
    ) -> Result<PathBuf> {
        let path = if let Some(path) = configured {
            if path.is_relative() {
                canonical_root.join(path)
            } else {
                path
            }
        } else {
            Self::default_uploads_root(canonical_root)
        };

        if !path.as_path().exists() {
            fs::create_dir_all(&path).await.with_context(|| {
                format!("failed to create uploads directory at {}", path.display())
            })?;
        }

        fs::canonicalize(&path).await.with_context(|| {
            format!(
                "failed to resolve absolute path for uploads directory at {}",
                path.display()
            )
        })
    }

    fn default_uploads_root(root: &Path) -> PathBuf {
        if let Some(parent) = root.parent() {
            let mut dir_name = OsString::from(".gha-cache-uploads");
            if let Some(name) = root.file_name() {
                dir_name.push("-");
                dir_name.push(name);
            }
            parent.join(dir_name)
        } else {
            root.join(".gha-cache-uploads")
        }
    }

    fn uploads_root(&self) -> &Path {
        &self.uploads_root
    }

    fn upload_dir(&self, upload_id: &str) -> PathBuf {
        self.uploads_root().join(upload_id)
    }

    fn part_path(&self, upload_id: &str, part_number: i32) -> PathBuf {
        self.upload_dir(upload_id)
            .join(format!("part-{part_number:05}.chunk"))
    }

    fn staging_path(&self, upload_id: &str) -> PathBuf {
        self.upload_dir(upload_id).join("complete.tmp")
    }

    fn destination_path(&self, key: &str) -> Result<PathBuf> {
        let relative = Self::sanitize_key(key)?;
        Ok(self.root.join(relative))
    }

    async fn remove_empty_parent_dirs(&self, path: &Path) -> Result<()> {
        let mut current = path.parent().map(|p| p.to_path_buf());
        while let Some(dir) = current.clone() {
            if dir == self.root {
                break;
            }
            if !dir.starts_with(&self.root) {
                break;
            }

            let next = dir.parent().map(|p| p.to_path_buf());
            match fs::remove_dir(&dir).await {
                Ok(()) => {
                    current = next;
                }
                Err(ref err) if err.kind() == ErrorKind::NotFound => {
                    current = next;
                }
                Err(err) if err.kind() == ErrorKind::DirectoryNotEmpty => break,
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("failed to remove empty directory {}", dir.display())
                    });
                }
            }
        }

        Ok(())
    }

    fn sanitize_key(key: &str) -> Result<PathBuf> {
        let mut normalized = PathBuf::new();
        for component in Path::new(key).components() {
            match component {
                Component::Normal(part) => normalized.push(part),
                Component::CurDir => {}
                _ => {
                    anyhow::bail!("key contains invalid path segments");
                }
            }
        }
        if normalized.as_os_str().is_empty() {
            anyhow::bail!("key may not be empty");
        }
        Ok(normalized)
    }

    async fn ensure_dir_mode<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        #[cfg(unix)]
        {
            if let Some(mode) = self.dir_mode {
                use std::os::unix::fs::PermissionsExt;
                let perms = std::fs::Permissions::from_mode(mode);
                fs::set_permissions(path.as_ref(), perms).await?;
            }
        }
        #[cfg(not(unix))]
        {
            let _ = (&self.dir_mode, path);
        }
        Ok(())
    }

    async fn ensure_file_mode<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        #[cfg(unix)]
        {
            if let Some(mode) = self.file_mode {
                use std::os::unix::fs::PermissionsExt;
                let perms = std::fs::Permissions::from_mode(mode);
                fs::set_permissions(path.as_ref(), perms).await?;
            }
        }
        #[cfg(not(unix))]
        {
            let _ = (&self.file_mode, path);
        }
        Ok(())
    }

    fn is_cross_device_error(error: &std::io::Error) -> bool {
        #[cfg(unix)]
        {
            const EXDEV: i32 = 18;
            if error.raw_os_error() == Some(EXDEV) {
                return true;
            }
        }

        #[cfg(windows)]
        {
            const ERROR_NOT_SAME_DEVICE: i32 = 17;
            if error.raw_os_error() == Some(ERROR_NOT_SAME_DEVICE) {
                return true;
            }
        }

        let _ = error;
        false
    }
}

#[async_trait]
impl BlobStore for FsStore {
    async fn create_multipart(&self, key: &str) -> Result<String> {
        let _ = self.destination_path(key)?;
        let upload_id = Uuid::new_v4().to_string();
        let upload_dir = self.upload_dir(&upload_id);
        fs::create_dir_all(&upload_dir).await.with_context(|| {
            format!(
                "failed to create upload directory at {}",
                upload_dir.display()
            )
        })?;
        self.ensure_dir_mode(&upload_dir).await?;
        Ok(upload_id)
    }

    async fn upload_part(
        &self,
        _key: &str,
        upload_id: &str,
        part_number: i32,
        mut body: BlobUploadPayload,
    ) -> Result<String> {
        let part_path = self.part_path(upload_id, part_number);
        if let Some(parent) = part_path.parent() {
            fs::create_dir_all(parent).await.with_context(|| {
                format!("failed to prepare upload directory at {}", parent.display())
            })?;
            self.ensure_dir_mode(parent).await?;
        }

        let mut hasher = Sha256::new();
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&part_path)
            .await
            .with_context(|| format!("failed to open part file {}", part_path.display()))?;

        while let Some(chunk) = body.next().await {
            let bytes: Bytes = chunk?;
            hasher.update(&bytes);
            file.write_all(&bytes).await?;
        }
        file.flush().await?;
        drop(file);

        drop_page_cache(&part_path).await?;
        self.ensure_file_mode(&part_path).await?;

        let digest = hasher.finalize();
        let etag = digest.iter().map(|b| format!("{b:02x}")).collect();
        Ok(etag)
    }

    async fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: Vec<(i32, String)>,
    ) -> Result<()> {
        if parts.is_empty() {
            anyhow::bail!("multipart upload must include at least one part");
        }

        let staging_path = self.staging_path(upload_id);
        if let Some(parent) = staging_path.parent() {
            fs::create_dir_all(parent).await.with_context(|| {
                format!(
                    "failed to prepare staging directory at {}",
                    parent.display()
                )
            })?;
            self.ensure_dir_mode(parent).await?;
        }

        let mut output = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&staging_path)
            .await
            .with_context(|| format!("failed to create staging file {}", staging_path.display()))?;

        for (part_number, _) in &parts {
            let part_path = self.part_path(upload_id, *part_number);
            let mut part = OpenOptions::new()
                .read(true)
                .open(&part_path)
                .await
                .with_context(|| format!("missing part file {}", part_path.display()))?;
            io::copy(&mut part, &mut output).await?;
        }
        output.flush().await?;
        drop(output);

        drop_page_cache(&staging_path).await?;
        let destination = self.destination_path(key)?;
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).await.with_context(|| {
                format!(
                    "failed to create destination directory at {}",
                    parent.display()
                )
            })?;
            self.ensure_dir_mode(parent).await?;
        }

        let finalize_err = || format!("failed to finalize upload to {}", destination.display());
        match fs::rename(&staging_path, &destination).await {
            Ok(()) => {}
            Err(err) => {
                if Self::is_cross_device_error(&err) {
                    fs::copy(&staging_path, &destination)
                        .await
                        .with_context(finalize_err)?;
                    fs::remove_file(&staging_path).await.with_context(|| {
                        format!("failed to remove staging file {}", staging_path.display())
                    })?;
                } else {
                    return Err(err).with_context(finalize_err);
                }
            }
        }
        self.ensure_file_mode(&destination).await?;

        let upload_dir = self.upload_dir(upload_id);
        if upload_dir.as_path().exists() {
            fs::remove_dir_all(&upload_dir).await.ok();
        }

        Ok(())
    }

    async fn presign_get(&self, _key: &str, _ttl: Duration) -> Result<Option<PresignedUrl>> {
        Ok(None)
    }

    async fn get(&self, key: &str) -> Result<Option<BlobDownloadStream>> {
        let path = self.destination_path(key)?;
        let file = match File::open(&path).await {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to open blob at {}", path.display()));
            }
        };

        let path_for_stream = path.clone();
        let stream = ReaderStream::new(file).map(|chunk| chunk.map_err(anyhow::Error::from));
        let stream = CacheDroppingStream::new(stream.boxed(), path_for_stream).boxed();

        Ok(Some(stream))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.destination_path(key)?;
        match fs::remove_file(&path).await {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to remove blob at {}", path.display()));
            }
        }

        self.remove_empty_parent_dirs(&path).await
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<()> {
        let path = self.destination_path(prefix)?;
        match fs::remove_dir_all(&path).await {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to remove prefix at {}", path.display()));
            }
        }

        self.remove_empty_parent_dirs(&path).await
    }
}

#[cfg(unix)]
fn advise_drop_from_cache(fd: std::os::unix::io::RawFd) -> std::io::Result<()> {
    // SAFETY: `fd` comes from `File::as_raw_fd`, so it refers to a live file
    // descriptor for the duration of the call. Passing zero for the offset and
    // length requests the kernel to apply the advice to the entire file, which
    // is permitted by POSIX, and `POSIX_FADV_DONTNEED` is a valid advice flag.
    let result = unsafe { libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::from_raw_os_error(result))
    }
}

fn drop_page_cache_blocking(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::fs::File;
        use std::os::unix::io::AsRawFd;

        match File::open(path) {
            Ok(file) => {
                advise_drop_from_cache(file.as_raw_fd())?;
            }
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err),
        }
    }

    #[cfg(not(unix))]
    {
        let _ = path;
    }

    Ok(())
}

async fn drop_page_cache(path: &Path) -> Result<()> {
    let path = path.to_path_buf();
    let result = task::spawn_blocking(move || drop_page_cache_blocking(path.as_path()))
        .await
        .context("failed to run drop_page_cache task")?;
    result.map_err(anyhow::Error::from)
}

struct CacheDroppingStream {
    inner: BlobDownloadStream,
    path: Option<PathBuf>,
}

impl CacheDroppingStream {
    fn new(stream: BlobDownloadStream, path: PathBuf) -> Self {
        Self {
            inner: stream,
            path: Some(path),
        }
    }
}

impl Stream for CacheDroppingStream {
    type Item = anyhow::Result<Bytes>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl Drop for CacheDroppingStream {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            if let Ok(handle) = Handle::try_current() {
                handle.spawn(async move {
                    let _ = drop_page_cache(path.as_path()).await;
                });
            } else {
                let _ = std::thread::spawn(move || {
                    let _ = drop_page_cache_blocking(path.as_path());
                });
            }
        }
    }
}
