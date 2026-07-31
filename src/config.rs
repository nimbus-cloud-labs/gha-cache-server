use std::{env::VarError, fs, path::Path, path::PathBuf, str::FromStr, time::Duration};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::Value;

#[derive(Clone, Debug)]
pub enum BlobStoreSelector {
    Fs,
    S3,
    Gcs,
}

impl FromStr for BlobStoreSelector {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "fs" => Ok(Self::Fs),
            "s3" => Ok(Self::S3),
            "gcs" => Ok(Self::Gcs),
            other => anyhow::bail!("unsupported blob store '{other}'"),
        }
    }
}

#[derive(Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint_url: Option<String>,
    pub force_path_style: bool,
    pub tls: S3TlsConfig,
}

#[derive(Clone)]
pub struct S3TlsConfig {
    pub accept_invalid_certs: bool,
    pub custom_ca_bundle: Option<PathBuf>,
}

#[derive(Clone)]
pub struct FsConfig {
    pub root: PathBuf,
    pub uploads_root: Option<PathBuf>,
    pub file_mode: Option<u32>,
    pub dir_mode: Option<u32>,
}

#[derive(Clone)]
pub struct GcsCredentials {
    pub raw_json: Value,
    pub service_account: ServiceAccountKeyConfig,
}

#[derive(Clone)]
pub struct GcsConfig {
    pub bucket: String,
    pub credentials: GcsCredentials,
    pub endpoint: Option<String>,
}

#[derive(Clone, Deserialize)]
pub struct ServiceAccountKeyConfig {
    #[serde(rename = "client_email")]
    pub client_email: String,
    #[serde(rename = "private_key")]
    pub private_key: String,
    #[serde(rename = "private_key_id")]
    pub private_key_id: String,
}

#[derive(Clone, Debug)]
pub struct CleanupSettings {
    pub interval: Duration,
    pub max_entry_age: Option<Duration>,
    pub max_total_bytes: Option<u64>,
    pub min_available_bytes: Option<u64>,
    pub target_available_bytes: Option<u64>,
    pub filesystem_path: Option<PathBuf>,
}

#[derive(Clone)]
pub struct Config {
    pub port: u16,
    pub enable_direct_downloads: bool,
    pub defer_finalize_in_background: bool,
    pub request_timeout: Duration,
    pub max_concurrency: usize,

    pub database_url: String,
    pub database_driver: DatabaseDriver,

    pub blob_store: BlobStoreSelector,

    pub s3: Option<S3Config>,
    pub fs: Option<FsConfig>,
    pub gcs: Option<GcsConfig>,

    pub cleanup: CleanupSettings,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatabaseDriver {
    Postgres,
    Mysql,
    Sqlite,
}

impl FromStr for DatabaseDriver {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "postgres" | "postgresql" | "pg" => Ok(Self::Postgres),
            "mysql" => Ok(Self::Mysql),
            "sqlite" => Ok(Self::Sqlite),
            other => anyhow::bail!("unsupported database driver '{other}'"),
        }
    }
}

impl DatabaseDriver {
    pub fn from_url(url: &str) -> Result<Self> {
        let scheme = url
            .split_once(':')
            .map(|(scheme, _)| scheme)
            .unwrap_or("")
            .to_ascii_lowercase();

        match scheme.as_str() {
            "postgres" | "postgresql" | "pg" => Ok(Self::Postgres),
            "mysql" => Ok(Self::Mysql),
            "sqlite" => Ok(Self::Sqlite),
            other => {
                anyhow::bail!("unsupported database driver in DATABASE_URL (scheme '{other}')")
            }
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let blob_store = std::env::var("BLOB_STORE")
            .unwrap_or_else(|_| "s3".to_string())
            .parse()?;

        let s3 = if matches!(blob_store, BlobStoreSelector::S3) {
            Some(S3Config {
                bucket: std::env::var("S3_BUCKET")
                    .context("S3_BUCKET is required when BLOB_STORE=s3")?,
                region: std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into()),
                endpoint_url: std::env::var("AWS_ENDPOINT_URL").ok(),
                force_path_style: std::env::var("S3_FORCE_PATH_STYLE")
                    .map(|v| v == "true")
                    .unwrap_or(true),
                tls: S3TlsConfig {
                    accept_invalid_certs: parse_env_bool("AWS_TLS_INSECURE")?.unwrap_or(false),
                    custom_ca_bundle: parse_optional_path("AWS_TLS_CA_BUNDLE")?,
                },
            })
        } else {
            None
        };

        let fs = if matches!(blob_store, BlobStoreSelector::Fs) {
            let root = std::env::var("FS_ROOT")
                .context("FS_ROOT is required when BLOB_STORE=fs")?
                .into();
            let uploads_root = std::env::var("FS_UPLOAD_ROOT").ok().map(Into::into);
            Some(FsConfig {
                root,
                uploads_root,
                file_mode: parse_mode("FS_FILE_MODE")?,
                dir_mode: parse_mode("FS_DIR_MODE")?,
            })
        } else {
            None
        };

        let gcs = if matches!(blob_store, BlobStoreSelector::Gcs) {
            let bucket = std::env::var("GCS_BUCKET")
                .context("GCS_BUCKET is required when BLOB_STORE=gcs")?;
            let bucket = bucket.trim().to_string();
            if bucket.is_empty() {
                bail!("GCS_BUCKET may not be empty");
            }

            let credentials_json = if let Ok(inline) = std::env::var("GCS_SERVICE_ACCOUNT_JSON") {
                if inline.trim().is_empty() {
                    bail!("GCS_SERVICE_ACCOUNT_JSON may not be empty");
                }
                serde_json::from_str::<Value>(&inline)
                    .context("GCS_SERVICE_ACCOUNT_JSON must contain valid JSON")?
            } else {
                let path = std::env::var("GCS_SERVICE_ACCOUNT_PATH").context(
                    "set GCS_SERVICE_ACCOUNT_JSON or GCS_SERVICE_ACCOUNT_PATH when BLOB_STORE=gcs",
                )?;
                let contents = fs::read_to_string(&path).with_context(|| {
                    format!("failed to read GCS service account file at {path}")
                })?;
                serde_json::from_str::<Value>(&contents).with_context(|| {
                    format!("invalid JSON in GCS service account file at {path}")
                })?
            };

            let service_account: ServiceAccountKeyConfig =
                serde_json::from_value(credentials_json.clone())
                    .context("GCS service account JSON is missing required fields")?;

            let endpoint = std::env::var("GCS_ENDPOINT")
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty());

            Some(GcsConfig {
                bucket,
                credentials: GcsCredentials {
                    raw_json: credentials_json,
                    service_account,
                },
                endpoint,
            })
        } else {
            None
        };

        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL is required");
        let database_driver = DatabaseDriver::from_url(&database_url)?;

        let cleanup_min_available_bytes = std::env::var("CACHE_STORAGE_MIN_AVAILABLE_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok());
        let cleanup_target_available_bytes = std::env::var("CACHE_STORAGE_TARGET_AVAILABLE_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .or(cleanup_min_available_bytes);
        if let (Some(min), Some(target)) =
            (cleanup_min_available_bytes, cleanup_target_available_bytes)
            && target < min
        {
            bail!(
                "CACHE_STORAGE_TARGET_AVAILABLE_BYTES must be greater than or equal to CACHE_STORAGE_MIN_AVAILABLE_BYTES"
            );
        }
        let cleanup_filesystem_path =
            cleanup_filesystem_path(&blob_store, fs.as_ref().map(|cfg| cfg.root.as_path()));

        Ok(Self {
            port: std::env::var("PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(8080),
            enable_direct_downloads: std::env::var("ENABLE_DIRECT_DOWNLOADS")
                .map(|v| v == "true")
                .unwrap_or(true),
            defer_finalize_in_background: parse_env_bool("DEFER_FINALIZE_IN_BACKGROUND")?
                .unwrap_or(true),
            request_timeout: std::env::var("REQUEST_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .map(Duration::from_secs)
                .unwrap_or(Duration::from_secs(3600)),
            max_concurrency: std::env::var("MAX_CONCURRENCY")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(64),

            database_url,
            database_driver,

            blob_store,

            s3,
            fs,
            gcs,

            cleanup: CleanupSettings {
                interval: std::env::var("CLEANUP_INTERVAL_SECS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .map(|secs| Duration::from_secs(secs.max(1)))
                    .unwrap_or_else(|| Duration::from_secs(300)),
                max_entry_age: std::env::var("CACHE_ENTRY_MAX_AGE_SECS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .map(Duration::from_secs),
                max_total_bytes: std::env::var("CACHE_STORAGE_MAX_BYTES")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok()),
                min_available_bytes: cleanup_min_available_bytes,
                target_available_bytes: cleanup_target_available_bytes,
                filesystem_path: cleanup_filesystem_path,
            },
        })
    }
}

fn cleanup_filesystem_path(
    blob_store: &BlobStoreSelector,
    fs_root: Option<&Path>,
) -> Option<PathBuf> {
    if matches!(blob_store, BlobStoreSelector::Fs) {
        fs_root.map(Path::to_path_buf)
    } else {
        None
    }
}

fn parse_env_bool(name: &str) -> Result<Option<bool>> {
    parse_env_bool_result(name, std::env::var(name))
}

fn parse_env_bool_result(name: &str, value: Result<String, VarError>) -> Result<Option<bool>> {
    match value {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                bail!("{name} may not be empty");
            }
            match trimmed.to_ascii_lowercase().as_str() {
                "1" | "true" => Ok(Some(true)),
                "0" | "false" => Ok(Some(false)),
                other => bail!("invalid boolean value '{other}' for {name}"),
            }
        }
        Err(VarError::NotPresent) => Ok(None),
        Err(VarError::NotUnicode(_)) => {
            bail!("{name} contains invalid unicode characters")
        }
    }
}

fn parse_optional_path(name: &str) -> Result<Option<PathBuf>> {
    parse_optional_path_result(name, std::env::var(name))
}

fn parse_optional_path_result(
    name: &str,
    value: Result<String, VarError>,
) -> Result<Option<PathBuf>> {
    match value {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                bail!("{name} may not be empty");
            }
            Ok(Some(PathBuf::from(trimmed)))
        }
        Err(VarError::NotPresent) => Ok(None),
        Err(VarError::NotUnicode(_)) => {
            bail!("{name} contains invalid unicode characters")
        }
    }
}

fn parse_mode(var: &str) -> Result<Option<u32>> {
    match std::env::var(var) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            let digits = trimmed
                .strip_prefix("0o")
                .or_else(|| trimmed.strip_prefix("0O"))
                .unwrap_or(trimmed)
                .trim_start_matches('0');
            let digits = if digits.is_empty() { "0" } else { digits };
            let mode = u32::from_str_radix(digits, 8)
                .with_context(|| format!("{var} must be a valid octal number"))?;
            Ok(Some(mode))
        }
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            anyhow::bail!("{var} contains invalid UTF-8")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn determines_postgres_from_url() {
        assert_eq!(
            DatabaseDriver::from_url("postgres://localhost/db").unwrap(),
            DatabaseDriver::Postgres
        );
        assert_eq!(
            DatabaseDriver::from_url("postgresql://localhost/db").unwrap(),
            DatabaseDriver::Postgres
        );
        assert_eq!(
            DatabaseDriver::from_url("pg://localhost/db").unwrap(),
            DatabaseDriver::Postgres
        );
    }

    #[test]
    fn determines_mysql_from_url() {
        assert_eq!(
            DatabaseDriver::from_url("mysql://localhost/db").unwrap(),
            DatabaseDriver::Mysql
        );
    }

    #[test]
    fn determines_sqlite_from_url() {
        assert_eq!(
            DatabaseDriver::from_url("sqlite::memory:").unwrap(),
            DatabaseDriver::Sqlite
        );
        assert_eq!(
            DatabaseDriver::from_url("sqlite:///tmp/test.db").unwrap(),
            DatabaseDriver::Sqlite
        );
    }

    #[test]
    fn errors_on_unknown_scheme() {
        let err = DatabaseDriver::from_url("redis://localhost/db").unwrap_err();
        assert!(
            err.to_string()
                .contains("unsupported database driver in DATABASE_URL")
        );
    }

    #[test]
    fn parse_env_bool_supports_true_and_false() {
        assert_eq!(
            parse_env_bool_result("__TEST_BOOL_TRUE", Ok("true".to_string())).unwrap(),
            Some(true)
        );

        assert_eq!(
            parse_env_bool_result("__TEST_BOOL_FALSE", Ok("0".to_string())).unwrap(),
            Some(false)
        );
    }

    #[test]
    fn parse_env_bool_rejects_invalid_values() {
        let err =
            parse_env_bool_result("__TEST_BOOL_INVALID", Ok("maybe".to_string())).unwrap_err();
        assert!(
            err.to_string()
                .contains("invalid boolean value 'maybe' for __TEST_BOOL_INVALID")
        );
    }

    #[test]
    fn parse_env_bool_returns_none_when_missing() {
        assert_eq!(
            parse_env_bool_result("__TEST_BOOL_MISSING", Err(VarError::NotPresent)).unwrap(),
            None
        );
    }

    #[test]
    fn parse_optional_path_trims_and_accepts_values() {
        assert_eq!(
            parse_optional_path_result("__TEST_PATH_VALUE", Ok("  /tmp/example  ".to_string()))
                .unwrap(),
            Some(PathBuf::from("/tmp/example"))
        );
    }

    #[test]
    fn parse_optional_path_rejects_empty_values() {
        let err =
            parse_optional_path_result("__TEST_PATH_EMPTY", Ok("   ".to_string())).unwrap_err();
        assert!(
            err.to_string()
                .contains("__TEST_PATH_EMPTY may not be empty")
        );
    }
}
