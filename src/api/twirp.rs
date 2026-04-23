use std::marker::PhantomData;
use std::time::Duration;

use axum::{
    Json,
    extract::{FromRequest, Request, State},
    http::{HeaderValue, header, request::Parts},
    response::{IntoResponse, Response},
};
use http_body_util::BodyExt;
use prost::Message;
use serde::{Serialize, de::DeserializeOwned};
use sqlx::Row;
use uuid::Uuid;

use crate::api::path::encode_path_segment;
use crate::api::proto::cache;
use crate::api::types::*;
use crate::api::upload::{build_generation_scoped_storage_key, normalize_key, normalize_version};
use crate::db::rewrite_placeholders;
use crate::error::{ApiError, Result};
use crate::http::AppState;
use crate::meta;

#[derive(Clone, Debug)]
struct RequestOrigin {
    scheme: String,
    authority: String,
}

impl RequestOrigin {
    fn absolute(&self, path: &str) -> String {
        format!("{}://{}{}", self.scheme, self.authority, path)
    }

    fn from_parts(parts: &Parts) -> Result<Self> {
        let scheme = parts
            .uri
            .scheme_str()
            .map(str::to_owned)
            .or_else(|| {
                parts
                    .headers
                    .get("x-forwarded-proto")
                    .and_then(|value| value.to_str().ok())
                    .and_then(|raw| raw.split(',').next().map(|item| item.trim().to_owned()))
            })
            .unwrap_or_else(|| "http".to_string());
        let authority = parts
            .uri
            .authority()
            .map(|value| value.to_string())
            .or_else(|| {
                parts
                    .headers
                    .get(header::HOST)
                    .and_then(|value| value.to_str().ok())
                    .map(|value| value.to_string())
            })
            .ok_or_else(|| ApiError::BadRequest("missing Host header".into()))?;
        Ok(Self { scheme, authority })
    }
}

impl Default for RequestOrigin {
    fn default() -> Self {
        Self {
            scheme: "http".into(),
            authority: "localhost".into(),
        }
    }
}

fn build_upload_url(origin: &RequestOrigin, id: Uuid) -> String {
    origin.absolute(&format!("/upload/{id}?_=1"))
}

fn build_download_url(origin: &RequestOrigin, cache_key: &str, id: Uuid) -> String {
    let encoded_key = encode_path_segment(cache_key);
    let encoded_filename = encode_path_segment(&format!("{id}.tgz"));
    origin.absolute(&format!("/download/{encoded_key}/{encoded_filename}"))
}

fn unique_keys(primary: String, restores: &[String]) -> Vec<String> {
    let mut result = Vec::with_capacity(restores.len() + 1);
    result.push(primary);
    for item in restores {
        if !result.iter().any(|existing| existing == item) {
            result.push(item.clone());
        }
    }
    result
}

#[derive(Clone, Copy, Debug)]
enum TwirpFormat {
    Json,
    Protobuf,
}

pub struct TwirpRequest<T, P> {
    data: T,
    format: TwirpFormat,
    origin: RequestOrigin,
    _marker: PhantomData<P>,
}

impl<T, P> TwirpRequest<T, P> {
    fn into_parts(self) -> (T, TwirpFormat, RequestOrigin) {
        (self.data, self.format, self.origin)
    }

    #[cfg_attr(
        not(any(test, feature = "test-util")),
        expect(
            dead_code,
            reason = "Only used in integration tests to craft JSON requests"
        )
    )]
    pub(crate) fn from_json(data: T) -> Self {
        Self {
            data,
            format: TwirpFormat::Json,
            origin: RequestOrigin::default(),
            _marker: PhantomData,
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
const _: fn(
    TwirpGetUrlReq,
) -> TwirpRequest<TwirpGetUrlReq, cache::GetCacheEntryDownloadUrlRequest> =
    TwirpRequest::<TwirpGetUrlReq, cache::GetCacheEntryDownloadUrlRequest>::from_json;

fn parse_content_type(value: &HeaderValue) -> Option<TwirpFormat> {
    let raw = value.to_str().ok()?.split(';').next()?.trim();
    match raw {
        "application/json" => Some(TwirpFormat::Json),
        "application/protobuf" => Some(TwirpFormat::Protobuf),
        _ => None,
    }
}

fn pick_response_format(header: Option<&HeaderValue>, fallback: TwirpFormat) -> TwirpFormat {
    let Some(value) = header.and_then(|h| h.to_str().ok()) else {
        return fallback;
    };
    let mut wants_json = false;
    for candidate in value.split(',') {
        let ty = candidate.split(';').next().map(str::trim);
        match ty {
            Some("application/protobuf") => return TwirpFormat::Protobuf,
            Some("application/json") => wants_json = true,
            _ => continue,
        }
    }
    if wants_json {
        TwirpFormat::Json
    } else {
        fallback
    }
}

impl<S, T, P> FromRequest<S> for TwirpRequest<T, P>
where
    S: Send + Sync,
    T: DeserializeOwned + TryFrom<P, Error = ApiError>,
    P: Message + Default,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, _state: &S) -> Result<Self> {
        let (parts, body) = req.into_parts();
        let content_type = parts
            .headers
            .get(header::CONTENT_TYPE)
            .ok_or_else(|| ApiError::BadRequest("missing Content-Type header".into()))?;
        let request_format = parse_content_type(content_type).ok_or_else(|| {
            ApiError::BadRequest(format!(
                "unsupported Content-Type: {}",
                content_type.to_str().unwrap_or_default()
            ))
        })?;
        let response_format =
            pick_response_format(parts.headers.get(header::ACCEPT), request_format);

        let origin = RequestOrigin::from_parts(&parts)?;

        let collected = body
            .collect()
            .await
            .map_err(|err| ApiError::BadRequest(format!("failed to read request body: {err}")))?;
        let bytes = collected.to_bytes();

        let data = match request_format {
            TwirpFormat::Json => serde_json::from_slice(&bytes)
                .map_err(|err| ApiError::BadRequest(format!("invalid JSON payload: {err}")))?,
            TwirpFormat::Protobuf => {
                let proto = P::decode(bytes).map_err(|err| {
                    ApiError::BadRequest(format!("invalid protobuf payload: {err}"))
                })?;
                T::try_from(proto)?
            }
        };

        Ok(Self {
            data,
            format: response_format,
            origin,
            _marker: PhantomData,
        })
    }
}

pub struct TwirpResponse<T, P> {
    data: T,
    format: TwirpFormat,
    _marker: PhantomData<P>,
}

impl<T, P> TwirpResponse<T, P> {
    fn new(data: T, format: TwirpFormat) -> Self {
        Self {
            data,
            format,
            _marker: PhantomData,
        }
    }
}

impl<T, P> IntoResponse for TwirpResponse<T, P>
where
    T: Serialize + Into<P>,
    P: Message,
{
    fn into_response(self) -> Response {
        match self.format {
            TwirpFormat::Json => Json(self.data).into_response(),
            TwirpFormat::Protobuf => {
                let proto: P = self.data.into();
                (
                    [(
                        header::CONTENT_TYPE,
                        HeaderValue::from_static("application/protobuf"),
                    )],
                    proto.encode_to_vec(),
                )
                    .into_response()
            }
        }
    }
}

// POST /twirp/.../CreateCacheEntry
pub async fn create_cache_entry(
    State(st): State<AppState>,
    request: TwirpRequest<TwirpCreateReq, cache::CreateCacheEntryRequest>,
) -> Result<TwirpResponse<TwirpCreateResp, cache::CreateCacheEntryResponse>> {
    let (req, format, origin) = request.into_parts();
    let key = normalize_key(&req.key)?;
    let version = normalize_version(&req.version)?;
    let generation = meta::current_generation(&st.pool, st.database_driver).await?;
    let storage_key =
        build_generation_scoped_storage_key(generation, "twirp", &key, Some(&version));
    let entry = meta::create_entry(
        &st.pool,
        st.database_driver,
        "_",
        "_",
        &key,
        &version,
        "_",
        &storage_key,
    )
    .await?;
    let upload_id = st
        .store
        .create_multipart(&storage_key)
        .await
        .map_err(|e| ApiError::S3(format!("{e}")))?;
    let _ = meta::upsert_upload(
        &st.pool,
        st.database_driver,
        entry.id,
        &upload_id,
        "reserved",
    )
    .await?;
    Ok(TwirpResponse::new(
        TwirpCreateResp {
            ok: true,
            signed_upload_url: build_upload_url(&origin, entry.id),
        },
        format,
    ))
}

// POST /twirp/.../FinalizeCacheEntryUpload
pub async fn finalize_cache_entry_upload(
    State(st): State<AppState>,
    request: TwirpRequest<TwirpFinalizeReq, cache::FinalizeCacheEntryUploadRequest>,
) -> Result<TwirpResponse<TwirpFinalizeResp, cache::FinalizeCacheEntryUploadResponse>> {
    let (req, format, _) = request.into_parts();
    let key = normalize_key(&req.key)?;
    let version = normalize_version(&req.version)?;
    let Some(entry) =
        meta::find_entry_by_key_version(&st.pool, st.database_driver, &key, &version).await?
    else {
        return Ok(TwirpResponse::new(
            TwirpFinalizeResp {
                ok: false,
                entry_id: 0,
            },
            format,
        ));
    };
    let query = rewrite_placeholders(
        "SELECT upload_id, storage_key FROM cache_uploads u JOIN cache_entries e ON e.id = u.entry_id WHERE e.id = ?",
        st.database_driver,
    );
    let rec = sqlx::query(&query)
        .bind(entry.id.to_string())
        .fetch_one(&st.pool)
        .await?;
    let upload_id: String = rec.try_get("upload_id")?;
    let storage_key: String = rec.try_get("storage_key")?;

    let mut status = meta::get_upload_status(&st.pool, st.database_driver, &upload_id).await?;
    if status.pending_finalize {
        return Ok(TwirpResponse::new(
            TwirpFinalizeResp {
                ok: true,
                entry_id: uuid_to_i64(entry.id),
            },
            format,
        ));
    }
    if let Err(err) =
        meta::set_pending_finalize(&st.pool, st.database_driver, &upload_id, true).await
    {
        return Err(err.into());
    }

    let run_in_background = if st.defer_finalize_in_background {
        status = meta::get_upload_status(&st.pool, st.database_driver, &upload_id).await?;
        let completed_parts =
            meta::get_completed_part_count(&st.pool, st.database_driver, &upload_id).await?;
        !(status.active_part_count == 0 && completed_parts == 1)
    } else {
        false
    };

    let job_state = st.clone();
    let job = crate::jobs::finalize::FinalizeUploadJob::new(
        job_state,
        entry.id,
        upload_id.clone(),
        storage_key.clone(),
        None,
    );
    if run_in_background {
        tokio::spawn(async move {
            if let Err(err) = crate::jobs::finalize::run(job).await {
                tracing::error!(?err, upload_id = %upload_id, "finalize upload job failed");
            }
        });

        Ok(TwirpResponse::new(
            TwirpFinalizeResp {
                ok: true,
                entry_id: uuid_to_i64(entry.id),
            },
            format,
        ))
    } else {
        match crate::jobs::finalize::run(job).await {
            Ok(()) => Ok(TwirpResponse::new(
                TwirpFinalizeResp {
                    ok: true,
                    entry_id: uuid_to_i64(entry.id),
                },
                format,
            )),
            Err(err) => {
                tracing::error!(?err, upload_id = %upload_id, "finalize upload job failed");
                Err(err)
            }
        }
    }
}

// POST /twirp/.../GetCacheEntryDownloadURL
pub async fn get_cache_entry_download_url(
    State(st): State<AppState>,
    request: TwirpRequest<TwirpGetUrlReq, cache::GetCacheEntryDownloadUrlRequest>,
) -> Result<TwirpResponse<TwirpGetUrlResp, cache::GetCacheEntryDownloadUrlResponse>> {
    let (req, format, origin) = request.into_parts();
    let key = normalize_key(&req.key)?;
    let version = normalize_version(&req.version)?;
    let mut restore_keys = Vec::with_capacity(req.restore_keys.len());
    for candidate in req.restore_keys {
        restore_keys.push(normalize_key(&candidate)?);
    }
    let candidates = unique_keys(key, &restore_keys);

    for candidate in candidates {
        if let Some(entry) =
            meta::find_entry_by_key_version(&st.pool, st.database_driver, &candidate, &version)
                .await?
        {
            meta::touch_entry(&st.pool, st.database_driver, entry.id).await?;
            if st.enable_direct {
                let pres = st
                    .store
                    .presign_get(&entry.storage_key, Duration::from_secs(3600))
                    .await
                    .map_err(|e| ApiError::S3(format!("{e}")))?;
                if let Some(url) = pres {
                    return Ok(TwirpResponse::new(
                        TwirpGetUrlResp {
                            ok: true,
                            signed_download_url: url.url.to_string(),
                            matched_key: candidate,
                        },
                        format,
                    ));
                }
            }

            return Ok(TwirpResponse::new(
                TwirpGetUrlResp {
                    ok: true,
                    signed_download_url: build_download_url(&origin, &candidate, entry.id),
                    matched_key: candidate,
                },
                format,
            ));
        }
    }

    Ok(TwirpResponse::new(
        TwirpGetUrlResp {
            ok: false,
            signed_download_url: String::new(),
            matched_key: String::new(),
        },
        format,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_content_type_handles_parameters() {
        let header = HeaderValue::from_str("application/protobuf; charset=utf-8").unwrap();
        assert!(matches!(
            parse_content_type(&header),
            Some(TwirpFormat::Protobuf)
        ));
    }

    #[test]
    fn accept_header_prefers_protobuf() {
        let header = HeaderValue::from_static("application/json, application/protobuf");
        let format = pick_response_format(Some(&header), TwirpFormat::Json);
        assert!(matches!(format, TwirpFormat::Protobuf));
    }
}
