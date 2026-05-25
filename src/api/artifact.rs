use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
};
use base64::{Engine as _, engine::general_purpose};
use chrono::{DateTime, Utc};
use http_body_util::BodyExt;
use quick_xml::{Reader, events::Event};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

use crate::api::path::encode_path_segment;
use crate::api::proto::artifact;
use crate::api::twirp::{TwirpRequest, TwirpResponse};
use crate::api::upload::body_to_blob_payload;
use crate::error::{ApiError, Result};
use crate::http::AppState;
use crate::meta::{self, ArtifactEntry};

const ARTIFACT_PART_NUMBER: i32 = 1;

#[derive(Default, Deserialize)]
pub(crate) struct ArtifactUploadQuery {
    #[serde(default)]
    comp: Option<String>,
    #[serde(default)]
    blockid: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct CreateArtifactReq {
    #[serde(alias = "workflowRunBackendId")]
    workflow_run_backend_id: String,
    #[serde(alias = "workflowJobRunBackendId")]
    workflow_job_run_backend_id: String,
    name: String,
    #[serde(default)]
    version: i32,
    #[serde(default, alias = "expiresAt")]
    expires_at: Option<DateTime<Utc>>,
}

impl TryFrom<artifact::CreateArtifactRequest> for CreateArtifactReq {
    type Error = ApiError;

    fn try_from(value: artifact::CreateArtifactRequest) -> Result<Self> {
        Ok(Self {
            workflow_run_backend_id: value.workflow_run_backend_id,
            workflow_job_run_backend_id: value.workflow_job_run_backend_id,
            name: value.name,
            version: value.version,
            expires_at: value.expires_at.and_then(timestamp_to_datetime),
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CreateArtifactResp {
    ok: bool,
    #[serde(rename = "signedUploadUrl")]
    signed_upload_url: String,
}

impl From<CreateArtifactResp> for artifact::CreateArtifactResponse {
    fn from(value: CreateArtifactResp) -> Self {
        Self {
            ok: value.ok,
            signed_upload_url: value.signed_upload_url,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct FinalizeArtifactReq {
    #[serde(alias = "workflowRunBackendId")]
    workflow_run_backend_id: String,
    #[serde(
        default,
        rename = "workflow_job_run_backend_id",
        alias = "workflowJobRunBackendId"
    )]
    _workflow_job_run_backend_id: String,
    name: String,
    #[serde(default, deserialize_with = "deserialize_i64_from_string_or_number")]
    size: i64,
    #[serde(default)]
    hash: Option<String>,
}

impl TryFrom<artifact::FinalizeArtifactRequest> for FinalizeArtifactReq {
    type Error = ApiError;

    fn try_from(value: artifact::FinalizeArtifactRequest) -> Result<Self> {
        Ok(Self {
            workflow_run_backend_id: value.workflow_run_backend_id,
            _workflow_job_run_backend_id: value.workflow_job_run_backend_id,
            name: value.name,
            size: value.size,
            hash: value.hash,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct FinalizeArtifactResp {
    ok: bool,
    #[serde(rename = "artifactId")]
    artifact_id: i64,
}

impl From<FinalizeArtifactResp> for artifact::FinalizeArtifactResponse {
    fn from(value: FinalizeArtifactResp) -> Self {
        Self {
            ok: value.ok,
            artifact_id: value.artifact_id,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ListArtifactsReq {
    #[serde(alias = "workflowRunBackendId")]
    workflow_run_backend_id: String,
    #[serde(
        default,
        rename = "workflow_job_run_backend_id",
        alias = "workflowJobRunBackendId"
    )]
    _workflow_job_run_backend_id: String,
    #[serde(default, alias = "nameFilter")]
    name_filter: Option<String>,
    #[serde(
        default,
        alias = "idFilter",
        deserialize_with = "deserialize_option_i64_from_string_or_number"
    )]
    id_filter: Option<i64>,
}

impl TryFrom<artifact::ListArtifactsRequest> for ListArtifactsReq {
    type Error = ApiError;

    fn try_from(value: artifact::ListArtifactsRequest) -> Result<Self> {
        Ok(Self {
            workflow_run_backend_id: value.workflow_run_backend_id,
            _workflow_job_run_backend_id: value.workflow_job_run_backend_id,
            name_filter: value.name_filter,
            id_filter: value.id_filter,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ListArtifactsResp {
    artifacts: Vec<ListArtifact>,
}

impl From<ListArtifactsResp> for artifact::ListArtifactsResponse {
    fn from(value: ListArtifactsResp) -> Self {
        Self {
            artifacts: value.artifacts.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct ListArtifact {
    #[serde(rename = "workflowRunBackendId")]
    workflow_run_backend_id: String,
    #[serde(rename = "workflowJobRunBackendId")]
    workflow_job_run_backend_id: String,
    #[serde(rename = "databaseId")]
    database_id: i64,
    name: String,
    size: i64,
    #[serde(rename = "createdAt")]
    created_at: DateTime<Utc>,
}

impl From<ListArtifact> for artifact::list_artifacts_response::MonolithArtifact {
    fn from(value: ListArtifact) -> Self {
        Self {
            workflow_run_backend_id: value.workflow_run_backend_id,
            workflow_job_run_backend_id: value.workflow_job_run_backend_id,
            database_id: value.database_id,
            name: value.name,
            size: value.size,
            created_at: Some(datetime_to_timestamp(value.created_at)),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct GetSignedArtifactUrlReq {
    #[serde(alias = "workflowRunBackendId")]
    workflow_run_backend_id: String,
    #[serde(
        default,
        rename = "workflow_job_run_backend_id",
        alias = "workflowJobRunBackendId"
    )]
    _workflow_job_run_backend_id: String,
    name: String,
}

impl TryFrom<artifact::GetSignedArtifactUrlRequest> for GetSignedArtifactUrlReq {
    type Error = ApiError;

    fn try_from(value: artifact::GetSignedArtifactUrlRequest) -> Result<Self> {
        Ok(Self {
            workflow_run_backend_id: value.workflow_run_backend_id,
            _workflow_job_run_backend_id: value.workflow_job_run_backend_id,
            name: value.name,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct GetSignedArtifactUrlResp {
    #[serde(rename = "signedUrl")]
    signed_url: String,
}

impl From<GetSignedArtifactUrlResp> for artifact::GetSignedArtifactUrlResponse {
    fn from(value: GetSignedArtifactUrlResp) -> Self {
        Self {
            signed_url: value.signed_url,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct DeleteArtifactReq {
    #[serde(alias = "workflowRunBackendId")]
    workflow_run_backend_id: String,
    #[serde(
        default,
        rename = "workflow_job_run_backend_id",
        alias = "workflowJobRunBackendId"
    )]
    _workflow_job_run_backend_id: String,
    name: String,
}

impl TryFrom<artifact::DeleteArtifactRequest> for DeleteArtifactReq {
    type Error = ApiError;

    fn try_from(value: artifact::DeleteArtifactRequest) -> Result<Self> {
        Ok(Self {
            workflow_run_backend_id: value.workflow_run_backend_id,
            _workflow_job_run_backend_id: value.workflow_job_run_backend_id,
            name: value.name,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct DeleteArtifactResp {
    ok: bool,
    #[serde(rename = "artifactId")]
    artifact_id: i64,
}

impl From<DeleteArtifactResp> for artifact::DeleteArtifactResponse {
    fn from(value: DeleteArtifactResp) -> Self {
        Self {
            ok: value.ok,
            artifact_id: value.artifact_id,
        }
    }
}

pub(crate) async fn create_artifact(
    State(st): State<AppState>,
    request: TwirpRequest<CreateArtifactReq, artifact::CreateArtifactRequest>,
) -> Result<TwirpResponse<CreateArtifactResp, artifact::CreateArtifactResponse>> {
    let (req, format, origin) = request.into_parts();
    validate_artifact_identity(
        &req.workflow_run_backend_id,
        &req.workflow_job_run_backend_id,
        &req.name,
    )?;

    if let Some(existing) = meta::find_active_artifact_by_name(
        &st.pool,
        st.database_driver,
        &req.workflow_run_backend_id,
        &req.name,
    )
    .await?
    {
        if existing.state == "finalized" {
            return Err(ApiError::BadRequest("artifact already exists".into()));
        }

        let upload = meta::fetch_artifact_upload(&st.pool, st.database_driver, existing.id).await?;
        if existing.state == "created" && upload.state == "reserved" {
            return Ok(TwirpResponse::new(
                CreateArtifactResp {
                    ok: true,
                    signed_upload_url: origin
                        .absolute(&format!("/artifact-upload/{}", existing.id)),
                },
                format,
            ));
        }

        meta::delete_artifact_entry(&st.pool, st.database_driver, existing.id).await?;
        st.store
            .delete(&existing.storage_key)
            .await
            .map_err(|err| ApiError::S3(format!("{err}")))?;
    }

    let provisional_id = Uuid::new_v4();
    let storage_key = build_artifact_storage_key(
        &req.workflow_run_backend_id,
        &req.workflow_job_run_backend_id,
        &req.name,
        provisional_id,
    );
    let entry = meta::create_artifact_entry(
        &st.pool,
        st.database_driver,
        &req.workflow_run_backend_id,
        &req.workflow_job_run_backend_id,
        &req.name,
        i64::from(req.version),
        &storage_key,
        req.expires_at,
    )
    .await?;
    let upload_id = st
        .store
        .create_multipart(&entry.storage_key)
        .await
        .map_err(|err| ApiError::S3(format!("{err}")))?;
    meta::upsert_artifact_upload(&st.pool, st.database_driver, entry.id, &upload_id).await?;

    Ok(TwirpResponse::new(
        CreateArtifactResp {
            ok: true,
            signed_upload_url: origin.absolute(&format!("/artifact-upload/{}", entry.id)),
        },
        format,
    ))
}

pub(crate) async fn put_artifact(
    State(st): State<AppState>,
    Path(artifact_id): Path<String>,
    Query(query): Query<ArtifactUploadQuery>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response> {
    let artifact_id = parse_uuid(&artifact_id, "invalid artifact id")?;
    let entry = meta::fetch_artifact_entry(&st.pool, st.database_driver, artifact_id).await?;
    if entry.state == "finalized" || entry.state == "deleted" {
        return Err(ApiError::BadRequest("artifact is not writable".into()));
    }
    let upload = meta::fetch_artifact_upload(&st.pool, st.database_driver, artifact_id).await?;
    if upload.state == "completed" {
        return Err(ApiError::BadRequest(
            "artifact upload is already complete".into(),
        ));
    }

    match query.comp.as_deref() {
        Some("block") => {
            let block_id = query
                .blockid
                .as_deref()
                .ok_or_else(|| ApiError::BadRequest("missing blockid query parameter".into()))?;
            return put_artifact_block(&st, &entry, &upload.upload_id, block_id, headers, body)
                .await;
        }
        Some("blocklist") => {
            return put_artifact_block_list(&st, &entry, &upload.upload_id, body).await;
        }
        Some(other) => {
            return Err(ApiError::BadRequest(format!(
                "unsupported artifact upload component '{other}'"
            )));
        }
        None => {}
    }

    if upload.state != "reserved" {
        return Err(ApiError::BadRequest(
            "artifact upload is already complete".into(),
        ));
    }

    let size = parse_content_length(&headers)?;
    let payload = body_to_blob_payload(body);
    let etag = st
        .store
        .upload_part(
            &entry.storage_key,
            &upload.upload_id,
            ARTIFACT_PART_NUMBER,
            payload,
        )
        .await
        .map_err(|err| ApiError::S3(format!("{err}")))?;
    meta::mark_artifact_uploaded(&st.pool, st.database_driver, artifact_id, &etag, size).await?;

    Ok(StatusCode::CREATED.into_response())
}

async fn put_artifact_block(
    st: &AppState,
    entry: &ArtifactEntry,
    upload_id: &str,
    block_id: &str,
    headers: HeaderMap,
    body: Body,
) -> Result<Response> {
    let _ = validate_block_id(block_id)?;
    let size = parse_content_length(&headers)?;
    let part_number =
        meta::next_artifact_part_number(&st.pool, st.database_driver, upload_id, block_id).await?;
    let storage_key = artifact_block_storage_key(entry, upload_id, block_id);
    let block_upload_id = st
        .store
        .create_multipart(&storage_key)
        .await
        .map_err(|err| ApiError::S3(format!("{err}")))?;
    let payload = body_to_blob_payload(body);
    let etag = st
        .store
        .upload_part(
            &storage_key,
            &block_upload_id,
            ARTIFACT_PART_NUMBER,
            payload,
        )
        .await
        .map_err(|err| ApiError::S3(format!("{err}")))?;
    st.store
        .complete_multipart(
            &storage_key,
            &block_upload_id,
            vec![(ARTIFACT_PART_NUMBER, etag.clone())],
        )
        .await
        .map_err(|err| ApiError::S3(format!("{err}")))?;
    meta::record_artifact_part(
        &st.pool,
        st.database_driver,
        upload_id,
        block_id,
        part_number,
        size,
        &etag,
        &storage_key,
    )
    .await?;
    Ok(azure_created_response())
}

async fn put_artifact_block_list(
    st: &AppState,
    entry: &ArtifactEntry,
    upload_id: &str,
    body: Body,
) -> Result<Response> {
    let collected = body
        .collect()
        .await
        .map_err(|err| ApiError::BadRequest(format!("failed to read block list body: {err}")))?;
    let block_ids = parse_block_list(&collected.to_bytes())?;
    if block_ids.is_empty() {
        return Err(ApiError::BadRequest("block list is empty".into()));
    }

    let records =
        meta::artifact_parts_by_block_ids(&st.pool, st.database_driver, upload_id, &block_ids)
            .await?;
    let total_size = records.iter().map(|part| part.size).sum();
    let mut final_parts = Vec::with_capacity(records.len());
    let mut temporary_keys = Vec::with_capacity(records.len());
    for (index, part) in records.into_iter().enumerate() {
        let storage_key = part
            .storage_key
            .ok_or_else(|| ApiError::BadRequest("artifact block is missing storage".into()))?;
        let Some(stream) = st
            .store
            .get(&storage_key)
            .await
            .map_err(|err| ApiError::S3(format!("{err}")))?
        else {
            return Err(ApiError::BadRequest(
                "artifact block storage is missing".into(),
            ));
        };
        let part_number = i32::try_from(index + 1)
            .map_err(|_| ApiError::BadRequest("artifact has too many blocks".into()))?;
        let etag = st
            .store
            .upload_part(&entry.storage_key, upload_id, part_number, stream)
            .await
            .map_err(|err| ApiError::S3(format!("{err}")))?;
        final_parts.push((part_number, etag));
        temporary_keys.push(storage_key);
    }
    st.store
        .complete_multipart(&entry.storage_key, upload_id, final_parts)
        .await
        .map_err(|err| ApiError::S3(format!("{err}")))?;
    for storage_key in temporary_keys {
        if let Err(err) = st.store.delete(&storage_key).await {
            tracing::warn!(%storage_key, ?err, "failed to delete temporary artifact block");
        }
    }
    meta::mark_artifact_multipart_committed(&st.pool, st.database_driver, entry.id, total_size)
        .await?;
    Ok(azure_created_response())
}

pub(crate) async fn finalize_artifact(
    State(st): State<AppState>,
    request: TwirpRequest<FinalizeArtifactReq, artifact::FinalizeArtifactRequest>,
) -> Result<TwirpResponse<FinalizeArtifactResp, artifact::FinalizeArtifactResponse>> {
    let (req, format, _) = request.into_parts();
    let Some(entry) = meta::find_active_artifact_by_name(
        &st.pool,
        st.database_driver,
        &req.workflow_run_backend_id,
        &req.name,
    )
    .await?
    else {
        return Ok(TwirpResponse::new(
            FinalizeArtifactResp {
                ok: false,
                artifact_id: 0,
            },
            format,
        ));
    };

    if entry.state == "finalized" {
        return Ok(TwirpResponse::new(
            FinalizeArtifactResp {
                ok: true,
                artifact_id: entry.numeric_id,
            },
            format,
        ));
    }

    let upload = meta::fetch_artifact_upload(&st.pool, st.database_driver, entry.id).await?;
    if upload.state != "completed" {
        let Some(etag) = upload.etag else {
            return Err(ApiError::BadRequest(
                "artifact upload has not completed".into(),
            ));
        };
        st.store
            .complete_multipart(
                &entry.storage_key,
                &upload.upload_id,
                vec![(ARTIFACT_PART_NUMBER, etag)],
            )
            .await
            .map_err(|err| ApiError::S3(format!("{err}")))?;
    }

    let size = if req.size > 0 {
        req.size
    } else {
        upload.size_bytes
    };
    meta::finalize_artifact_entry(
        &st.pool,
        st.database_driver,
        entry.id,
        size,
        req.hash.as_deref(),
    )
    .await?;

    Ok(TwirpResponse::new(
        FinalizeArtifactResp {
            ok: true,
            artifact_id: entry.numeric_id,
        },
        format,
    ))
}

pub(crate) async fn list_artifacts(
    State(st): State<AppState>,
    request: TwirpRequest<ListArtifactsReq, artifact::ListArtifactsRequest>,
) -> Result<TwirpResponse<ListArtifactsResp, artifact::ListArtifactsResponse>> {
    let (req, format, _) = request.into_parts();
    let entries = meta::list_artifact_entries(
        &st.pool,
        st.database_driver,
        &req.workflow_run_backend_id,
        req.name_filter.as_deref(),
        req.id_filter,
    )
    .await?;

    Ok(TwirpResponse::new(
        ListArtifactsResp {
            artifacts: entries.into_iter().map(entry_to_list_item).collect(),
        },
        format,
    ))
}

pub(crate) async fn get_signed_artifact_url(
    State(st): State<AppState>,
    request: TwirpRequest<GetSignedArtifactUrlReq, artifact::GetSignedArtifactUrlRequest>,
) -> Result<TwirpResponse<GetSignedArtifactUrlResp, artifact::GetSignedArtifactUrlResponse>> {
    let (req, format, origin) = request.into_parts();
    let entry = meta::find_finalized_artifact_by_name(
        &st.pool,
        st.database_driver,
        &req.workflow_run_backend_id,
        &req.name,
    )
    .await?
    .or(
        meta::find_latest_finalized_artifact_by_name(&st.pool, st.database_driver, &req.name)
            .await?,
    )
    .ok_or(ApiError::NotFound)?;

    let signed_url = if st.enable_direct {
        let presigned = st
            .store
            .presign_get(&entry.storage_key, Duration::from_secs(3600))
            .await
            .map_err(|err| ApiError::S3(format!("{err}")))?;
        presigned
            .map(|url| url.url.to_string())
            .unwrap_or_else(|| build_download_url(&origin, &entry))
    } else {
        build_download_url(&origin, &entry)
    };

    Ok(TwirpResponse::new(
        GetSignedArtifactUrlResp { signed_url },
        format,
    ))
}

pub(crate) async fn download_artifact(
    State(st): State<AppState>,
    Path((artifact_id, filename)): Path<(String, String)>,
) -> Result<Response> {
    let artifact_id = parse_uuid(&artifact_id, "invalid artifact id")?;
    let entry = meta::fetch_artifact_entry(&st.pool, st.database_driver, artifact_id).await?;
    if entry.state != "finalized" || filename != artifact_filename(&entry) {
        return Err(ApiError::NotFound);
    }
    if st.enable_direct {
        let presigned = st
            .store
            .presign_get(&entry.storage_key, Duration::from_secs(3600))
            .await
            .map_err(|err| ApiError::S3(format!("{err}")))?;
        if let Some(url) = presigned {
            return Ok(Redirect::temporary(url.url.as_str()).into_response());
        }
    }
    let Some(stream) = st
        .store
        .get(&entry.storage_key)
        .await
        .map_err(|err| ApiError::S3(format!("{err}")))?
    else {
        return Err(ApiError::NotFound);
    };
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/zip")
        .body(Body::from_stream(stream))
        .map_err(|err| ApiError::Internal(format!("failed to build response: {err}")))?;
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!(
            "attachment; filename=\"{}\"",
            artifact_filename(&entry)
        ))
        .map_err(|err| ApiError::Internal(format!("invalid header value: {err}")))?,
    );
    Ok(response)
}

pub(crate) async fn delete_artifact(
    State(st): State<AppState>,
    request: TwirpRequest<DeleteArtifactReq, artifact::DeleteArtifactRequest>,
) -> Result<TwirpResponse<DeleteArtifactResp, artifact::DeleteArtifactResponse>> {
    let (req, format, _) = request.into_parts();
    let Some(entry) = meta::find_active_artifact_by_name(
        &st.pool,
        st.database_driver,
        &req.workflow_run_backend_id,
        &req.name,
    )
    .await?
    else {
        return Ok(TwirpResponse::new(
            DeleteArtifactResp {
                ok: false,
                artifact_id: 0,
            },
            format,
        ));
    };
    meta::delete_artifact_entry(&st.pool, st.database_driver, entry.id).await?;
    st.store
        .delete(&entry.storage_key)
        .await
        .map_err(|err| ApiError::S3(format!("{err}")))?;

    Ok(TwirpResponse::new(
        DeleteArtifactResp {
            ok: true,
            artifact_id: entry.numeric_id,
        },
        format,
    ))
}

fn validate_artifact_identity(run_id: &str, job_id: &str, name: &str) -> Result<()> {
    if run_id.trim().is_empty() {
        return Err(ApiError::BadRequest(
            "workflow_run_backend_id is required".into(),
        ));
    }
    if job_id.trim().is_empty() {
        return Err(ApiError::BadRequest(
            "workflow_job_run_backend_id is required".into(),
        ));
    }
    if name.trim().is_empty() {
        return Err(ApiError::BadRequest("artifact name is required".into()));
    }
    Ok(())
}

fn build_artifact_storage_key(
    run_id: &str,
    job_id: &str,
    name: &str,
    provisional_id: Uuid,
) -> String {
    let name = name.replace('/', "_");
    format!(
        "artifacts/{}/{}/{}/{}.zip",
        encode_path_segment(run_id),
        encode_path_segment(job_id),
        encode_path_segment(&name),
        provisional_id
    )
}

fn build_download_url(origin: &crate::api::twirp::RequestOrigin, entry: &ArtifactEntry) -> String {
    origin.absolute(&format!(
        "/artifact-download/{}/{}",
        entry.id,
        encode_path_segment(&artifact_filename(entry))
    ))
}

fn artifact_filename(entry: &ArtifactEntry) -> String {
    format!("{}.zip", entry.name.replace(['/', '\\'], "_"))
}

fn artifact_block_storage_key(entry: &ArtifactEntry, upload_id: &str, block_id: &str) -> String {
    format!(
        "artifact-blocks/{}/{}/{}",
        entry.id,
        encode_path_segment(upload_id),
        encode_path_segment(block_id)
    )
}

fn entry_to_list_item(entry: ArtifactEntry) -> ListArtifact {
    ListArtifact {
        workflow_run_backend_id: entry.workflow_run_backend_id,
        workflow_job_run_backend_id: entry.workflow_job_run_backend_id,
        database_id: entry.numeric_id,
        name: entry.name,
        size: entry.size_bytes,
        created_at: entry.created_at,
    }
}

fn parse_uuid(raw: &str, message: &str) -> Result<Uuid> {
    Uuid::parse_str(raw).map_err(|_| ApiError::BadRequest(message.into()))
}

fn parse_content_length(headers: &HeaderMap) -> Result<i64> {
    let value = headers
        .get(header::CONTENT_LENGTH)
        .ok_or_else(|| ApiError::BadRequest("missing Content-Length header".into()))?;
    let value = value
        .to_str()
        .map_err(|_| ApiError::BadRequest("invalid Content-Length header".into()))?;
    let size = value
        .parse::<i64>()
        .map_err(|_| ApiError::BadRequest("invalid Content-Length header".into()))?;
    if size < 0 {
        return Err(ApiError::BadRequest("invalid Content-Length header".into()));
    }
    Ok(size)
}

fn azure_created_response() -> Response {
    let mut response = StatusCode::CREATED.into_response();
    let request_id = Uuid::new_v4().to_string();
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-ms-request-id", value);
    }
    response
}

fn validate_block_id(block_id: &str) -> Result<Vec<u8>> {
    if block_id.trim().is_empty() {
        return Err(ApiError::BadRequest("blockid may not be empty".into()));
    }
    general_purpose::STANDARD
        .decode(block_id)
        .map_err(|_| ApiError::BadRequest("blockid must be base64 encoded".into()))
}

fn parse_block_list(body: &[u8]) -> Result<Vec<String>> {
    let mut reader = Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut blocks = Vec::new();
    let mut in_block_element = false;

    loop {
        match reader.read_event() {
            Ok(Event::Start(event)) => {
                in_block_element = matches!(
                    event.name().as_ref(),
                    b"Latest" | b"Uncommitted" | b"Committed"
                );
            }
            Ok(Event::Text(text)) if in_block_element => {
                let block_id = text
                    .decode()
                    .map_err(|err| ApiError::BadRequest(format!("invalid block list XML: {err}")))?
                    .into_owned();
                let _ = validate_block_id(&block_id)?;
                blocks.push(block_id);
            }
            Ok(Event::End(_)) => {
                in_block_element = false;
            }
            Ok(Event::Eof) => break,
            Err(err) => {
                return Err(ApiError::BadRequest(format!(
                    "invalid block list XML: {err}"
                )));
            }
            _ => {}
        }
    }

    Ok(blocks)
}

fn timestamp_to_datetime(value: prost_types::Timestamp) -> Option<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(value.seconds, value.nanos.try_into().ok()?)
}

fn datetime_to_timestamp(value: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: value.timestamp(),
        nanos: i32::try_from(value.timestamp_subsec_nanos()).unwrap_or_default(),
    }
}

fn deserialize_i64_from_string_or_number<'de, D>(
    deserializer: D,
) -> std::result::Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(deserialize_option_i64_from_string_or_number(deserializer)?.unwrap_or_default())
}

fn deserialize_option_i64_from_string_or_number<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{Error as DeError, Visitor};
    use std::fmt;

    struct OptionVisitor;

    impl<'de> Visitor<'de> for OptionVisitor {
        type Value = Option<i64>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("an integer, string containing an integer, or null")
        }

        fn visit_none<E>(self) -> std::result::Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            deserializer.deserialize_any(self)
        }

        fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E> {
            Ok(Some(value))
        }

        fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
        where
            E: DeError,
        {
            i64::try_from(value)
                .map(Some)
                .map_err(|_| E::custom("integer overflow"))
        }

        fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
        where
            E: DeError,
        {
            value
                .parse::<i64>()
                .map(Some)
                .map_err(|_| E::custom("invalid integer string"))
        }

        fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E>
        where
            E: DeError,
        {
            self.visit_str(&value)
        }
    }

    deserializer.deserialize_option(OptionVisitor)
}
