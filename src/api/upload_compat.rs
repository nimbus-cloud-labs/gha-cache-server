use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use sqlx::Row;
use uuid::Uuid;

use crate::api::upload::{body_to_blob_payload, chunk_index_from_block_id};
use crate::db::{rewrite_placeholders, safe_sql};
use crate::error::{ApiError, Result};
use crate::http::AppState;
use crate::meta;

// PUT /upload/{cache-id}
// Compatibility handler some forks rely on. Treats the whole body as a single part.
#[derive(Default, Deserialize)]
pub struct UploadQuery {
    #[serde(default)]
    comp: Option<String>,
    #[serde(default)]
    blockid: Option<String>,
}

pub async fn put_upload(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<UploadQuery>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Result<Response> {
    let uuid = Uuid::parse_str(&id).map_err(|_| ApiError::BadRequest("invalid cacheId".into()))?;

    if matches!(query.comp.as_deref(), Some("blocklist")) {
        return Ok(created_response());
    }

    let sql = rewrite_placeholders(
        "SELECT upload_id, storage_key FROM cache_uploads u JOIN cache_entries e ON e.id = u.entry_id WHERE e.id = ?",
        st.database_driver,
    );
    let rec = sqlx::query(safe_sql(&sql))
        .bind(uuid.to_string())
        .fetch_one(&st.pool)
        .await?;
    let upload_id: String = rec.try_get("upload_id")?;
    let storage_key: String = rec.try_get("storage_key")?;

    let mut status = meta::get_upload_status(&st.pool, st.database_driver, &upload_id).await?;
    if status.pending_finalize {
        return Err(ApiError::BadRequest("upload is finalizing".into()));
    }

    let chunk_index = query
        .blockid
        .as_deref()
        .map(chunk_index_from_block_id)
        .unwrap_or(Ok(0))?;

    let part_no = i32::try_from(chunk_index + 1)
        .map_err(|_| ApiError::BadRequest("invalid chunk index".into()))?;
    let size = parse_content_length(&headers)?;
    if status.state != "uploading" {
        meta::transition_to_uploading(&st.pool, st.database_driver, &upload_id, &mut status)
            .await?;
    }

    if let Err(err) = meta::reserve_part(
        &st.pool,
        st.database_driver,
        &upload_id,
        part_no - 1,
        None,
        size,
    )
    .await
    {
        return Err(err.into());
    }

    if let Err(err) = meta::begin_part_upload(&st.pool, st.database_driver, &upload_id).await {
        return Err(err.into());
    }

    let bs = body_to_blob_payload(body);
    let etag = match st
        .store
        .upload_part(&storage_key, &upload_id, part_no, bs)
        .await
    {
        Ok(etag) => etag,
        Err(err) => {
            let finish = meta::finish_part_upload(&st.pool, st.database_driver, &upload_id).await;
            return match finish {
                Ok(_) => Err(ApiError::S3(format!("{err}"))),
                Err(db_err) => Err(db_err.into()),
            };
        }
    };
    if let Err(err) = meta::complete_part(
        &st.pool,
        st.database_driver,
        &upload_id,
        part_no - 1,
        None,
        &etag,
    )
    .await
    {
        let finish = meta::finish_part_upload(&st.pool, st.database_driver, &upload_id).await;
        return match finish {
            Ok(_) => Err(err.into()),
            Err(db_err) => Err(db_err.into()),
        };
    }

    meta::finish_part_upload(&st.pool, st.database_driver, &upload_id).await?;

    Ok(created_response())
}

fn parse_content_length(headers: &HeaderMap) -> Result<i64> {
    let value = headers
        .get(axum::http::header::CONTENT_LENGTH)
        .ok_or_else(|| ApiError::BadRequest("missing Content-Length header".into()))?;
    let value = value
        .to_str()
        .map_err(|_| ApiError::BadRequest("invalid Content-Length header".into()))?;
    let size = value
        .parse::<i64>()
        .map_err(|_| ApiError::BadRequest("invalid Content-Length header".into()))?;
    if size <= 0 {
        return Err(ApiError::BadRequest("invalid Content-Length header".into()));
    }
    Ok(size)
}

fn created_response() -> Response {
    let mut response = StatusCode::CREATED.into_response();
    let request_id = Uuid::new_v4().to_string();
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-ms-request-id", value);
    }
    response
}
