use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
};
use sqlx::Row;
use std::time::Duration;
use uuid::Uuid;

use crate::db::{rewrite_placeholders, safe_sql};
use crate::error::{ApiError, Result};
use crate::http::AppState;
use crate::meta;

// GET /download/{cache_key}/{filename}
// We validate the {cache_key} against the database record and expect {filename} to be
// "{entry_id}.tgz" so clients receive stable names when downloading through the proxy.
pub async fn download_proxy(
    State(st): State<AppState>,
    Path((cache_key, filename)): Path<(String, String)>,
) -> Result<Response> {
    let Some(entry_id) = filename.strip_suffix(".tgz") else {
        return Err(ApiError::BadRequest("invalid cache filename".into()));
    };
    let entry_id =
        Uuid::parse_str(entry_id).map_err(|_| ApiError::BadRequest("invalid cache id".into()))?;
    let query = rewrite_placeholders(
        "SELECT storage_key, cache_key FROM cache_entries WHERE id = ?",
        st.database_driver,
    );
    let rec = sqlx::query(safe_sql(&query))
        .bind(entry_id.to_string())
        .fetch_optional(&st.pool)
        .await?;
    let row = rec.ok_or(ApiError::NotFound)?;
    let storage_key: String = row.try_get("storage_key")?;
    let stored_key: String = row.try_get("cache_key")?;
    if stored_key != cache_key {
        return Err(ApiError::NotFound);
    }
    meta::touch_entry(&st.pool, st.database_driver, entry_id).await?;
    if st.enable_direct {
        let pres = st
            .store
            .presign_get(&storage_key, Duration::from_secs(3600))
            .await
            .map_err(|e| ApiError::S3(format!("{e}")))?;
        if let Some(p) = pres {
            return Ok(Redirect::temporary(p.url.as_str()).into_response());
        }
    }
    let stream = st
        .store
        .get(&storage_key)
        .await
        .map_err(|e| ApiError::S3(format!("{e}")))?;
    let Some(stream) = stream else {
        return Err(ApiError::NotFound);
    };
    let body = Body::from_stream(stream);
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(body)
        .map_err(|err| ApiError::Internal(format!("failed to build response: {err}")))?;
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"{}.tgz\"", entry_id))
            .map_err(|err| ApiError::Internal(format!("invalid header value: {err}")))?,
    );
    Ok(response)
}
