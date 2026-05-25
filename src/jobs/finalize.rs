use tracing::error;

use crate::db::safe_sql;
use crate::error::ApiError;
use crate::http::AppState;
use crate::meta;
use crate::meta::UploadPartRecord;

pub struct FinalizeUploadJob {
    pub state: AppState,
    pub entry_id: uuid::Uuid,
    pub upload_id: String,
    pub storage_key: String,
    pub expected_size: Option<i64>,
}

impl FinalizeUploadJob {
    pub fn new(
        state: AppState,
        entry_id: uuid::Uuid,
        upload_id: String,
        storage_key: String,
        expected_size: Option<i64>,
    ) -> Self {
        Self {
            state,
            entry_id,
            upload_id,
            storage_key,
            expected_size,
        }
    }
}

pub async fn run(job: FinalizeUploadJob) -> Result<(), ApiError> {
    let FinalizeUploadJob {
        state,
        entry_id,
        upload_id,
        storage_key,
        expected_size,
    } = job;

    let store = state.store.clone();
    let pool = state.pool.clone();
    let driver = state.database_driver;

    let result = async {
        if let Err(err) = meta::wait_for_no_active_parts(&pool, driver, &upload_id).await {
            return Err(err.into());
        }

        let reserved = meta::transition_upload_state(
            &pool,
            driver,
            &upload_id,
            &["reserved", "ready", "uploading"],
            "finalizing",
        )
        .await?;

        if !reserved {
            return Err(ApiError::BadRequest(
                "upload is still receiving parts".into(),
            ));
        }

        let parts = meta::get_completed_parts(&pool, driver, &upload_id).await?;

        if let Err(err) = ensure_all_parts_uploaded(&parts, expected_size) {
            let _ = meta::transition_upload_state(
                &pool,
                driver,
                &upload_id,
                &["finalizing"],
                "uploading",
            )
            .await;
            return Err(err);
        }

        match store
            .complete_multipart(
                &storage_key,
                &upload_id,
                parts
                    .iter()
                    .map(|part| (part.part_number, part.etag.clone()))
                    .collect(),
            )
            .await
        {
            Ok(()) => {
                let finalized = meta::transition_upload_state(
                    &pool,
                    driver,
                    &upload_id,
                    &["finalizing"],
                    "completed",
                )
                .await?;

                if !finalized {
                    return Err(ApiError::Internal(
                        "failed to record completed upload state".into(),
                    ));
                }
            }
            Err(err) => {
                let _ = meta::transition_upload_state(
                    &pool,
                    driver,
                    &upload_id,
                    &["finalizing"],
                    "uploading",
                )
                .await;

                return Err(ApiError::S3(format!("{err}")));
            }
        }

        if let Some(size) = expected_size {
            let query = crate::db::rewrite_placeholders(
                "UPDATE cache_entries SET size_bytes = ? WHERE id = ?",
                driver,
            );

            sqlx::query(safe_sql(&query))
                .bind(size)
                .bind(entry_id.to_string())
                .execute(&pool)
                .await?;
        }

        Ok(())
    }
    .await;

    let clear_result = meta::set_pending_finalize(&pool, driver, &upload_id, false).await;

    if let Err(err) = clear_result {
        error!(
            ?err,
            upload_id = %upload_id,
            "failed to clear pending finalize flag"
        );
        if result.is_ok() {
            return Err(err.into());
        }
    }

    result
}

fn ensure_all_parts_uploaded(
    parts: &[UploadPartRecord],
    expected_size: Option<i64>,
) -> Result<(), ApiError> {
    if parts.is_empty() {
        return Err(ApiError::BadRequest(
            "multipart upload must include at least one part".into(),
        ));
    }

    let mut expected_offset = 0i64;
    for (index, part) in parts.iter().enumerate() {
        let expected_index = index as i32;
        let expected_part_number = expected_index + 1;
        if part.part_index != expected_index {
            return Err(ApiError::BadRequest(format!(
                "missing part {expected_part_number} before finalization"
            )));
        }
        if part.offset != expected_offset {
            return Err(ApiError::BadRequest(format!(
                "unexpected offset for part {}",
                part.part_number
            )));
        }
        if part.size <= 0 {
            return Err(ApiError::BadRequest(format!(
                "invalid size recorded for part {}",
                part.part_number
            )));
        }
        expected_offset = expected_offset
            .checked_add(part.size)
            .ok_or_else(|| ApiError::BadRequest("upload size overflow".into()))?;
    }

    if let Some(total) = expected_size
        && total != expected_offset
    {
        return Err(ApiError::BadRequest(
            "uploaded parts do not match expected size".into(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_all_parts_uploaded_accepts_contiguous_sequence() {
        let parts = vec![
            UploadPartRecord {
                part_index: 0,
                part_number: 1,
                offset: 0,
                size: 10,
                etag: "etag-1".into(),
            },
            UploadPartRecord {
                part_index: 1,
                part_number: 2,
                offset: 10,
                size: 5,
                etag: "etag-2".into(),
            },
        ];

        assert!(ensure_all_parts_uploaded(&parts, Some(15)).is_ok());
    }

    #[test]
    fn ensure_all_parts_uploaded_rejects_gaps() {
        let parts = vec![
            UploadPartRecord {
                part_index: 0,
                part_number: 1,
                offset: 0,
                size: 10,
                etag: "etag-1".into(),
            },
            UploadPartRecord {
                part_index: 2,
                part_number: 3,
                offset: 10,
                size: 5,
                etag: "etag-3".into(),
            },
        ];

        let err = ensure_all_parts_uploaded(&parts, None).expect_err("gap should be rejected");
        if let ApiError::BadRequest(message) = err {
            assert!(message.contains("missing part 2"));
        } else {
            panic!("unexpected error variant");
        }
    }
}
