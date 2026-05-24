CREATE TABLE IF NOT EXISTS artifact_entries (
    id VARCHAR(36) PRIMARY KEY,
    numeric_id BIGINT NOT NULL UNIQUE,
    workflow_run_backend_id VARCHAR(255) NOT NULL,
    workflow_job_run_backend_id VARCHAR(255) NOT NULL,
    name VARCHAR(512) NOT NULL,
    version BIGINT NOT NULL DEFAULT 4,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    hash TEXT,
    storage_key TEXT NOT NULL,
    state VARCHAR(32) NOT NULL CHECK (state IN ('created','uploading','finalized','deleted')),
    expires_at BIGINT,
    created_at BIGINT NOT NULL DEFAULT (UNIX_TIMESTAMP()),
    updated_at BIGINT NOT NULL DEFAULT (UNIX_TIMESTAMP())
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS artifact_uploads (
    id VARCHAR(36) PRIMARY KEY,
    artifact_id VARCHAR(36) NOT NULL,
    upload_id VARCHAR(255) NOT NULL UNIQUE,
    state VARCHAR(32) NOT NULL CHECK (state IN ('reserved','uploaded','completed')),
    etag TEXT,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL DEFAULT (UNIX_TIMESTAMP()),
    updated_at BIGINT NOT NULL DEFAULT (UNIX_TIMESTAMP()),
    CONSTRAINT fk_artifact_upload_entry FOREIGN KEY (artifact_id) REFERENCES artifact_entries(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS artifact_upload_parts (
    upload_id VARCHAR(255) NOT NULL,
    block_id VARCHAR(512) NOT NULL,
    part_number BIGINT NOT NULL,
    size BIGINT NOT NULL,
    etag TEXT NOT NULL,
    created_at BIGINT NOT NULL DEFAULT (UNIX_TIMESTAMP()),
    updated_at BIGINT NOT NULL DEFAULT (UNIX_TIMESTAMP()),
    PRIMARY KEY (upload_id, block_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_artifact_entries_active_name
ON artifact_entries (workflow_run_backend_id, workflow_job_run_backend_id, name(191), state);

CREATE INDEX idx_artifact_entries_run_job
ON artifact_entries (workflow_run_backend_id, workflow_job_run_backend_id);

CREATE INDEX idx_artifact_upload_parts_upload_part
ON artifact_upload_parts (upload_id, part_number);
