CREATE TABLE IF NOT EXISTS artifact_entries (
    id TEXT PRIMARY KEY,
    numeric_id BIGINT NOT NULL UNIQUE,
    workflow_run_backend_id TEXT NOT NULL,
    workflow_job_run_backend_id TEXT NOT NULL,
    name TEXT NOT NULL,
    version BIGINT NOT NULL DEFAULT 4,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    hash TEXT,
    storage_key TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('created','uploading','finalized','deleted')),
    expires_at BIGINT,
    created_at BIGINT NOT NULL DEFAULT (strftime('%s','now')),
    updated_at BIGINT NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE TABLE IF NOT EXISTS artifact_uploads (
    id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL,
    upload_id TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK (state IN ('reserved','uploaded','completed')),
    etag TEXT,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL DEFAULT (strftime('%s','now')),
    updated_at BIGINT NOT NULL DEFAULT (strftime('%s','now')),
    FOREIGN KEY (artifact_id) REFERENCES artifact_entries(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS artifact_upload_parts (
    upload_id TEXT NOT NULL,
    block_id TEXT NOT NULL,
    part_number BIGINT NOT NULL,
    size BIGINT NOT NULL,
    etag TEXT NOT NULL,
    created_at BIGINT NOT NULL DEFAULT (strftime('%s','now')),
    updated_at BIGINT NOT NULL DEFAULT (strftime('%s','now')),
    PRIMARY KEY (upload_id, block_id)
);

CREATE INDEX IF NOT EXISTS idx_artifact_entries_active_name
ON artifact_entries (workflow_run_backend_id, workflow_job_run_backend_id, name)
WHERE state != 'deleted';

CREATE INDEX IF NOT EXISTS idx_artifact_entries_run_job
ON artifact_entries (workflow_run_backend_id, workflow_job_run_backend_id);

CREATE INDEX IF NOT EXISTS idx_artifact_upload_parts_upload_part
ON artifact_upload_parts (upload_id, part_number);
