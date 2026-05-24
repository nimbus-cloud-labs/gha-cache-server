DROP INDEX idx_artifact_entries_active_name ON artifact_entries;

CREATE INDEX idx_artifact_entries_active_name
ON artifact_entries (workflow_run_backend_id, name(191), state);
