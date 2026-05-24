DROP INDEX IF EXISTS idx_artifact_entries_active_name;

CREATE INDEX IF NOT EXISTS idx_artifact_entries_active_name
ON artifact_entries (workflow_run_backend_id, name)
WHERE state != 'deleted';
