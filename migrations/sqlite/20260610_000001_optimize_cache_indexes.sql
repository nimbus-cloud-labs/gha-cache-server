-- Improve lookup performance for cache entries and upload joins
DROP INDEX IF EXISTS idx_cache_entries_key_version;
CREATE INDEX IF NOT EXISTS idx_cache_entries_key_version_created_at ON cache_entries (cache_key, cache_version, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_cache_entries_key_created_at ON cache_entries (cache_key, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_cache_uploads_entry_id ON cache_uploads (entry_id);
