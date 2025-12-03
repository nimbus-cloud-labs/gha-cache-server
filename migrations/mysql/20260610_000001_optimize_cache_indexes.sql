-- Improve lookup performance for cache entries and upload joins
DROP INDEX idx_cache_entries_key_version ON cache_entries;
CREATE INDEX idx_cache_entries_key_version_created_at ON cache_entries (cache_key(191), cache_version(191), created_at DESC);

CREATE INDEX idx_cache_entries_key_created_at ON cache_entries (cache_key(191), created_at DESC);

CREATE INDEX idx_cache_uploads_entry_id ON cache_uploads (entry_id);
