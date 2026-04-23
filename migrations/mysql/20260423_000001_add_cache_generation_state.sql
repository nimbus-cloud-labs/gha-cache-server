CREATE TABLE IF NOT EXISTS cache_state (
    singleton TINYINT PRIMARY KEY,
    current_generation BIGINT NOT NULL,
    CONSTRAINT chk_cache_state_singleton CHECK (singleton = 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO cache_state (singleton, current_generation)
VALUES (1, 1);
