CREATE TABLE IF NOT EXISTS cache_state (
    singleton SMALLINT PRIMARY KEY CHECK (singleton = 1),
    current_generation BIGINT NOT NULL
);

INSERT INTO cache_state (singleton, current_generation)
VALUES (1, 1)
ON CONFLICT (singleton) DO NOTHING;
