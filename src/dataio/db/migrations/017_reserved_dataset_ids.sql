CREATE TABLE IF NOT EXISTS reserved_dataset_ids (
    id SERIAL PRIMARY KEY,
    ds_id TEXT NOT NULL UNIQUE,
    collection_id TEXT NULL,
    note TEXT NULL,
    reserved_by TEXT NOT NULL REFERENCES users(email),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_reserved_dataset_ids_collection_id
    ON reserved_dataset_ids(collection_id);
