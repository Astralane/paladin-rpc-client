CREATE TABLE IF NOT EXISTS auction_results (
    id SERIAL PRIMARY KEY,
    tx TEXT NOT NULL,
    slot BIGINT NOT NULL,
    identity TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT now()
);