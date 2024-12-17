DROP TABLE IF EXISTS blocks CASCADE;
CREATE TABLE blocks (
    id TEXT PRIMARY KEY,
    "number" SERIAL NOT NULL UNIQUE,
    hash TEXT NOT NULL UNIQUE DEFAULT gen_random_uuid()::text,
    parent_hash TEXT,
    nonce TEXT,
    transactions_root TEXT,
    state_root TEXT,
    receipts_root TEXT,
    miner TEXT,
    difficulty TEXT,
    total_difficulty TEXT,
    size TEXT,
    gas_limit TEXT,
    gas_used TEXT,
    timestamp BIGINT,
    transaction_count INT,
    item_timestamp TIMESTAMP
);


DROP TABLE IF EXISTS transactions CASCADE;
CREATE TABLE transactions (
    id TEXT PRIMARY KEY,
    transaction_hash TEXT NOT NULL UNIQUE DEFAULT gen_random_uuid()::text,
    nonce INT,
    transaction_index INT,
    from_address TEXT,
    to_address TEXT,
    value TEXT,
    gas TEXT,
    gas_price TEXT,
    input TEXT,
    block_timestamp BIGINT,
    block_number INT REFERENCES blocks("number"),
    block_hash TEXT REFERENCES blocks(hash),
    receipt_cumulative_gas_used TEXT,
    receipt_gas_used TEXT,
    receipt_contract_address TEXT,
    receipt_root TEXT,
    receipt_status INT,
    item_timestamp TIMESTAMP
);

DROP TABLE IF EXISTS token_transfers CASCADE;
CREATE TABLE token_transfers (
    contract_address VARCHAR(255),
    transaction_hash VARCHAR(255),
    log_index INT,
    block_number INT,
    from_address VARCHAR(255),
    to_address VARCHAR(255),
    value DECIMAL
);

DROP TABLE IF EXISTS internal_transactions CASCADE;
CREATE TABLE internal_transactions (
    id TEXT PRIMARY KEY,
    hash TEXT REFERENCES transactions(transaction_hash),
    block_number INT REFERENCES blocks("number"),
    from_address TEXT,
    to_address TEXT,
    value TEXT,
    contract_address TEXT,
    input TEXT,
    type TEXT,
    gas TEXT,
    gas_used TEXT,
    trace_id TEXT,
    is_error INT,
    err_code TEXT
);


DROP TABLE IF EXISTS events CASCADE;
CREATE TABLE events (
    id TEXT PRIMARY KEY,
    block_number INT REFERENCES blocks("number"),
    block_timestamp BIGINT,
    contract_address TEXT,
    event_type TEXT,
    log_index INT,
    transaction_hash TEXT REFERENCES transactions(transaction_hash),
    "user" TEXT,
    wallet TEXT,
    reserve TEXT,
    amount DOUBLE PRECISION,
    liquidity TEXT,
    protocol_fees_token0 TEXT,
    protocol_fees_token1 TEXT,
    recipient TEXT,
    sender TEXT,
    sqrt_price_x96 TEXT,
    tick TEXT,
    topic TEXT
);


DROP TABLE IF EXISTS dex_events CASCADE;
CREATE TABLE dex_events (
    id TEXT PRIMARY KEY,
    block_number INT REFERENCES blocks("number"),
    block_timestamp BIGINT,
    contract_address TEXT,
    event_type TEXT,
    log_index INT,
    transaction_hash TEXT REFERENCES transactions(transaction_hash),
    "user" TEXT,
    wallet TEXT,
    reserve TEXT,
    amount DOUBLE PRECISION,
    liquidity TEXT,
    protocol_fees_token0 TEXT,
    protocol_fees_token1 TEXT,
    recipient TEXT,
    sender TEXT,
    sqrt_price_x96 TEXT,
    tick TEXT,
    topic TEXT
);


DROP TABLE IF EXISTS lending_events CASCADE;
CREATE TABLE lending_events (
    id TEXT PRIMARY KEY,
    block_number INT REFERENCES blocks("number"),
    block_timestamp BIGINT,
    contract_address TEXT,
    event_type TEXT,
    log_index INT,
    transaction_hash TEXT REFERENCES transactions(transaction_hash),
    "user" TEXT,
    wallet TEXT,
    reserve TEXT,
    amount DOUBLE PRECISION,
    liquidity TEXT,
    protocol_fees_token0 TEXT,
    protocol_fees_token1 TEXT,
    recipient TEXT,
    sender TEXT,
    sqrt_price_x96 TEXT,
    tick TEXT,
    topic TEXT
);
