CREATE SCHEMA IF NOT EXISTS options_data;

CREATE TABLE IF NOT EXISTS options_data.underlying_symbols (
    underlying_symbol TEXT PRIMARY KEY,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS options_data.contracts (
    contract TEXT PRIMARY KEY,
    underlying_symbol TEXT NOT NULL,
    exp_date DATE,
    expiration_type TEXT,
    option_type TEXT NOT NULL,
    strike DOUBLE PRECISION,
    exchange TEXT,
    currency TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_json JSONB
);

CREATE INDEX IF NOT EXISTS idx_options_contracts_lookup
    ON options_data.contracts (underlying_symbol, exp_date, option_type, strike);

CREATE TABLE IF NOT EXISTS options_data.option_eod (
    contract TEXT NOT NULL,
    tradetime DATE NOT NULL,
    underlying_symbol TEXT NOT NULL,
    exp_date DATE,
    expiration_type TEXT,
    option_type TEXT NOT NULL,
    strike DOUBLE PRECISION,
    exchange TEXT,
    currency TEXT,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    last DOUBLE PRECISION,
    last_size DOUBLE PRECISION,
    previous DOUBLE PRECISION,
    previous_date DATE,
    bid DOUBLE PRECISION,
    bid_date TIMESTAMPTZ,
    bid_size DOUBLE PRECISION,
    ask DOUBLE PRECISION,
    ask_date TIMESTAMPTZ,
    ask_size DOUBLE PRECISION,
    midpoint DOUBLE PRECISION,
    moneyness DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    open_interest DOUBLE PRECISION,
    volatility DOUBLE PRECISION,
    theoretical DOUBLE PRECISION,
    delta DOUBLE PRECISION,
    gamma DOUBLE PRECISION,
    theta DOUBLE PRECISION,
    vega DOUBLE PRECISION,
    rho DOUBLE PRECISION,
    dte INTEGER,
    vol_oi_ratio DOUBLE PRECISION,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_json JSONB,
    PRIMARY KEY (contract, tradetime)
);

CREATE INDEX IF NOT EXISTS idx_option_eod_chain_lookup
    ON options_data.option_eod (underlying_symbol, tradetime, exp_date, option_type, strike);

CREATE INDEX IF NOT EXISTS idx_option_eod_contract_time
    ON options_data.option_eod (contract, tradetime);

CREATE TABLE IF NOT EXISTS options_data.backfill_jobs (
    job_id BIGSERIAL PRIMARY KEY,
    endpoint TEXT NOT NULL,
    status TEXT NOT NULL,
    underlying_symbol TEXT,
    contract TEXT,
    option_type TEXT,
    exp_date_eq DATE,
    exp_date_from DATE,
    exp_date_to DATE,
    tradetime_eq DATE,
    tradetime_from DATE,
    tradetime_to DATE,
    strike_eq DOUBLE PRECISION,
    strike_from DOUBLE PRECISION,
    strike_to DOUBLE PRECISION,
    page_limit INTEGER NOT NULL,
    max_pages_per_task INTEGER,
    max_retries_per_task INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS options_data.backfill_tasks (
    task_id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES options_data.backfill_jobs(job_id),
    endpoint TEXT NOT NULL,
    query_params JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 100,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    page_offset INTEGER NOT NULL DEFAULT 0,
    page_limit INTEGER NOT NULL DEFAULT 1000,
    max_pages INTEGER,
    rows_received INTEGER NOT NULL DEFAULT 0,
    rows_inserted INTEGER NOT NULL DEFAULT 0,
    api_requests_made INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    claimed_by TEXT,
    claimed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_options_backfill_tasks_claim
    ON options_data.backfill_tasks (status, priority, created_at);

CREATE INDEX IF NOT EXISTS idx_options_backfill_tasks_job_status
    ON options_data.backfill_tasks (job_id, status);

CREATE TABLE IF NOT EXISTS options_data.api_request_log (
    request_id BIGSERIAL PRIMARY KEY,
    job_id BIGINT REFERENCES options_data.backfill_jobs(job_id),
    task_id BIGINT REFERENCES options_data.backfill_tasks(task_id),
    endpoint TEXT NOT NULL,
    url_without_token TEXT NOT NULL,
    status_code INTEGER,
    row_count INTEGER,
    elapsed_ms INTEGER,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
