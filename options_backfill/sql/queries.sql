-- name: create_job
INSERT INTO options_data.backfill_jobs (
    endpoint,
    status,
    underlying_symbol,
    contract,
    option_type,
    exp_date_eq,
    exp_date_from,
    exp_date_to,
    tradetime_eq,
    tradetime_from,
    tradetime_to,
    strike_eq,
    strike_from,
    strike_to,
    page_limit,
    max_pages_per_task,
    max_retries_per_task
)
VALUES (
    $1, 'pending', $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
)
RETURNING job_id;

-- name: insert_task
INSERT INTO options_data.backfill_tasks (
    job_id,
    endpoint,
    query_params,
    priority,
    max_retries,
    page_limit,
    max_pages
)
VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7)
RETURNING task_id;

-- name: claim_next_task
WITH next_task AS (
    SELECT task_id
    FROM options_data.backfill_tasks
    WHERE status = 'pending'
    ORDER BY priority, created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE options_data.backfill_tasks t
SET
    status = 'running',
    claimed_by = $1,
    claimed_at = NOW(),
    updated_at = NOW()
FROM next_task
WHERE t.task_id = next_task.task_id
RETURNING t.*;

-- name: mark_task_completed
UPDATE options_data.backfill_tasks
SET
    status = 'completed',
    rows_received = rows_received + $2,
    rows_inserted = rows_inserted + $3,
    api_requests_made = api_requests_made + $4,
    updated_at = NOW()
WHERE task_id = $1;

-- name: mark_task_failed
UPDATE options_data.backfill_tasks
SET
    retry_count = retry_count + 1,
    status = CASE
        WHEN retry_count + 1 >= max_retries OR $3 = FALSE THEN 'failed'
        ELSE 'pending'
    END,
    last_error = $2,
    claimed_by = NULL,
    claimed_at = NULL,
    updated_at = NOW()
WHERE task_id = $1;

-- name: set_job_status
UPDATE options_data.backfill_jobs
SET
    status = $2,
    updated_at = NOW()
WHERE job_id = $1;

-- name: summarize_job
SELECT
    j.job_id,
    j.endpoint,
    j.status,
    COUNT(t.task_id) AS requested_tasks,
    COUNT(*) FILTER (WHERE t.status = 'completed') AS completed_tasks,
    COUNT(*) FILTER (WHERE t.status = 'failed') AS failed_tasks,
    COALESCE(SUM(t.rows_inserted), 0) AS inserted_rows,
    COALESCE(SUM(t.api_requests_made), 0) AS api_requests_made,
    COALESCE(array_agg(t.task_id) FILTER (WHERE t.status <> 'completed'), '{}') AS unresolved_tasks
FROM options_data.backfill_jobs j
LEFT JOIN options_data.backfill_tasks t ON t.job_id = j.job_id
WHERE j.job_id = $1
GROUP BY j.job_id, j.endpoint, j.status;

-- name: upsert_option_eod_row
INSERT INTO options_data.option_eod (
    contract,
    tradetime,
    underlying_symbol,
    exp_date,
    option_type,
    strike,
    bid,
    ask,
    midpoint,
    volatility,
    delta,
    gamma,
    theta,
    vega,
    rho,
    fetched_at,
    raw_json
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, NOW(), $16::jsonb
)
ON CONFLICT (contract, tradetime)
DO UPDATE SET
    bid = EXCLUDED.bid,
    ask = EXCLUDED.ask,
    midpoint = EXCLUDED.midpoint,
    volatility = EXCLUDED.volatility,
    delta = EXCLUDED.delta,
    gamma = EXCLUDED.gamma,
    theta = EXCLUDED.theta,
    vega = EXCLUDED.vega,
    rho = EXCLUDED.rho,
    fetched_at = NOW(),
    raw_json = COALESCE(EXCLUDED.raw_json, options_data.option_eod.raw_json);
