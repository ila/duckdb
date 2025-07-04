-- Step 1: Insert into delta_daily_steps_user
INSERT INTO delta_daily_steps_user
SELECT nickname, city, club,  date, SUM(steps) AS total_steps, _duckdb_ivm_multiplicity
FROM delta_activity_metrics
WHERE _duckdb_ivm_timestamp >= '2025-04-24 09:29:03.284'
GROUP BY nickname, city, club,  date, _duckdb_ivm_multiplicity;

-- Step 2: Update last_update
UPDATE _duckdb_ivm_delta_tables
SET last_update = datetime('now')
WHERE view_name = 'daily_steps_user';

-- Step 3: Insert or replace into daily_steps_user using IVM logic
INSERT OR REPLACE INTO daily_steps_user
WITH ivm_cte AS (
    SELECT nickname, city, club, date,
        SUM(CASE WHEN _duckdb_ivm_multiplicity = 0 THEN -total_steps ELSE total_steps END) AS total_steps
    FROM delta_daily_steps_user
    GROUP BY nickname, city, club, date
)
SELECT delta.nickname, delta.city, delta.date,
       COALESCE(existing.total_steps, 0) + delta.total_steps
FROM ivm_cte AS delta
         LEFT JOIN daily_steps_user AS existing
                   ON existing.nickname = delta.nickname
                       AND existing.city = delta.city
                       AND existing.date = delta.date;

-- Step 4: Delete zeroed rows (not applicable here, but kept for completeness)
DELETE FROM daily_steps_user
WHERE total_steps = 0;

-- Step 5: Clean up deltas
DELETE FROM delta_daily_steps_user;
DELETE FROM delta_activity_metrics;
