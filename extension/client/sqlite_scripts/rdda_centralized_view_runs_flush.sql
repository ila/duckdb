attach if not exists 'dbname=rdda_client user=ubuntu password=test host=localhost' as rdda_client (type postgres);
attach 'rdda_parser.db' as rdda_parser (read_only);

WITH stats AS (
    SELECT rdda_window, rdda_ttl FROM rdda_parser.rdda_view_constraints
    WHERE view_name = 'mv_daily_runs_city'),
     current_window AS (
         SELECT rdda_window FROM rdda_parser.rdda_current_window
         WHERE view_name = 'mv_daily_runs_city'),
     threshold_window AS (
         SELECT (cw.rdda_window - s.rdda_ttl) / s.rdda_window AS expired_window
         FROM current_window cw, stats s
     )
INSERT INTO rdda_client.d_rdda_centralized_view_runs
SELECT *, true AS _duckdb_ivm_multiplicity, now() as _duckdb_ivm_timestamp
FROM rdda_client.rdda_centralized_view_runs
WHERE rdda_window > (SELECT expired_window FROM threshold_window);

-- Now IVM

-- Step 1: Insert into d_daily_runs_city
INSERT INTO d_mv_daily_runs_city
SELECT nickname, city, date, SUM(steps) AS total_steps, rdda_window, _duckdb_ivm_multiplicity
FROM rdda_client.d_rdda_centralized_view_runs
GROUP BY nickname, city, date, rdda_window, _duckdb_ivm_multiplicity;

-- Skipping updating metadata for now

-- Step 3: Insert or replace into daily_runs_city using IVM logic
INSERT OR REPLACE INTO mv_daily_runs_city
WITH ivm_cte AS (
    SELECT nickname, city, date, rdda_window,
        SUM(CASE WHEN _duckdb_ivm_multiplicity = 0 THEN -total_steps ELSE total_steps END) AS total_steps
    FROM d_mv_daily_runs_city
    GROUP BY nickname, city, date, rdda_window
)
SELECT d.nickname, d.city, d.date,
       COALESCE(existing.total_steps, 0) + d.total_steps, d.rdda_window
FROM ivm_cte AS d
         LEFT JOIN mv_daily_runs_city AS existing
                   ON existing.nickname = d.nickname
                       AND existing.city = d.city
                       AND existing.date = d.date
                       AND existing.rdda_window = d.rdda_window;

-- Step 4: Delete zeroed rows
DELETE FROM mv_daily_runs_city
WHERE total_steps = 0;

-- Step 5: Clean up deltas
DELETE FROM rdda_client.rdda_centralized_view_runs;

DELETE FROM d_mv_daily_runs_city;

DELETE FROM rdda_client.d_rdda_centralized_view_runs;
