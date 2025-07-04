attach if not exists 'dbname=sidra user=sidra_user password=test host=localhost' as sidra_client (type postgres);
attach 'sidra_parser.db' as sidra_parser (read_only);

WITH stats AS (
    SELECT sidra_window, sidra_ttl FROM sidra_parser.sidra_view_constraints
    WHERE view_name = 'centralized_daily_steps_user'),
     current_window AS (
         SELECT sidra_window FROM sidra_parser.sidra_current_window
         WHERE view_name = 'centralized_daily_steps_user'),
     threshold_window AS (
         SELECT (cw.sidra_window - s.sidra_ttl) / s.sidra_window AS expired_window
         FROM current_window cw, stats s
     )
INSERT INTO d_centralized_daily_steps_user
SELECT city, date, SUM(steps) AS total_steps, true AS _duckdb_ivm_multiplicity, now() as _duckdb_ivm_timestamp
FROM sidra_client.sidra_centralized_view_activity_metrics
WHERE sidra_window > (SELECT expired_window FROM threshold_window)
GROUP BY city, date, sidra_window, _duckdb_ivm_multiplicity;

-- Skipping updating metadata for now

-- Step 3: Insert or replace into daily_steps_user using IVM logic
INSERT OR REPLACE INTO centralized_daily_steps_user
WITH ivm_cte AS (
    SELECT city, date, sidra_window,
        SUM(CASE WHEN _duckdb_ivm_multiplicity = 0 THEN -total_steps ELSE total_steps END) AS total_steps
    FROM d_centralized_daily_steps_user
    GROUP BY city, date, sidra_window
)
SELECT d.city, d.date,
       COALESCE(existing.total_steps, 0) + d.total_steps, d.sidra_window
FROM ivm_cte AS d
         LEFT JOIN centralized_daily_steps_user AS existing
                   ON existing.city = d.city
                       AND existing.date = d.date
                       AND existing.sidra_window = d.sidra_window;

-- Step 4: Delete zeroed rows
DELETE FROM centralized_daily_steps_user
WHERE total_steps = 0;

-- Step 5: Clean up deltas
DELETE FROM sidra_client.sidra_centralized_view_activity_metrics;

DELETE FROM d_centralized_daily_steps_user;
