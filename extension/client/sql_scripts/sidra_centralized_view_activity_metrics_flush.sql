attach if not exists 'dbname=sidra user=sidra_user password=test host=localhost' as sidra_client (type postgres);
attach 'sidra_parser.db' as sidra_parser (read_only);
attach 'activity_metrics.db' as activity_metrics;

insert into activity_metrics.d_sidra_centralized_view_activity_metrics by name
select nickname, city, club, date, total_steps as total_steps, sidra_window, client_id
from sidra_client.sidra_centralized_view_activity_metrics
group by nickname, city, club, date, sidra_window, client_id;

-- Skipping updating metadata

-- Step 3: Insert or replace into daily_steps_user using IVM logic
INSERT OR REPLACE INTO centralized_daily_steps_user
WITH ivm_cte AS (
    SELECT nickname, city, club, date, sidra_window,
        SUM(CASE WHEN _duckdb_ivm_multiplicity = 0 THEN -total_steps ELSE total_steps END) AS total_steps
    FROM d_centralized_daily_steps_user
    GROUP BY nickname, city, club, date, sidra_window
)
SELECT d.city, d.date,
    COALESCE(existing.total_steps, 0) + d.total_steps, d.sidra_window
FROM ivm_cte AS d
LEFT JOIN centralized_daily_steps_user AS existing
ON existing.city = d.city
AND existing.date = d.date
AND existing.sidra_window = d.sidra_window;

INSERT OR REPLACE INTO centralized_daily_steps_club
WITH ivm_cte AS (
    SELECT club, date, sidra_window,
        SUM(CASE WHEN _duckdb_ivm_multiplicity = 0 THEN -total_steps ELSE total_steps END) AS total_steps
    FROM d_centralized_daily_steps_user
    GROUP BY club, date, sidra_window
)
SELECT d.club, d.date,
    COALESCE(existing.total_steps, 0) + d.total_steps, d.sidra_window
FROM ivm_cte AS d
LEFT JOIN centralized_daily_steps_club AS existing
ON existing.club = d.club
AND existing.date = d.date
AND existing.sidra_window = d.sidra_window;

-- Step 4: Delete zeroed rows
DELETE FROM centralized_daily_steps_user
WHERE total_steps = 0;

-- Step 5: Clean up deltas
DELETE FROM sidra_client.sidra_centralized_view_activity_metrics;
DELETE FROM d_centralized_daily_steps_user;
DELETE FROM d_centralized_daily_steps_club;

