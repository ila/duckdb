-- Step 1: Insert into delta_daily_runs_city
INSERT INTO delta_daily_runs_city
SELECT nickname, city, date, SUM(steps) AS total_steps, _duckdb_ivm_multiplicity
FROM delta_runs
GROUP BY nickname, city, date, _duckdb_ivm_multiplicity;

-- Step 3: Insert or replace into daily_runs_city using IVM logic
INSERT OR REPLACE INTO daily_runs_city
WITH ivm_cte AS (
    SELECT nickname, city, date,
        SUM(CASE WHEN _duckdb_ivm_multiplicity = 0 THEN -total_steps ELSE total_steps END) AS total_steps
    FROM delta_daily_runs_city
    GROUP BY nickname, city, date
)
SELECT delta.nickname, delta.city, delta.date,
       COALESCE(existing.total_steps, 0) + delta.total_steps
FROM ivm_cte AS delta
         LEFT JOIN daily_runs_city AS existing
                   ON existing.nickname = delta.nickname
                       AND existing.city = delta.city
                       AND existing.date = delta.date;


-- Step 5: Clean up deltas
DELETE FROM delta_daily_runs_city;

DELETE FROM delta_runs;
