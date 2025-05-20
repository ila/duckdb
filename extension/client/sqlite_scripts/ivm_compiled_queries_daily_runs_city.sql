-- update rdda_view_constraints set rdda_min_agg = 100 where view_name = 'daily_runs_city';
-- LEAST((d.window_client_count::DOUBLE / t.total_client_count) * 100, 100) AS percentage
-- update rdda_current_window set rdda_window = 0;

CREATE TABLE runs
(
    nickname       TEXT,
    city           TEXT,
    date           TEXT, -- SQLite uses TEXT for storing dates in ISO 8601 format (YYYY-MM-DD)
    start_time     TEXT, -- Also stored as TEXT in HH:MM:SS format
    end_time       TEXT,
    steps          INTEGER,
    heartbeat_rate INTEGER
);

-- Create materialized view table (valid in SQLite)
CREATE TABLE daily_runs_city AS
SELECT nickname, city, date, sum
(
    steps
) AS total_steps
    FROM runs
    GROUP BY nickname, city, date;

-- Create empty delta table with schema from runs (fixed for SQLite)
CREATE TABLE IF NOT EXISTS delta_runs AS
SELECT *,
       1               AS _duckdb_ivm_multiplicity, -- Changed from true (SQLite uses 1/0 for booleans)
       datetime('now') AS _duckdb_ivm_timestamp     -- Changed from now
FROM runs LIMIT 0;

-- Create empty delta table for the materialized view (fixed for SQLite)
CREATE TABLE IF NOT EXISTS delta_daily_runs_city AS
SELECT *,
       1 AS _duckdb_ivm_multiplicity -- Changed from true
FROM daily_runs_city LIMIT 0;

CREATE TRIGGER after_runs_insert
    AFTER INSERT
    ON runs
    FOR EACH ROW
BEGIN
    INSERT INTO delta_runs (nickname,
                            city,
                            date,
                            start_time,
                            end_time,
                            steps,
                            heartbeat_rate,
                            _duckdb_ivm_multiplicity,
                            _duckdb_ivm_timestamp)
    VALUES (NEW.nickname,
            NEW.city,
            NEW.date,
            NEW.start_time,
            NEW.end_time,
            NEW.steps,
            NEW.heartbeat_rate,
            1, -- Default multiplicity (1 for true)
            datetime('now') -- Current timestamp
           );
END;