CREATE TABLE activity_metrics
(
    nickname       TEXT,
    city           TEXT,
    club           TEXT,
    date           TEXT, -- SQLite uses TEXT for storing dates in ISO 8601 format (YYYY-MM-DD)
    start_time     TEXT, -- Also stored as TEXT in HH:MM:SS format
    end_time       TEXT,
    steps          INTEGER,
    heartbeat_rate INTEGER
);

todo tomorrow
     write readme
     fix flush scripts (add d tables)
     add clubs data

-- Create materialized view table (valid in SQLite)
CREATE TABLE daily_steps_user AS
SELECT nickname, city, club, date, sum
(
    steps
) AS total_steps
    FROM activity_metrics
    GROUP BY nickname, club, city, date;

-- Create empty delta table with schema from activity_metrics (fixed for SQLite)
CREATE TABLE IF NOT EXISTS delta_activity_metrics AS
SELECT *,
       1               AS _duckdb_ivm_multiplicity, -- Changed from true (SQLite uses 1/0 for booleans)
       datetime('now') AS _duckdb_ivm_timestamp     -- Changed from now
FROM activity_metrics LIMIT 0;

-- Create empty delta table for the materialized view (fixed for SQLite)
CREATE TABLE IF NOT EXISTS delta_daily_steps_user AS
SELECT *,
       1 AS _duckdb_ivm_multiplicity -- Changed from true
FROM daily_steps_user LIMIT 0;

CREATE TRIGGER after_activity_metrics_insert
    AFTER INSERT
    ON activity_metrics
    FOR EACH ROW
BEGIN
    INSERT INTO delta_activity_metrics (nickname,
                            city,
                            club,
                            date,
                            start_time,
                            end_time,
                            steps,
                            heartbeat_rate,
                            _duckdb_ivm_multiplicity,
                            _duckdb_ivm_timestamp)
    VALUES (NEW.nickname,
            NEW.city,
            NEW.club,
            NEW.date,
            NEW.start_time,
            NEW.end_time,
            NEW.steps,
            NEW.heartbeat_rate,
            1, -- Default multiplicity (1 for true)
            datetime('now') -- Current timestamp
           );
END;