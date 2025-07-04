CREATE TABLE IF NOT EXISTS _duckdb_ivm_views
(
    view_name
    VARCHAR
    PRIMARY
    KEY,
    sql_string
    VARCHAR,
    type
    INTEGER, -- Changed from tinyint
    last_update
    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS _duckdb_ivm_delta_tables
(
    view_name
    VARCHAR,
    table_name
    VARCHAR,
    last_update
    TIMESTAMP,
    PRIMARY
    KEY
(
    view_name,
    table_name
)
    );

INSERT
OR REPLACE INTO _duckdb_ivm_views VALUES (
    'daily_steps_user',
    'select nickname, city, club, date, sum(steps) as total_steps from activity_metrics group by nickname, city, club, date',
    0,
    CURRENT_TIMESTAMP  -- Changed from now()
);

INSERT INTO _duckdb_ivm_delta_tables
VALUES ('daily_steps_user',
        'delta_activity_metrics',
        CURRENT_TIMESTAMP -- Changed from now()
       );