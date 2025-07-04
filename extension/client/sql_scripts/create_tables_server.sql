-- ./duckdb activity_metrics.db
-- this file contains all the tables for both benchmark executions (centralized and decentralized)
attach if not exists 'sidra_parser.db' as sidra_parser;
attach if not exists 'dbname=sidra user=sidra_user password=test host=localhost' as sidra_client (type postgres);

-- we create the decentralized tables so the shadow parser is aware of the schema
-- when creating the upstream materialized view
CREATE DECENTRALIZED TABLE activity_metrics (
    nickname VARCHAR,
    city VARCHAR PROTECTED,
    club VARCHAR,
    date DATE PROTECTED,
    start_time TIME,
    end_time TIME,
    steps INT,
    heartbeat_rate INT SENSITIVE,
);

CREATE DECENTRALIZED MATERIALIZED VIEW daily_steps_user AS
SELECT nickname, city, club, date, SUM(steps) AS total_steps
FROM activity_metrics
GROUP BY nickname, city, club, date
    WINDOW 24
    TTL 72
    MINIMUM AGGREGATION 1
    REFRESH 24;

CREATE CENTRALIZED MATERIALIZED VIEW daily_steps_club AS
SELECT club, date, SUM(steps) AS total_steps
FROM activity_metrics
GROUP BY club, date
    REFRESH 24;

-- updating metadata tables
insert or replace into sidra_parser.sidra_tables values ('centralized_daily_steps_user', 1, '', true);
insert or replace into sidra_parser.sidra_view_constraints values ('centralized_daily_steps_user', 24, 72, 24, 0, now());
insert or replace into sidra_parser.sidra_current_window values ('centralized_daily_steps_user', 0, now());
insert or replace into sidra_parser.sidra_tables values ('centralized_daily_steps_club', 1, '', false);
insert or replace into sidra_parser.sidra_view_constraints values ('centralized_daily_steps_club', 24, 0, 0, 0, now());
insert or replace into sidra_parser.sidra_current_window values ('centralized_daily_steps_club', 0, now());

create table if not exists _duckdb_ivm_views (view_name varchar primary key, sql_string varchar, type tinyint, last_update timestamp);
create table if not exists _duckdb_ivm_delta_tables (view_name varchar, table_name varchar, last_update timestamp, primary key(view_name, table_name));

-- delta tables
create table if not exists d_centralized_daily_steps_user as select *, true as _duckdb_ivm_multiplicity, now() as _duckdb_ivm_timestamp
from sidra_client.sidra_centralized_view_daily_steps_users limit 0;
create table if not exists d_daily_steps_club as select *, true as _duckdb_ivm_multiplicity, now() as _duckdb_ivm_timestamp
from daily_steps_club limit 0;

-- now we create the tables for the fully centralized example
CREATE TABLE IF NOT EXISTS sidra_client.sidra_centralized_view_activity_metrics (
    nickname TEXT,
    city TEXT,
    club TEXT,
    date TEXT,
    start_time TEXT,
    end_time TEXT,
    steps INTEGER,
    heartbeat_rate INTEGER,
    generation TIMESTAMPTZ,
    arrival TIMESTAMPTZ,
    sidra_window INT,
    client_id BIGINT,
    action SMALLINT);

-- equivalent of daily_steps_user to test in a fully centralized architecture
create table centralized_daily_steps_user as
select nickname, city, club, date, sum(steps) as total_steps
from sidra_client.sidra_centralized_view_activity_metrics
group by nickname, city, club, date;

-- equivalent of daily_steps_club to test in a fully centralized architecture
create table centralized_daily_steps_club as
select club, date, sum(steps) as total_steps
from sidra_client.centralized_daily_steps_user
group by club, date;

-- manually creating delta tables now (to simplify reproducibility)
-- on postgres
create table if not exists sidra_client.d_sidra_centralized_view_activity_metrics as select *, true as _duckdb_ivm_multiplicity, now() as _duckdb_ivm_timestamp
from sidra_client.sidra_centralized_view_activity_metrics limit 0;

-- on duckdb
create table if not exists d_centralized_daily_steps_user as select *, true as _duckdb_ivm_multiplicity, now() as _duckdb_ivm_timestamp
    from centralized_view_daily_steps_user limit 0;

create table if not exists d_centralized_daily_steps_club as select *, true as _duckdb_ivm_multiplicity, now() as _duckdb_ivm_timestamp
    from d_centralized_daily_steps_club limit 0;

create unique index daily_steps_user_ivm_index on daily_steps_user(nickname, city, club, date);
create unique index daily_steps_club_ivm_index on daily_steps_club(club, date);
create unique index centralized_daily_steps_user_ivm_index on centralized_daily_steps_user(nickname, city, club, date);
create unique index centralized_daily_steps_club_ivm_index on centralized_daily_steps_club(club, date);