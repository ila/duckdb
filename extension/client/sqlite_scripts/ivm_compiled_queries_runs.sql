-- ./duckdb runs.db

CREATE DECENTRALIZED TABLE runs (
    nickname VARCHAR,
    city VARCHAR PROTECTED,
    date DATE PROTECTED,
    start_time TIME,
    end_time TIME,
    steps INT,
    heartbeat_rate INT SENSITIVE,
);

CREATE DECENTRALIZED MATERIALIZED VIEW daily_runs_city AS
SELECT nickname, city, date, SUM(steps) AS total_steps
FROM runs
GROUP BY nickname, city, date
    WINDOW 24
    TTL 72
    MINIMUM AGGREGATION 1
    REFRESH 24;

attach if not exists 'rdda_parser.db' as rdda_parser;
insert or replace into rdda_parser.rdda_tables values ('mv_daily_runs_city', 1, '', true);
insert or replace into rdda_parser.rdda_view_constraints values ('mv_daily_runs_city', 24, 72, 24, 0, now());
insert or replace into rdda_parser.rdda_current_window values ('mv_daily_runs_city', 0, now());

create table if not exists _duckdb_ivm_views (view_name varchar primary key, sql_string varchar, type tinyint, last_update timestamp);
create table if not exists _duckdb_ivm_delta_tables (view_name varchar, table_name varchar, last_update timestamp, primary key(view_name, table_name));
--insert or replace into _duckdb_ivm_views values ('mv_daily_runs_city', '', 0, now());
--insert into _duckdb_ivm_delta_tables values ('mv_daily_runs_city', 'd_rdda_centralized_table_runs', now());

attach if not exists 'dbname=rdda_client user=ubuntu password=test host=localhost' as rdda_client (type postgres);

create table mv_daily_runs_city as select nickname, city, date, sum(steps) as total_steps, rdda_window
from rdda_client.rdda_centralized_view_runs
group by nickname, city, date, rdda_window;

create table if not exists rdda_client.d_rdda_centralized_view_runs as select *, true as _duckdb_ivm_multiplicity, now() as _duckdb_ivm_timestamp
from rdda_client.rdda_centralized_view_runs limit 0;

create table if not exists d_mv_daily_runs_city as select *, true as _duckdb_ivm_multiplicity from mv_daily_runs_city limit 0;

create unique index mv_daily_runs_city_ivm_index on mv_daily_runs_city(nickname, city, date, rdda_window);