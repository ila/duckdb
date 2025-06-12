create table if not exists _duckdb_ivm_views (view_name varchar primary key, sql_string varchar, type tinyint, filter bool);

create table if not exists _duckdb_ivm_delta_tables (view_name varchar, table_name varchar, last_update timestamp, primary key(view_name, table_name));

insert or replace into _duckdb_ivm_views values ('example_count', 'select mode, count(*) as count_star from p.public.example_table group by mode', 0, 0);

insert into _duckdb_ivm_delta_tables values ('example_count', 'delta_example_table', now());

