create table example_count as select mode, count(*) as count_star from p.public.example_table group by mode;

create table if not exists p.public.delta_example_table(id integer, mode varchar not null, value integer not null, _duckdb_ivm_multiplicity boolean, timestamp timestamp default now());

create table if not exists delta_example_count as select *, true as _duckdb_ivm_multiplicity from example_count limit 0;

