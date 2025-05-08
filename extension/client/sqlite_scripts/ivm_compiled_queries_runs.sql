-- ./duckdb runs.db
attach if not exists 'dbname=rdda_client user=ubuntu password=test host=localhost' as rdda_client (type postgres);

create table mv_daily_runs_city as select nickname, city, date, sum(steps) as total_steps, rdda_window,
from rdda_client.rdda_centralized_view_runs
group by nickname, city, date, rdda_window;

create table if not exists delta_rdda_centralized_view_runs as select *, true as _duckdb_ivm_multiplicity, now() as _duckdb_ivm_timestamp
from rdda_client.rdda_centralized_view_runs limit 0;

create table if not exists delta_mv_daily_runs_city as select *, true as _duckdb_ivm_multiplicity from mv_daily_runs_city limit 0;

create unique index mv_daily_runs_city_ivm_index on mv_daily_runs_city(nickname, city, date, rdda_window);