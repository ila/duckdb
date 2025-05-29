-- rdda_parser.db
update rdda_view_constraints set rdda_min_agg = 100 where view_name = 'daily_runs_city';
-- LEAST((d.window_client_count::DOUBLE / t.total_client_count) * 100, 100) AS percentage
update rdda_current_window set rdda_window = 0;

-- runs.db
create unique index i on rdda_centralized_table_daily_runs_city (nickname, city, date, rdda_window);
delete from rdda_centralized_table_daily_runs_city;
delete from mv_daily_runs_city;