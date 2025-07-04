-- sidra_parser.db
attach if not exists 'sidra_parser.db' as sidra_parser;
update sidra_parser.sidra_view_constraints set sidra_min_agg = 100 where view_name = 'daily_steps_user';
-- LEAST((d.window_client_count::DOUBLE / t.total_client_count) * 100, 100) AS percentage
update sidra_parser.sidra_current_window set sidra_window = 0;
delete from sidra_parser.sidra_clients;

-- activity_metrics.db
attach if not exists 'activity_metrics.db' as activity_metrics;
delete from activity_metrics.sidra_centralized_table_daily_steps_user;
delete from activity_metrics.centralized_daily_steps_user;
-- todo more deletes