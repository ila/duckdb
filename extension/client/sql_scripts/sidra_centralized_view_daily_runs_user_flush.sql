attach if not exists 'dbname=sidra user=sidra_user password=test host=localhost' as sidra_client (type postgres);
attach 'sidra_parser.db' as sidra_parser;
attach 'activity_metrics.db' as activity_metrics;

WITH stats AS (
    SELECT sidra_window, sidra_ttl FROM sidra_parser.sidra_view_constraints
    WHERE view_name = 'daily_steps_user'),
     current_window AS (
         SELECT sidra_window FROM sidra_parser.sidra_current_window
         WHERE view_name = 'sidra_centralized_view_daily_steps_user'),
     threshold_window AS (
         SELECT cw.sidra_window - (s.sidra_ttl / s.sidra_window) AS expired_window
         FROM current_window cw, stats s)
update sidra_client.sidra_centralized_view_daily_steps_user x
set action = 2
where x.sidra_window > (select expired_window from threshold_window);

insert into activity_metrics.d_sidra_centralized_table_daily_steps_user by name
select nickname, city, club, date, total_steps, sidra_window, client_id
from sidra_client.sidra_centralized_view_daily_steps_user
where action = 2
group by nickname, city, club, date, sidra_window, client_id;

delete from sidra_client.sidra_centralized_view_daily_steps_user
where action = 2;

INSERT INTO activity_metrics.sidra_centralized_table_daily_steps_user
WITH ivm_cte AS (
    SELECT nickname, city, club, date, sidra_window,
        SUM(CASE WHEN _duckdb_ivm_multiplicity = false THEN -total_steps ELSE total_steps END) AS total_steps
    FROM activity_metrics.d_sidra_centralized_table_daily_steps_user
    GROUP BY nickname, city, club, date, sidra_window
)
SELECT d.nickname, d.city, d.club, d.date,
       COALESCE(existing.total_steps, 0) + d.total_steps, d.sidra_window
FROM ivm_cte AS d
LEFT JOIN activity_metrics.sidra_centralized_table_daily_steps_user AS existing
ON existing.nickname = d.nickname
AND existing.city = d.city
AND existing.club = d.club
AND existing.date = d.date
AND existing.sidra_window = d.sidra_window;

delete from sidra_parser.sidra_clients where last_update < today() - interval 7 day;

WITH distinct_clients_per_window AS (
    SELECT sidra_window, COUNT(DISTINCT client_id) AS window_client_count
    FROM sidra_centralized_table_daily_steps_user
    GROUP BY sidra_window),
     total_clients AS (
         SELECT COUNT(DISTINCT id) AS total_client_count FROM sidra_parser.sidra_clients),
     percentages AS (
         SELECT d.sidra_window, LEAST((d.window_client_count::DOUBLE / t.total_client_count) * 100, 100) AS percentage
         FROM distinct_clients_per_window d, total_clients t)
UPDATE sidra_centralized_table_daily_steps_user sidra_metadata_update
SET responsiveness = p.percentage
    FROM percentages p
WHERE sidra_metadata_update.sidra_window = p.sidra_window;

WITH stats AS (
    SELECT sidra_window, sidra_ttl FROM sidra_parser.sidra_view_constraints
    WHERE view_name = 'daily_steps_user'),
     current_window AS (
         SELECT sidra_window FROM sidra_parser.sidra_current_window
         WHERE view_name = 'sidra_centralized_view_daily_steps_user'),
     threshold_window AS (
         SELECT cw.sidra_window - (s.sidra_ttl / s.sidra_window) AS expired_window
         FROM current_window cw, stats s),
     to_discard AS (
         SELECT sidra_window, COUNT(*) AS discarded_count
         FROM  sidra_client.sidra_centralized_view_daily_steps_user
         WHERE sidra_window <= (SELECT expired_window FROM threshold_window)
         GROUP BY sidra_window),
     to_keep AS (
         SELECT sidra_window, COUNT(*) AS kept_count
         FROM sidra_centralized_table_daily_steps_user
         GROUP BY sidra_window),
     combined AS (
         SELECT
             COALESCE(d.sidra_window, k.sidra_window) AS sidra_window,
             COALESCE(d.discarded_count, 0) AS discarded,
             COALESCE(k.kept_count, 0) AS kept
         FROM to_discard d
                  FULL OUTER JOIN to_keep k
                                  ON d.sidra_window = k.sidra_window),
     discard_stats AS (
         SELECT sidra_window, discarded, kept,
                CASE WHEN (discarded + kept) > 0 THEN (kept::decimal / (discarded + kept)) * 100 ELSE 100 END AS discard_percentage
         FROM combined)
UPDATE sidra_centralized_table_daily_steps_user sidra_metadata_update
SET completeness = ds.discard_percentage
    FROM discard_stats ds
WHERE sidra_metadata_update.sidra_window = sidra_metadata_update.sidra_window;

WITH stats AS (
    SELECT sidra_window, sidra_ttl FROM sidra_parser.sidra_view_constraints
    WHERE view_name = 'daily_steps_user'),
     current_window AS (
         SELECT sidra_window FROM sidra_parser.sidra_current_window
         WHERE view_name = 'sidra_centralized_view_daily_steps_user'),
     threshold_window AS (
         SELECT cw.sidra_window - (s.sidra_ttl / s.sidra_window) AS expired_window
         FROM current_window cw, stats s)
delete from sidra_client.sidra_centralized_view_daily_steps_user where sidra_window <= (SELECT expired_window FROM threshold_window);

WITH buffer_counts AS (
    SELECT sidra_window, COUNT(*) AS buffer_count
    FROM  sidra_client.sidra_centralized_view_daily_steps_user
    GROUP BY sidra_window),
     centralized_counts AS (
         SELECT sidra_window, COUNT(*) AS centralized_count
         FROM sidra_centralized_table_daily_steps_user
         GROUP BY sidra_window),
     combined AS (
         SELECT
             COALESCE(b.sidra_window, c.sidra_window) AS sidra_window,
             COALESCE(b.buffer_count, 0) AS buffer,
             COALESCE(c.centralized_count, 0) AS centralized
         FROM buffer_counts b
                  FULL OUTER JOIN centralized_counts c
                                  ON b.sidra_window = c.sidra_window),
     buffer_stats AS (
         SELECT sidra_window, buffer, centralized,
                CASE WHEN (buffer + centralized) > 0 THEN (buffer::decimal / (buffer + centralized)) * 100 ELSE 0 END AS buffer_percentage
         FROM combined) UPDATE sidra_centralized_table_daily_steps_user sidra_metadata_update
SET buffer_size = bs.buffer_percentage
FROM buffer_stats bs
WHERE sidra_metadata_update.sidra_window = bs.sidra_window;

insert into d_daily_steps_club
select city, date, sum(total_steps) as total_steps, _duckdb_ivm_multiplicity
from activity_metrics.main.d_sidra_centralized_table_daily_steps_user 
group by city, date, _duckdb_ivm_multiplicity;

insert or replace into daily_steps_club
with ivm_cte AS (
    select city, date,
        sum(case when _duckdb_ivm_multiplicity = false then -total_steps else total_steps end) as total_steps
    from d_daily_steps_club
    group by city, date)
select d_daily_steps_club.city, d_daily_steps_club.date,
       sum(coalesce(daily_steps_club.total_steps, 0) + d_daily_steps_club.total_steps)
from ivm_cte as d_daily_steps_club
         left join daily_steps_club on daily_steps_club.city = d_daily_steps_club.city and daily_steps_club.date = d_daily_steps_club.date
group by d_daily_steps_club.city, d_daily_steps_club.date;

delete from daily_steps_club where total_steps = 0;
delete from d_daily_steps_club;
delete from d_sidra_centralized_table_daily_steps_user;
