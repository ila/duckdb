CREATE MATERIALIZED VIEW query_groups AS SELECT group_index, SUM(group_value) AS total_value FROM groups GROUP BY group_index;
