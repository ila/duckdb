CREATE DECENTRALIZED TABLE activity_metrics (
    nickname VARCHAR,
    city VARCHAR PROTECTED,
    date DATE PROTECTED,
    start_time TIME,
    end_time TIME,
    steps INT,
    heartbeat_rate INT SENSITIVE,
);

CREATE DECENTRALIZED MATERIALIZED VIEW daily_steps_user AS
    SELECT nickname, city, date, SUM(steps) AS total_steps
    FROM activity_metrics
    GROUP BY nickname, city, date
    WINDOW 24
    TTL 72
    MINIMUM AGGREGATION 100
    REFRESH 24;

CREATE CENTRALIZED MATERIALIZED VIEW daily_steps AS
    SELECT city, date, SUM(total_steps) AS total_steps
    FROM daily_steps_user
    GROUP BY city, date
    REFRESH 24;

-- todo incrementalize this kind of query
CREATE CENTRALIZED MATERIALIZED VIEW weekly_activity_metrics_city AS
    SELECT nickname, city, week(date), SUM(total_steps) AS total_steps
    FROM daily_steps_user
    GROUP BY nickname, city, week(date)
    REFRESH 24;

INSERT INTO activity_metrics (nickname, city, date, start_time, end_time, steps, heartbeat_rate) VALUES
-- Day 1
('runner42', 'Berlin', '2025-02-22', '08:00:00', '09:00:00', 7520, 140),
('runner42', 'Berlin', '2025-02-22', '17:00:00', '18:00:00', 6780, 137),
-- Day 2
('runner42', 'Berlin', '2025-02-23', '07:45:00', '08:45:00', 7890, 135),
('runner42', 'Berlin', '2025-02-23', '18:15:00', '19:15:00', 7300, 136),
-- Day 3
('runner42', 'Berlin', '2025-02-24', '06:30:00', '07:30:00', 8040, 142),
('runner42', 'Berlin', '2025-02-24', '19:00:00', '20:00:00', 7450, 139),
-- Day 4
('runner42', 'Berlin', '2025-02-25', '08:15:00', '09:15:00', 7680, 138),
('runner42', 'Berlin', '2025-02-25', '17:30:00', '18:30:00', 7150, 134);

INSERT INTO activity_metrics (nickname, city, date, start_time, end_time, steps, heartbeat_rate) VALUES
-- Day 1
('fastestgirlinberlin', 'Berlin', '2025-02-22', '08:00:00', '09:00:00', 7920, 140),
-- Day 2
('fastestgirlinberlin', 'Berlin', '2025-02-23', '07:45:00', '08:45:00', 8500, 135),
('fastestgirlinberlin', 'Berlin', '2025-02-23', '18:15:00', '19:15:00', 7200, 136),
-- Day 3
('fastestgirlinberlin', 'Berlin', '2025-02-24', '06:30:00', '07:30:00', 7100, 142),
('fastestgirlinberlin', 'Berlin', '2025-02-24', '19:00:00', '20:00:00', 7250, 139);

insert into daily_steps_user
SELECT nickname, city, date, SUM(steps) AS total_steps
FROM activity_metrics
GROUP BY nickname, city, date;

pragma ivm('daily_steps_user');
pragma refresh('daily_steps_user');
pragma flush('daily_steps_user', 'duckdb');
pragma flush('daily_steps_user', 'postgres');

