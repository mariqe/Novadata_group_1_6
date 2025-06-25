
CREATE TABLE user_events
(
    user_id UInt32,
    event_type String,
    points_spent UInt32,
    event_time DateTime
)
ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY;

CREATE TABLE user_events_agg
(
    event_date Date,
    event_type String,
    users AggregateFunction(uniq, UInt32),
    total_points AggregateFunction(sum, UInt32),
    action_count AggregateFunction(count, UInt32)
)
ENGINE = AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;

CREATE MATERIALIZED VIEW user_events_mv TO user_events_agg
AS SELECT
    toDate(event_time) AS event_date,
    event_type,
    uniqState(user_id) AS users,
    sumState(points_spent) AS total_points,
    countState() AS action_count
FROM user_events
GROUP BY event_date, event_type;

INSERT INTO user_events (user_id, event_type, points_spent, event_time) VALUES
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),
(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),
(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),
(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),
(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),
(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());

WITH 
day_0_users AS (
    SELECT DISTINCT user_id
    FROM user_events
    WHERE toDate(event_time) = (now() - INTERVAL 10 DAY)
),

returned_users AS (
    SELECT DISTINCT user_id
    FROM user_events
    WHERE toDate(event_time) BETWEEN (now() - INTERVAL 9 DAY) AND (now() - INTERVAL 3 DAY)
    AND user_id IN (SELECT user_id FROM day_0_users)
)
SELECT 
    count() AS total_users_day_0,
    countIf(user_id IN (SELECT user_id FROM returned_users)) AS returned_in_7_days,
    round(countIf(user_id IN (SELECT user_id FROM returned_users)) / count() * 100, 2) AS retention_7d_percent
FROM day_0_users;

SELECT 
    event_date,
    event_type,
    uniqMerge(users) AS unique_users,
    sumMerge(total_points) AS total_spent,
    countMerge(action_count) AS total_actions
FROM user_events_agg
WHERE event_date >= now() - INTERVAL 7 DAY
GROUP BY event_date, event_type
ORDER BY event_date DESC, event_type;