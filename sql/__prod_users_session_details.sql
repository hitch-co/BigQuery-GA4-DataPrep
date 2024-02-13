WITH 

users as (
  SELECT
    u.user_pseudo_id,
    u.user_traffic_source,
    u.user_traffic_medium,
    u.user_traffic_name,
    --u.geo,
    u.min_user_event_timestamp,
    u.max_user_event_timestamp
  FROM 
    `key-utility-407314.eh_ga4_stg._users` as u
  GROUP BY
    1,2,3,4,5,6
),

sessions as (
  SELECT 
    s.user_pseudo_id,
    s.ga_session_id,
    s.min_session_event_timestamp,
    s.max_session_event_timestamp,
    s.session_traffic_source,
    s.session_traffic_medium,
    s.session_traffic_campaign,
    s.session_duration_seconds,
    COUNT(DISTINCT s.ga_session_id) as total_sessions,
    SUM(s.pageviews) as total_pageviews
  FROM
    `key-utility-407314.eh_ga4_stg._sessions` as s
  GROUP BY 
    s.user_pseudo_id,
    s.ga_session_id,
    s.min_session_event_timestamp,
    s.max_session_event_timestamp,
    s.session_traffic_source,
    s.session_traffic_medium,
    s.session_traffic_campaign,
    s.session_duration_seconds
),

transactions as (
  SELECT 
    t.user_pseudo_id,
    t.ga_session_id,
    COUNT(DISTINCT t.transaction_id) as total_transactions,
    SUM(t.total_item_quantity) as total_item_quantity,
    SUM(t.revenue) as total_revenue
  FROM
    `key-utility-407314.eh_ga4_stg._transactions` as t 
  GROUP BY 
    t.user_pseudo_id,
    t.ga_session_id
)

SELECT
  u.user_pseudo_id,
  s.ga_session_id,
  u.user_traffic_source,
  u.user_traffic_medium,
  u.user_traffic_name,
  --u.geo,
  u.min_user_event_timestamp,
  u.max_user_event_timestamp,
  s.session_traffic_source,
  s.session_traffic_medium,
  s.session_traffic_campaign,
  s.min_session_event_timestamp,
  s.max_session_event_timestamp,
  SUM(s.total_sessions) as total_sessions,
  SUM(s.total_pageviews) as total_pageviews,
  SUM(t.total_transactions) as total_transactions,
  SUM(t.total_item_quantity) as total_items_purchased,
  SUM(t.total_revenue) as total_revenue
FROM sessions as s
LEFT JOIN transactions as t
  ON s.user_pseudo_id = t.user_pseudo_id
  AND s.ga_session_id = t.ga_session_id
LEFT JOIN users as u
  ON s.user_pseudo_id = u.user_pseudo_id
GROUP BY
  1,2,3,4,5,6,7,8,9,10,11,12
ORDER BY u.user_pseudo_id, s.ga_session_id