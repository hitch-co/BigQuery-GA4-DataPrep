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
    user_pseudo_id,
    ga_session_id,
    transaction_id
  FROM `key-utility-407314.eh_ga4_stg._transactions`
),

transaction_id_items as (
  SELECT
    transaction_id,
    event_timestamp,
    item_id,
    price,
    quantity,
    IFNULL(price,0) * IFNULL(quantity,0) as revenue
  FROM `key-utility-407314.eh_ga4_stg._transaction_items`
  GROUP BY
    1,2,3,4,5,6
)

SELECT
  t.user_pseudo_id,
  t.ga_session_id,
  s.session_traffic_source,
  s.session_traffic_medium,
  s.session_traffic_campaign,
  s.min_session_event_timestamp,
  s.max_session_event_timestamp,
  ti.transaction_id,
  ti.item_id,
  ti.quantity as total_quantity,
  ti.revenue as total_revenue
FROM transaction_id_items as ti
LEFT JOIN transactions as t
  ON ti.transaction_id = t.transaction_id
LEFT JOIN sessions as s
  ON t.ga_session_id = s.ga_session_id
GROUP BY
  1,2,3,4,5,6,7,8,9,10,11
ORDER BY t.user_pseudo_id, t.ga_session_id