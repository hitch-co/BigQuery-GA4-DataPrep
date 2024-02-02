WITH users as (
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
    COUNT(DISTINCT s.ga_session_id) as total_sessions
  FROM
    `key-utility-407314.eh_ga4_stg._sessions` as s
  GROUP BY s.user_pseudo_id
),

pageviews as (
  SELECT 
    p.user_pseudo_id,
    COUNT(DISTINCT p.event_timestamp) as total_pageviews
  FROM
    `key-utility-407314.eh_ga4_obfuscated_sample_ecommerce.eh_ga4_obfuscated_filtered` as p
  WHERE p.event_name = 'page_view'
  GROUP BY p.user_pseudo_id
),

transactions as (
  SELECT 
    t.user_pseudo_id,
    COUNT(DISTINCT t.transaction_id) as total_transactions,
    SUM(t.total_item_quantity) as total_item_quantity,
    SUM(t.revenue) as total_revenue
  FROM
    `key-utility-407314.eh_ga4_stg._transactions` as t 
  GROUP BY t.user_pseudo_id
)

-- SELECT * FROM users
-- ORDER BY user_pseudo_id

SELECT
  u.user_pseudo_id,
  u.user_traffic_source,
  u.user_traffic_medium,
  u.user_traffic_name,
  --u.geo,
  u.min_user_event_timestamp,
  u.max_user_event_timestamp,
  SUM(s.total_sessions) as total_sessions,
  SUM(p.total_pageviews) as total_pageviews,
  SUM(t.total_transactions) as total_transactions,
  SUM(t.total_item_quantity) as total_items_purchased,
  SUM(t.total_revenue) as total_revenue
FROM users as u

LEFT JOIN transactions as t
  ON u.user_pseudo_id = t.user_pseudo_id
LEFT JOIN sessions as s
  ON u.user_pseudo_id = s.user_pseudo_id
LEFT JOIN pageviews as p
  ON u.user_pseudo_id = p.user_pseudo_id

GROUP BY
  1,2,3,4,5,6

ORDER BY u.user_pseudo_id