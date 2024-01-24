WITH users as (
  SELECT
    u.user_pseudo_id,
    u.user_traffic_source,
    u.user_traffic_medium,
    u.user_traffic_name,
    --u.geo,
    u.min_user_event_timestamp,
    u.max_user_event_timestamp,
    SUM(u.count_of_sessions) as total_sessions,
    SUM(u.pageviews) as total_pageviews
  FROM 
    `key-utility-407314.eh_ga4_stg._users` as u
  GROUP BY
    1,2,3,4,5,6
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
  SUM(u.total_sessions) as total_sessions,
  SUM(u.total_pageviews) as total_pageviews,
  SUM(t.total_transactions) as total_transactions,
  SUM(t.total_item_quantity) as total_items_purchased,
  SUM(t.total_revenue) as total_revenue
FROM users as u

LEFT JOIN transactions as t
  ON u.user_pseudo_id = t.user_pseudo_id

GROUP BY
  1,2,3,4,5,6

ORDER BY u.user_pseudo_id