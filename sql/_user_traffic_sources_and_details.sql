WITH 
  t1 AS (
    SELECT
      user_pseudo_id,
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as ga_session_id,
      event_name,
      event_timestamp,
      traffic_source.source as traffic_source,
      traffic_source.medium as traffic_medium,
      traffic_source.name as traffic_name
    FROM `key-utility-407314.eh_ga4_obfuscated_sample_ecommerce.eh_ga4_obfuscated_filtered`
    GROUP BY 1,2,3,4,5,6,7
  ),

  pageviews AS (
    SELECT
      pv.user_pseudo_id,
      COUNT(*) as pageviews
    FROM (
      SELECT 
        user_pseudo_id,
        event_name
      FROM t1
      WHERE event_name = 'page_view'   
    ) as pv
    GROUP BY pv.user_pseudo_id
  ),

  session_details AS (
    SELECT
      user_pseudo_id,
      MIN(event_timestamp) as min_user_event_timestamp,
      MIN(event_timestamp) as max_user_event_timestamp,
      COUNT(DISTINCT ga_session_id) as count_of_sessions, 
    FROM t1
    GROUP BY 
      1
  ),

  user_traffic_sources AS (
    SELECT
      t1.user_pseudo_id,
      TIMESTAMP_MICROS(ssd.min_user_event_timestamp) as min_user_event_timestamp,
      TIMESTAMP_MICROS(ssd.max_user_event_timestamp) as max_user_event_timestamp,

      -- first hit session source
      FIRST_VALUE(traffic_source)
      OVER (
        PARTITION BY t1.user_pseudo_id
        ORDER BY event_timestamp ASC
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) as user_traffic_source,

      -- first hit session medium
      FIRST_VALUE(traffic_medium)
      OVER (
        PARTITION BY t1.user_pseudo_id
        ORDER BY event_timestamp ASC
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) as user_traffic_medium,

      -- first hit session name
      FIRST_VALUE(traffic_name)
      OVER (
        PARTITION BY t1.user_pseudo_id
        ORDER BY event_timestamp ASC
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) as user_traffic_name,

      -- Measures
      ssd.count_of_sessions,
      pv.pageviews

    FROM t1 as t1
      LEFT JOIN session_details as ssd
        ON t1.user_pseudo_id = ssd.user_pseudo_id
      LEFT JOIN pageviews as pv
        ON t1.user_pseudo_id = pv.user_pseudo_id
  )

SELECT DISTINCT 
  *
FROM user_traffic_sources