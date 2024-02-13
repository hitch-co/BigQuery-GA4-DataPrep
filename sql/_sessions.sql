WITH 

_hit_traffic_sources AS (
  SELECT 
    event_timestamp,
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) as ep WHERE key = 'ga_session_id') as ga_session_id,
    (SELECT value.string_value FROM UNNEST(event_params) as ep WHERE key = 'source') as hit_traffic_source,
    (SELECT value.string_value FROM UNNEST(event_params) as ep WHERE key = 'medium') as hit_traffic_medium,
    (SELECT value.string_value FROM UNNEST(event_params) as ep WHERE key = 'campaign') as hit_traffic_campaign,
  FROM `key-utility-407314.eh_ga4_obfuscated_sample_ecommerce.eh_ga4_obfuscated_filtered` as t
  WHERE 
    (SELECT value.string_value FROM UNNEST(event_params) as ep WHERE key = 'source') IS NOT NULL AND
    (SELECT value.string_value FROM UNNEST(event_params) as ep WHERE key = 'medium') IS NOT NULL AND
    (SELECT value.string_value FROM UNNEST(event_params) as ep WHERE key = 'campaign') IS NOT NULL
),

_session_start_dates AS (
  SELECT
    user_pseudo_id,
    ga_session_id,
    MIN(event_timestamp) as min_session_event_timestamp,
    MAX(event_timestamp) as max_session_event_timestamp
  FROM _hit_traffic_sources
  GROUP BY
    1,2
),

session_traffic_sources AS (
  SELECT DISTINCT
    hts.user_pseudo_id,
    hts.ga_session_id,
    ssd.max_session_event_timestamp,
    ssd.min_session_event_timestamp,

    ----------------------------------
    --- SESSION/HIT COUNTS -----------

    -- ga_session_number
    DENSE_RANK() OVER (
      PARTITION BY hts.user_pseudo_id
      ORDER BY ssd.min_session_event_timestamp
    ) as ga_session_number,

    ---------------------------
    --- ATTRIBUTION -----------

    -- first hit SESSION source
    FIRST_VALUE(hts.hit_traffic_source)
    OVER (
      PARTITION BY hts.user_pseudo_id, hts.ga_session_id
      ORDER BY hts.event_timestamp ASC
      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as session_traffic_source,

    -- first hit SESSION medium
    FIRST_VALUE(hts.hit_traffic_medium)
    OVER (
      PARTITION BY hts.user_pseudo_id, hts.ga_session_id
      ORDER BY hts.event_timestamp ASC
      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as session_traffic_medium,

    -- first hit SESSION campaign
    FIRST_VALUE(hts.hit_traffic_campaign)
    OVER (
      PARTITION BY hts.user_pseudo_id, hts.ga_session_id
      ORDER BY hts.event_timestamp ASC
      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as session_traffic_campaign,

  FROM _hit_traffic_sources as hts
    LEFT JOIN _session_start_dates as ssd
      ON hts.user_pseudo_id = ssd.user_pseudo_id
      AND hts.ga_session_id = ssd.ga_session_id
  ORDER BY hts.user_pseudo_id, ssd.min_session_event_timestamp
),

pageviews AS (
  SELECT
    user_pseudo_id,
    ga_session_id,
    COUNT(*) as pageviews
  FROM (
    SELECT 
      user_pseudo_id,
      (SELECT value.int_value FROM UNNEST(event_params) as ep WHERE key = 'ga_session_id') as ga_session_id,
      event_name
    FROM `key-utility-407314.eh_ga4_obfuscated_sample_ecommerce.eh_ga4_obfuscated_filtered`
    WHERE event_name = 'page_view'   
  ) as pv
  GROUP BY 
    pv.user_pseudo_id,
    pv.ga_session_id
)


-- FINAL QUERY
SELECT DISTINCT
  sts.user_pseudo_id,
  sts.ga_session_id,
  TIMESTAMP_MICROS(sts.min_session_event_timestamp) as min_session_event_timestamp,
  TIMESTAMP_MICROS(sts.max_session_event_timestamp) as max_session_event_timestamp,
  TIMESTAMP_DIFF(TIMESTAMP_MICROS(sts.max_session_event_timestamp), TIMESTAMP_MICROS(sts.min_session_event_timestamp), SECOND) AS session_duration_seconds,
  sts.ga_session_number,
  sts.session_traffic_source,
  sts.session_traffic_medium,
  sts.session_traffic_campaign,
  SUM(pv.pageviews) as pageviews, 
FROM session_traffic_sources as sts
LEFT JOIN pageviews as pv
  ON sts.user_pseudo_id = pv.user_pseudo_id
  AND sts.ga_session_id = pv.ga_session_id
--WHERE user_pseudo_id = '2221352.0772999791'
GROUP BY
  1,2,3,4,5,6,7,8,9
ORDER BY user_pseudo_id, min_session_event_timestamp