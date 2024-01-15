WITH 

  hit_traffic_sources AS (
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

  session_start_dates AS (
    SELECT
      user_pseudo_id,
      ga_session_id,
      MIN(event_timestamp) as min_session_event_timestamp
    FROM hit_traffic_sources
    GROUP BY 
      1,2
  ),

  session_traffic_sources AS (
    SELECT
      hts.user_pseudo_id,
      hts.ga_session_id,
      ssd.min_session_event_timestamp,

      ----------------------------------
      --- SESSION/HIT COUNTS -----------

      -- session_number
      DENSE_RANK() OVER (
        PARTITION BY hts.user_pseudo_id
        ORDER BY ssd.min_session_event_timestamp
      ) as session_number,

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

    FROM hit_traffic_sources as hts
      LEFT JOIN session_start_dates as ssd
      ON hts.user_pseudo_id = ssd.user_pseudo_id
      AND hts.ga_session_id = ssd.ga_session_id 

    ORDER BY hts.user_pseudo_id, hts.event_timestamp
    )

  -- FINAL QUERY
  SELECT DISTINCT
    user_pseudo_id,
    ga_session_id,
    TIMESTAMP_MICROS(min_session_event_timestamp) as min_session_event_timestamp,
    session_number,
    session_traffic_source,
    session_traffic_medium,
    session_traffic_campaign
  FROM session_traffic_sources
  --WHERE user_pseudo_id = '2221352.0772999791'
  GROUP BY
    1,2,3,4,5,6,7
  ORDER BY user_pseudo_id, min_session_event_timestamp