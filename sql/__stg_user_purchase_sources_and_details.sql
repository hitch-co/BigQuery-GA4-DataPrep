--Name: 
-- 1: Tranactions by user/session/source (w/ items_json)
--Process:
-- 1: Grab the record in struct form and unnest into a json string
-- 2: Capture each nested value as a column 
WITH users_purchases as (
  SELECT 
    CAST(user_pseudo_id AS STRING) as user_pseudo_id,
    ga_session_id,
    CAST(ga_session_id AS STRING) as ga_session_id_string,
    TIMESTAMP_MICROS(event_timestamp) as event_timestamp_timestamp,
    transaction_id,
    items_json
  FROM (
    SELECT 
      event_timestamp,
      user_pseudo_id,
      ( -- get ga_session_id
        SELECT value.int_value FROM UNNEST(event_params) WHERE  key = 'ga_session_id'
      ) AS ga_session_id,

      ecommerce.transaction_id,
      event_name,

      ( -- get a newly built array from the items.* record
        SELECT
          TO_JSON_STRING(
            ARRAY_AGG(
              STRUCT(
                item.item_id, item.item_name, item.item_brand
                )
            )
          )
        FROM 
          UNNEST(items) AS item
        WHERE 
          event_name = 'purchase' AND
          item.item_id IS NOT NULL AND 
          item.item_id <> 'null' AND 
          item.item_id <> '(not set)'
      ) AS items_json

    FROM 
      `key-utility-407314.eh_ga4_obfuscated_sample_ecommerce.eh_ga4_obfuscated_filtered`
  ) 
  WHERE 
    -- Ensures looking at only transactions with items
    items_json IS NOT NULL AND 
    items_json <> '[]' AND
    items_json != 'null' AND
    transaction_id IS NOT NULL AND
    transaction_id != '(not set)' AND
    transaction_id <> 'null'
)

,session_level_details as (
  SELECT
    user_pseudo_id,
    ga_session_id,
    min_session_event_timestamp as session_visit_date,
    ga_session_number,
    session_traffic_source,
    session_traffic_medium,
    session_traffic_campaign,
  FROM `key-utility-407314.eh_ga4_stg._session_traffic_sources_and_details`
  GROUP BY
    1,2,3,4,5,6,7
),

user_level_details as (
  SELECT
    user_pseudo_id,
    min_user_event_timestamp as user_first_visited_date,
    user_traffic_source,
    user_traffic_medium,
    user_traffic_name,
  FROM `key-utility-407314.eh_ga4_stg._user_traffic_sources_and_details`
  GROUP BY
    1,2,3,4,5
)

--------------------------------------
--------------------------------------
-- FINAL QUERY: users_purchases records
-- #T0
SELECT
  -- Keys/identifiers
  up.user_pseudo_id,
  up.ga_session_id,
  up.event_timestamp_timestamp,

  -- Session/Transaction Order
  DENSE_RANK()
  OVER (
    PARTITION BY up.user_pseudo_id
    ORDER BY up.event_timestamp_timestamp ASC
  ) as user_transaction_number,
  
  DENSE_RANK()
  OVER (
    PARTITION BY up.user_pseudo_id, up.ga_session_id
    ORDER BY up.event_timestamp_timestamp ASC
  ) as session_transaction_number,

  sld.ga_session_number,

  -- Attribution
  session_traffic_source,
  session_traffic_medium,
  session_traffic_campaign,
  user_traffic_source,
  user_traffic_medium,
  user_traffic_name,
  
  -- ecommerce
  up.transaction_id,
  up.items_json

FROM users_purchases as up

-----------------------------
-- Join session_level details
LEFT JOIN session_level_details as sld
  ON up.user_pseudo_id = sld.user_pseudo_id
  AND up.ga_session_id = sld.ga_session_id

-----------------------------
-- Join user_level details
LEFT JOIN user_level_details as uld
  ON up.user_pseudo_id = uld.user_pseudo_id

ORDER BY up.user_pseudo_id, up.ga_session_id
/*WHERE up.user_pseudo_id = '2221352.0772999791'*/