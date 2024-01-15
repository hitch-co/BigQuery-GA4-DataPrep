-- --Process:
-- -- 1: Grab the record in struct form and unnest into a json string
-- -- 2: Capture each nested value as a column 
WITH users_purchases as (
  SELECT 
    CAST(user_pseudo_id AS STRING) as user_pseudo_id,
    CAST(ga_session_id AS STRING) as ga_session_id,
    TIMESTAMP_MICROS(event_timestamp) as event_timestamp,
    CAST(event_timestamp as STRING) as event_timestamp_string,
    items_json,
    items,
  FROM (
    SELECT 
      event_timestamp,
      user_pseudo_id,
      event_name,
      items,
      (
        SELECT 
          value.int_value 
        FROM UNNEST(event_params) 
        WHERE 
          key = 'ga_session_id'
      ) AS ga_session_id,
      
      ecommerce.transaction_id,

      (
        SELECT
          TO_JSON_STRING(
            ARRAY_AGG(
              STRUCT(item_id, item_name, item_brand)
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
    items_json IS NOT NULL AND 
    items_json <> '[]' AND
    items_json != 'null'
),

purchase_counts as (
  SELECT DISTINCT
    user_pseudo_id,
    COUNT(*) as count_of_purchases
  FROM users_purchases
  GROUP BY user_pseudo_id
  ORDER BY user_pseudo_id ASC
)

-- users_purchases records
-- #T0
SELECT
  *,
  JSON_EXTRACT_SCALAR(items_json, '$[0].item_id') as item_id,
  JSON_EXTRACT_SCALAR(items_json, '$[0].item_name') as item_name,
  JSON_EXTRACT_SCALAR(items_json, '$[0].item_brand') as item_brand
FROM users_purchases