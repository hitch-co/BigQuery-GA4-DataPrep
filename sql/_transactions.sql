WITH users_purchases as (
  SELECT 
    user_pseudo_id,
    ga_session_id,
    event_timestamp,
    transaction_id,
    total_item_quantity,
    purchase_revenue,
  FROM (
    SELECT 
      event_timestamp,
      user_pseudo_id,
      ecommerce.total_item_quantity,
      (SELECT value.int_value FROM UNNEST(event_params) WHERE  key = 'ga_session_id') AS ga_session_id,

      -- Measures
      ecommerce.transaction_id,
      ecommerce.purchase_revenue,
      event_name
    FROM 
      `key-utility-407314.eh_ga4_obfuscated_sample_ecommerce.eh_ga4_obfuscated_filtered`
    WHERE
      event_name = 'purchase'
  )
  WHERE
    -- Ensures looking at only transactions with items
    transaction_id IS NOT NULL AND
    transaction_id != '(not set)' AND
    transaction_id <> 'null'
)

SELECT
  user_pseudo_id,
  ga_session_id,
  transaction_id,
  TIMESTAMP_MICROS(event_timestamp) as event_timestamp,
  SUM(total_item_quantity) as total_item_quantity,
  SUM(purchase_revenue) as revenue
FROM users_purchases
GROUP BY
  user_pseudo_id,  
  ga_session_id,
  transaction_id,
  event_timestamp
ORDER BY 
  user_pseudo_id, 
  transaction_id, 
  ga_session_id,
  event_timestamp