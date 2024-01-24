WITH results as  (
  SELECT
    event_timestamp,
    user_pseudo_id,
    ecommerce.transaction_id as transaction_id,
    item.item_id,
    item.price,
    item.quantity,
    IFNULL(item.price,0) * IFNULL(item.quantity,0) as revenue
  FROM
    `key-utility-407314.eh_ga4_obfuscated_sample_ecommerce.eh_ga4_obfuscated_filtered`,
    UNNEST(items) AS item
  WHERE
    event_name = 'purchase'
  ORDER BY
    user_pseudo_id,
    transaction_id
)

SELECT 
  transaction_id,
  item_id,
  event_timestamp,
  AVG(price) as price,
  SUM(quantity) as quantity,
  SUM(revenue) as revenue
FROM results
GROUP BY
  transaction_id,
  item_id,
  event_timestamp
ORDER BY transaction_id, item_id