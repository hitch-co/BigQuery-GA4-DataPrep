SELECT DISTINCT
  item_id,
  item_name,
  item_brand,
  item_category,
  item_category2,
  item_category3,
  item_category4,
  item_category5
FROM `key-utility-407314.eh_ga4_obfuscated_sample_ecommerce.eh_ga4_obfuscated_filtered`,
 UNNEST(items)
WHERE event_name = 'purchase'
--AND item_id = '9184749'