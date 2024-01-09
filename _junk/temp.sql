DECLARE start_date STRING DEFAULT '20210101';
DECLARE end_date STRING DEFAULT '20210107';

with dims as (
  SELECT
    event_date,
    event_timestamp,
    user_id,
    user_pseudo_id
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN start_date AND end_date
),

rows_rank as (
  SELECT
    dims.user_pseudo_id as user_psuedo_id,
    count(*) as count_of_rows
  FROM dims
  GROUP BY
    dims.user_pseudo_id
)

--
SELECT 
  dims.*,
  rows_rank.count_of_rows
FROM dims
LEFT JOIN rows_rank
ON dims.user_pseudo_id = rows_rank.user_psuedo_id
ORDER BY rows_rank.count_of_rows DESC

-- --
-- SELECT * FROM rows_rank
-- ORDER BY count_of_rows DESC

-- --
--SELECT * from dims
--ORDER BY user_pseudo_id, event_timestamp ASC 