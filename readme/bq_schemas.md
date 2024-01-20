# Web Analytics Data Schema

### Table Name: `users`
### Fields:
- PK4: user_pseudo_id
- FK6: traffic_source_id
- first_visit_date
- last_visit_date
- geo
- user_Traffic_source
- user_traffic_campaign
- total_sessions
- total_pageviews

### Table Name: `sessions`
### Fields:
- SK8: user_pseudo_id+session_id
- PK4: user_pseudo_id
- FK5: ga_session_id
- FK6: traffic_source_id
- traffic_source_id
- session_start_time
- session_end_time
- session_durration
- session_number
- session_traffic_campaign
- session_traffic_medium
- session_traffic_source
- total_pageviews

### Table Name: `transactions`
### Fields:
- PK1: transaction_id
- FK4: user_id
- FK5: ga_session_id
- timestamp
- revenue

### Table Name: `items`
### Fields:
- PK2: item_id
- item_category
- item_price
- _price_date_start_
- _price_date_end_

### Table Name: `transaction_items`
### Fields:
- SK7: transaction_item_id
- FK1: transaction_id
- FK2: item_id
- quantity