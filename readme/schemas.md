# Web Analytics Data Schema

### Table Name: `user_event_session_info`
### Fields:
- `user_id`: Unique identifier for each visitor.
- `session_id`: Identifier for each session.
- `timestamp`: Exact time of the user action or event.
- `device_type`: Type of device used by the visitor.
- `geography`: Geographic location of the visitor.
- `user_type`: New vs. returning session indicator.
- `source`: Source of the traffic (e.g., Google, direct).
- `medium`: Medium of the traffic (e.g., organic, referral).
- `campaign`: Campaign name associated with the session.
- `page_url`: URL of the visited page.
- `event_type`: Type of interaction (e.g., page_view, form_submit).
- `time_on_page`: Duration spent on each page (in seconds).

## Product-Level Data (For Recommendation System, Cart Abandonment, and Purchase Forecasting)
### Table Name: `product_cart_action`
### Fields:
- `user_id`: Unique identifier for each visitor.
- `session_id`: Identifier for each session.
- `timestamp`: Exact time of the product interaction.
- `product_id`: Unique identifier for each product.
- `product_category`: Category of the product.
- `interaction_type`: Type of product interaction (e.g., view, add_to_cart, remove_from_cart).
- `quantity`: Number of products interacted with (useful for add_to_cart and purchase events).
- `view_duration`: Time spent viewing the product (if applicable).
- `abandonment_point_stg`: Stage of cart abandonment (if applicable).
- `abandonment_point_utc`: Timestamp of cart abandonment (if applicable).
- `total_purchase_value`: Total value of the purchase (for purchase events).

## Purchase Data (For Forecasting and Analysis of Purchases)
### Table Name: `purchase_order_details`
### Fields:
####  Note: purchase channel could be derived from a different analysis 
- `user_id`: Unique identifier for each visitor.
- `purchase_id`: Unique identifier for each purchase.
- `purchase_timestamp`: Exact time of purchase.
- `product_id`: Unique identifier for each product purchased.
- `quantity`: Number of each product purchased.
- `total_purchase_value`: Total value of the purchase.
- `purchase_channel`: Channel through which the purchase was made (e.g., online, in-store).

