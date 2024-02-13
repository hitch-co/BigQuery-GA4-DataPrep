-- Transaction Table
CREATE TABLE __user_details (
    user_pseudo_id CHAR NOT NULL,
    --traffic_source_id CHAR NOT NULL,
    user_traffic_source CHAR,
    user_traffic_medium CHAR,
    user_Traffic_campaign CHAR,
    min_user_event_timestamp DATETIME,
    max_user_event_timestamp DATETIME,
    total_sessions INT,
    total_pageviews INT,
    total_transactions INT,
    total_items_purchased INT,
    total_revenue FLOAT
)

CREATE TABLE __users_session_details (
    user_pseudo_id CHAR NOT NULL,
    ga_session_id CHAR NOT NULL,
    --traffic_source_id CHAR NOT NULL,
    user_traffic_source CHAR,
    user_traffic_medium CHAR,
    user_traffic_campaign CHAR,
    min_user_event_timestamp DATETIME,
    max_user_event_timestamp DATETIME,
    session_traffic_campaign CHAR,
    session_traffic_medium CHAR,
    session_traffic_source CHAR,
    min_session_event_timestamp DATETIME,
    max_session_event_timestamp DATETIME,
    total_sessions INT,
    total_pageviews INT,
    total_transactions INT,
    total_items_purchased INT,
    total_revenue FLOAT    
)

CREATE TABLE __transaction_details (
    user_pseudo_id CHAR NOT NULL,
    ga_session_id CHAR NOT NULL,
    --traffic_source_id CHAR NOT NULL,
    session_traffic_campaign CHAR,
    session_traffic_medium CHAR,
    session_traffic_source CHAR,
    min_session_event_timestamp DATETIME,
    max_session_event_timestamp DATETIME,
    transaction_id CHAR,
    item_id CHAR,
    total_quantity CHAR,
    total_revenue CHAR
)

CREATE TABLE Transaction (
    Transaction_ID CHAR NOT NULL,
    User_ID CHAR,
    Session_ID CHAR,
    Date_and_Time DATETIME NOT NULL,
    Total_Transaction_Value FLOAT
);

-- Item Table
CREATE TABLE Item (
    Item_ID CHAR NOT NULL,
    Item_Category CHAR,
    Item_Price FLOAT
);

-- Transaction Item Table
CREATE TABLE Transaction_Item (
    Transaction_Item_ID CHAR NOT NULL,
    Transaction_ID CHAR NOT NULL,
    Item_ID CHAR NOT NULL,
    Quantity INT
);

-- User Table
CREATE TABLE User (
    User_ID CHAR NOT NULL,
    User_First_Visit_Date DATE,
    Geographic_Location CHAR,
    First_Visit_Traffic_Source CHAR
);

-- Session Table
CREATE TABLE Session (
    Session_ID CHAR NOT NULL,
    User_ID CHAR,
    Session_Start_Time DATETIME,
    Session_Duration INT,
    Session_Traffic_Source_ID CHAR
);

-- Traffic Source Table
CREATE TABLE Traffic_Source (
    Traffic_Source_ID CHAR NOT NULL,
    Source_Type CHAR,
    Source_Name CHAR
);