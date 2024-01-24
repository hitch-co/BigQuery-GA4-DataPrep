### Users for testing

## NOTE: This is a summary of a handful of users that are included in all draft queries

# Users included
Users (py): ```users = ['29339544.5051604260', '5184896.7773934801', '73497675.9720954301', '2221352.0772999791', '6724688.1020387197', '67504810.2599834553', '77052125.0799530822']```
Users (sql): ```users = ('29339544.5051604260', '5184896.7773934801', '73497675.9720954301', '2221352.0772999791', '6724688.1020387197', '67504810.2599834553', '77052125.0799530822')```


# Conditions for caputring these users(only picked a handful from this group)
- DECLARE count_of_sessions_min DEFAULT 5;
- DECLARE count_of_sessions_max DEFAULT 7;
- DECLARE count_of_transactions_min DEFAULT 2;
- DECLARE count_of_transactions_max DEFAULT 5;

# Dates of coverage
Start Date: 2020-01-01
End Date: 2020-01-31

## Queries

## Sample Results for our specified user set 
# Data is filtered to user list below
| user_pseudo_id        | session_ids | transaction_ids | pageview records
|-----------------------|-------------|-----------------|-----------------
| 2221352.0772999791    | 5 | 2 | 38
| 29339544.5051604260   | 7 | 4 | 51
| 5184896.7773934801    | 5 | 4 | 39
| 6724688.1020387197    | 5 | 2 | 35
| 67504810.2599834553   | 5 | 2 | 78
| 73497675.9720954301   | 5 | 3 | 81
| 77052125.0799530822   | 7 | 2 | 166
|-------------------------------------------------------------------------
|       --TOTAL--       | 39| 19| 488

# Summary statistics by user
- Number of records in our query: 1639
- Number of sessions: 39
- Number of pageviews by user: 488 
- Purchase Revenue: 755.0 (validated using `__prod_users_details` query)
- Number of transaction_ids: 19
