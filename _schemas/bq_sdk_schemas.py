# BigQuery table schemas defined in Python

schemas = {
    "Transaction": [
        {"name": "Transaction_ID", "type": "STRING", "mode": "REQUIRED"},
        {"name": "User_ID", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Session_ID", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Date_and_Time", "type": "DATETIME", "mode": "REQUIRED"},
        {"name": "Total_Transaction_Value", "type": "FLOAT", "mode": "NULLABLE"}
    ],
    "Item": [
        {"name": "Item_ID", "type": "STRING", "mode": "REQUIRED"},
        {"name": "Item_Category", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Item_Price", "type": "FLOAT", "mode": "NULLABLE"}
    ],
    "Transaction_Item": [
        {"name": "Transaction_Item_ID", "type": "STRING", "mode": "REQUIRED"},
        {"name": "Transaction_ID", "type": "STRING", "mode": "REQUIRED"},
        {"name": "Item_ID", "type": "STRING", "mode": "REQUIRED"},
        {"name": "Quantity", "type": "INTEGER", "mode": "NULLABLE"}
    ],
    "User": [
        {"name": "User_ID", "type": "STRING", "mode": "REQUIRED"},
        {"name": "User_First_Visit_Date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "Geographic_Location", "type": "STRING", "mode": "NULLABLE"},
        {"name": "First_Visit_Traffic_Source", "type": "STRING", "mode": "NULLABLE"}
    ],
    "Session": [
        {"name": "Session_ID", "type": "STRING", "mode": "REQUIRED"},
        {"name": "User_ID", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Session_Start_Time", "type": "DATETIME", "mode": "NULLABLE"},
        {"name": "Session_Duration", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "Session_Traffic_Source_ID", "type": "STRING", "mode": "NULLABLE"}
    ],
    "Traffic_Source": [
        {"name": "Traffic_Source_ID", "type": "STRING", "mode": "REQUIRED"},
        {"name": "Source_Type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Source_Name", "type": "STRING", "mode": "NULLABLE"}
    ]
}