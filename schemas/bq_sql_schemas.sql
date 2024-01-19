-- Transaction Table
CREATE TABLE Transaction (
    Transaction_ID STRING NOT NULL,
    User_ID STRING,
    Session_ID STRING,
    Date_and_Time DATETIME NOT NULL,
    Total_Transaction_Value FLOAT
);

-- Item Table
CREATE TABLE Item (
    Item_ID STRING NOT NULL,
    Item_Category STRING,
    Item_Price FLOAT
);

-- Transaction Item Table
CREATE TABLE Transaction_Item (
    Transaction_Item_ID STRING NOT NULL,
    Transaction_ID STRING NOT NULL,
    Item_ID STRING NOT NULL,
    Quantity INT
);

-- User Table
CREATE TABLE User (
    User_ID STRING NOT NULL,
    User_First_Visit_Date DATE,
    Geographic_Location STRING,
    First_Visit_Traffic_Source STRING
);

-- Session Table
CREATE TABLE Session (
    Session_ID STRING NOT NULL,
    User_ID STRING,
    Session_Start_Time DATETIME,
    Session_Duration INT,
    Session_Traffic_Source_ID STRING
);

-- Traffic Source Table
CREATE TABLE Traffic_Source (
    Traffic_Source_ID STRING NOT NULL,
    Source_Type STRING,
    Source_Name STRING
);