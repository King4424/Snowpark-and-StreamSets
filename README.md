# A Dive Into Slowly Changing Dimensions with Snowpark and StreamSets

## 1. Overview
**StreamSets Transformer for Snowflake is a hosted service embedded within the StreamSets DataOps Platform that uses the Snowpark Client Libraries to generate SnowSQL queries that are executed in Snowflake. Build your pipelines in the StreamSets canvas and when you execute that pipeline, StreamSets generates a DAG. StreamSets then uses the DAG and the Snowpark Client Libraries to generate SnowSQL. That SnowSQL is sent over to Snowflake to be executed in the Warehouse of your choice.**

![image](https://github.com/King4424/Snowpark-and-StreamSets/assets/121480992/37e0590f-9706-41d7-a4e2-24051b50d732)

**Transformer for Snowflake accelerates the development of data pipelines by providing features that go beyond a drag and drop interface to construct the equivalent of basic SQL. Snowpark enables StreamSets to construct the SnowSQL queries at the pipeline runtime, allowing pipeline definitions to be more flexible than SnowSQL queries written by hand. StreamSets also provides processors that combine the logic for common use cases into a single element on your canvas.**

### Prerequisites

:small_blue_diamond: **Access to StreamSets DataOps Platform account**

:small_blue_diamond: **A Snowflake account and a schema that your user has CREATE TABLE privileges on**

:small_blue_diamond: **SQL Script**
```
/////////////////////////////////////////////////
// Section 1: Build and Load Tables /////////////
/////////////////////////////////////////////////

CREATE OR REPLACE DATABASE DEMO;
CREATE OR REPLACE SCHEMA HCM;

CREATE OR REPLACE TABLE DEMO.HCM.EMPLOYEES (
    ID INTEGER,
    FNAME VARCHAR(16777216),
    LNAME VARCHAR(16777216),
    TITLE VARCHAR(16777216),
    ACTIVE_FLAG BOOLEAN,
    VERSION INTEGER,
    START_TIMESTAMP TIMESTAMP,
    END_TIMESTAMP TIMESTAMP
);

INSERT INTO 
    DEMO.HCM.EMPLOYEES
VALUES
    (2,'Dwight','Schrute','Sales Rep',TRUE,1,current_timestamp(),null),
    (3,'Ryan','Howard','Temp',TRUE,1,current_timestamp(),null);
    

CREATE OR REPLACE TABLE DEMO.HCM.UPDATE_FEED
 (
        ID INTEGER,
        FNAME VARCHAR(16777216),
        LNAME VARCHAR(16777216),
        TITLE VARCHAR(16777216),
        CAKE_PREFERENCE VARCHAR(16777216)
    );

INSERT INTO
    DEMO.HCM.UPDATE_FEED
VALUES
    (2,'Dwight','Schrute','Assistant to the Regional Manager','(Beet) Red Velvet');

/////////////////////////////////////////////////
// Section 2: Check results /////////////////////
/////////////////////////////////////////////////

SELECT * FROM DEMO.HCM.EMPLOYEES;
```

## 2. Create you'r StreamSets Account
**https://cloud.login.streamsets.com/signup**

<img width="958" alt="image" src="https://github.com/King4424/Snowpark-and-StreamSets/assets/121480992/45e1b98d-948c-4637-9f9e-1307770dc23d">

## 3. Configure your Snowflake Credentials and Connection Defaults

**If you have not used Transformer for Snowflake before, after signing up for the platform, you will need to add your Snowflake Credentials to your user. You can find these settings under the User Icon > My Account**

<img width="960" alt="image" src="https://github.com/King4424/Snowpark-and-StreamSets/assets/121480992/db1697de-31e3-4c49-9419-d7325ab09106">

<img width="960" alt="image" src="https://github.com/King4424/Snowpark-and-StreamSets/assets/121480992/7173d3c8-3297-453c-8590-a2429dcd62ff">

**Click On Snowflake Credentials -> Add, Give Your Snowflake account Credentials.**

<img width="960" alt="image" src="https://github.com/King4424/Snowpark-and-StreamSets/assets/121480992/c8a872fe-d836-4dcf-a732-e572fe23ab71">

**Fill Snowflake Pipeline Defaults which you have created though SQL Script**

<img width="960" alt="image" src="https://github.com/King4424/Snowpark-and-StreamSets/assets/121480992/19739047-f336-4d99-b264-3446005c067c">

## 3. Create a New Pipeline
**Once your credentials are saved, you are ready to build your first pipeline. Using the tabs on the left hand menu, select Build > Pipelines. Use the ➕ icon to create a new pipeline.**

<img width="959" alt="image" src="https://github.com/King4424/Snowpark-and-StreamSets/assets/121480992/70747f15-42b6-4da6-8add-70b32d128217">








