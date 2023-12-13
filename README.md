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

**The New Pipeline window will appear, so enter a pipeline name and if desired, a description, and make sure that the Engine Type selected is Transformer for Snowflake.**

<img width="959" alt="image" src="https://github.com/King4424/Snowpark-and-StreamSets/assets/121480992/a3f7cb0c-ebf7-47ac-b20c-6b9904b9e3f7">

<img width="959" alt="image" src="https://github.com/King4424/Snowpark-and-StreamSets/assets/121480992/75817a84-d1a0-46c8-b671-f6a5dd39a54c">

**Add any users or groups for shared access to the pipeline, choose Save & Next, and then Open in Canvas.**

<img width="960" alt="image" src="https://github.com/King4424/Snowpark-and-StreamSets/assets/121480992/ebbfccdc-0879-41e5-88aa-8eb017aa42a2">

## 4. Code Snippets, Info Boxes, and Tables

**Every Transformer for Snowflake pipeline must have an origin and a destination. For this pipeline, select the Snowflake Table Origin from either the drop down menu at the top of the canvas or from the full menu of processors on the right.**

**Click on the Table origin, under the General tab, and name it ‘Employees', and on the Table tab, enter the table name EMPLOYEES. This will be the Dimension table that tracks the employee data.**

**Add a second Snowflake Table origin to the canvas. This origin will be for a feed of updated Employee data. Under the General tab, name the origin ‘Updates' and on the Table tab, enter UPDATE_FEED.**

**At this point, run the first section of SQL in the SQL Script file to create and populate the EMPLOYEES and UPDATE_FEED tables in a Snowflake Worksheet. You will need to substitute the names of your database and schema into the script if you already have your own that you want to use.**

<img width="960" alt="image" src="https://github.com/King4424/Snowpark-and-StreamSets/assets/121480992/b6cbe3ea-6b0d-4d04-a068-34518917f9dc">

<img width="960" alt="image" src="https://github.com/King4424/Snowpark-and-StreamSets/assets/121480992/a0e70d68-c76c-4f0c-a1a1-693598549ca5">

**NEXT Select Snowflake Table**

<img width="960" alt="image" src="https://github.com/King4424/Snowpark-and-StreamSets/assets/121480992/d8fad5d1-643d-4feb-aeb3-27f88bacdb61">

## 5. Add Slowly Changing Dimension
**Now add the Slowly Changing Dimension processor from the top menu (which only appears when the origin is selected on the canvas) or from the right side menu of all Processors. Make sure the Employees Table Origin is connected to the Slowly Changing Dimensions.**

<img width="960" alt="image" src="https://github.com/King4424/Snowpark-and-StreamSets/assets/121480992/ebd31a52-dff7-4892-a470-ab5cffcd867d">

**First, connect the Employees origin to the Slowly Changing Dimension processor. Next, connect the Updates origin to the Slowly Changing Dimension process. The line connecting from Employees to the SCD processor should have a 1 where it connects to the SCD, and the line connecting the Updates origin should have a 2**

<img width="960" alt="image" src="https://github.com/King4424/Snowpark-and-StreamSets/assets/121480992/88d09bb6-dcbd-49c0-bce2-386ea6398bd2">

**In the settings for the Slowly Changing Dimension, add the following configurations:**

:small_blue_diamond: SCD Type: Type 2

:small_blue_diamond: Key Fields: ID

:small_blue_diamond: Specify Version Field: Checked

:small_blue_diamond: Version Field: VERSION

:small_blue_diamond: Specify Active Flag: Checked

:small_blue_diamond: Active Flag Field: ACTIVE_FLAG

:small_blue_diamond: Active Field Type: True-False

:small_blue_diamond: Specify Timestamp Fields: Checked

:small_blue_diamond: Start Timestamp Field: START_TIMESTAMP

:small_blue_diamond: End Timestamp Field: END_TIMESTAMP

:small_blue_diamond: Timestamp Expression: CURRENT_TIMESTAMP (Default Value)

:small_blue_diamond: Behavior for New Fields: Remove from Change Data


<img width="959" alt="image" src="https://github.com/King4424/Snowpark-and-StreamSets/assets/121480992/c857439f-78c9-4550-bfc4-5fa7cac88d4d">

## 6. Add Snowflake Table Destination
**The final step to complete this pipeline is to add the Table Destination.**

**Select Snowflake Table from the "Select New Destination to connect" menu at the top of the canvas or the menu on the right, just be sure to select the Snowflake Table Destination, indicated by the red "D".**


<img width="960" alt="image" src="https://github.com/King4424/Snowpark-and-StreamSets/assets/121480992/475c3032-890d-4963-a8e8-1e848b7d5b5d">

<img width="960" alt="image" src="https://github.com/King4424/Snowpark-and-StreamSets/assets/121480992/541ff53c-0491-4799-87c3-c9b6cc2c4fa2">

## 7. Preview Results
**Now that the pipeline has sources and a target, there should be no validation warnings showing, and the pipeline can be previewed.**

**Activate the preview by selecting the eye icon in the top right of the canvas.**

<img width="959" alt="image" src="https://github.com/King4424/Snowpark-and-StreamSets/assets/121480992/c40044fc-52bc-4e80-98b7-34b4a807d05f">

<img width="959" alt="image" src="https://github.com/King4424/Snowpark-and-StreamSets/assets/121480992/2c56bca9-4d0b-4edf-b0f7-f86c11445d66">
















