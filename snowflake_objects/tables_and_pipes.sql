CREATE DATABASE SNOW_SANDBOX IF NOT EXSITS;
CREATE SCHEMA  SNOW_SANDBOX IF NOT EXSITS;

--create table to test snowpipe data loads 
CREATE TABLE PRODUCT_SNOWPIPE (PRODUCT_JSON VARIANT);
--create table to test kafka connector loads
CREATE TABLE PRODUCT_KAFKA_CONNECT (RECORD_METADATA VARIANT, RECORD_CONTENT VARIANT);
--create table to test kafka connector with streaming  
CREATE TABLE PRODUCT_KAFKA_STREAMING (RECORD_METADATA VARIANT, RECORD_CONTENT VARIANT);

--create vendor lookup table
CREATE TABLE VENDORS 
(VENDOR_ID NUMBER,
 VENDOR_NAME VARCHAR);
--populate with vendors
INSERT INTO VENDORS VALUES
(1, 'Home Improvement USA'), 
(2, 'Big Box Ltd'  ), 
(3, 'Electronic Center' ), 
(4, 'Organic Market' ), 
(5, 'Furniture Depot' ),
(6, 'Music Streaming Unlimited');

--create a stage to an S3 bucket where we will pick up data from snowpipe
create stage SNOW_SANDBOX.STREAMING.PRODUCT_DATA_S3 
  url='s3://ps-rsa-bucket/pneedleman/'
  storage_integration = S3_INT;


--create a Snowpipe Pipe to load data via SNS topic  
--this will automatically move files from the stage locaiton to a snowflake
--table (PRODUCT_SNOWPIPE) using serverless features.
create or replace pipe PRODUCT_PIPE auto_ingest=true  as 
copy into SNOW_SANDBOX.STREAMING.PRODUCT_SNOWPIPE
    from @PRODUCT_DATA_S3/pipe_data 
    file_format = (type = 'JSON');

--once created get the value from "notification_channel" to to your SNS queue of your S3 bucket 
--In AWS S3 bucket Properties:
--     |_Create event notification
--         |_Specify SQS queue
--             |_Choose from your SQS queues
--                 |_Enter SQS queue ARN
DESC PRODUCT_PIPE;

--create a table log to validate loadtimes for snowpipe streaming
create or replace TABLE TABLE_LOAD_LOG (
	TABLE_NM VARCHAR(16777216),
	TIMESTMP TIMESTAMP_NTZ(9),
	ROWCNT NUMBER(38,0)
);

--because kafka streaming doesn not yet have views for loadtimes, we can
--create a check for each second to see when the table is finished loading
--kickoff this job with "CALL STREAMING_STATS_TABLE_LOG ('PRODUCT_KAFKA_STREAMING');" 
--before running the datagen python script for Snowpipe streaming 
CREATE OR REPLACE PROCEDURE STREAMING_STATS_TABLE_LOG (TABLE_NM VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS 
$$
DECLARE
   sql_query string;
   num_seconds number := 10000;
   start_num number := 0;
BEGIN
   WHILE (start_num <= num_seconds) LOOP
        
        sql_query := 'INSERT INTO TABLE_LOAD_LOG SELECT \''|| :TABLE_NM  || '\'  , CURRENT_TIMESTAMP(),  COUNT(*) FROM ' || :table_nm;
        execute immediate :sql_query;
        start_num := start_num+1;
        let wait_sql := 'SELECT SYSTEM$WAIT(750, \'MILLISECONDS\')';
        execute immediate :wait_sql;
   END LOOP;     
    
    RETURN 'success running for '  || :num_seconds || ' seconds' ;
END
$$
;

