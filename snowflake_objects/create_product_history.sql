--create an XL warehouse to gen 1B records
CREATE WAREHOUSE COMPUTE_XL_WH WAREHOUSE_SIZE ='X-Large';
USE WAREHOUSE COMPUTE_XL_WH;

--create a history table with 1B records 
CREATE TABLE PRODUCT_HIST AS 
SELECT
 LPAD(seq4()+1, 9, '0') TRANSACTION_ID,
 dateadd(day, '-' || seq4(), current_timestamp()) as transaction_date,
 uniform(1, 6, random(15)) as VENDOR_ID,
 uniform(10000000, 99999999, random(21)) as Product_id,
 uniform(9, 2000, random(22)) as Product_price,
 uniform(1, 20, random(23)) as quantity,
 randstr(uniform(4,10,random(2)),uniform(1,10000,random(2)))::varchar(10) as product_name,
 randstr(uniform(6,20,random(4)),uniform(1,200000,random(4)))::varchar(20) as product_desc
 from table(generator(rowcount=>1000000000));
 
--turn off the XL warehouse 
ALTER WAREHOUSE COMPUTE_XL_WH SUSPEND;

--you can union this data with kafka data for realtime + historical results 
WITH PRODUCT_DATA AS 
  (SELECT  RECORD_CONTENT:product_price::number as PRODUCT_PRICE , RECORD_CONTENT:quantity::number as QUANTITY,  RECORD_CONTENT:vendor_id::number as vendor_id
     FROM PRODUCT_KAFKA_CONNECT P 
   UNION ALL 
  SELECT PRODUCT_PRICE, QUANTITY, VENDOR_ID FROM
   PRODUCT_HIST PH)
SELECT SUM(PRODUCT_PRICE * QUANTITY), VENDOR_NAME 
  FROM PRODUCT_DATA PD, VENDORS V
 WHERE PD.VENDOR_ID = V.VENDOR_ID
 GROUP BY VENDOR_NAME;