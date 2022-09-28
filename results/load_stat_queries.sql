---get stats for PRODUCT_SNOWPIPE from COPY_HISTORY
SELECT   MAX(TIMESTAMPDIFF('second',PIPE_RECEIVED_TIME, LAST_LOAD_TIME )),
         MIN( TIMESTAMPDIFF('second',PIPE_RECEIVED_TIME, LAST_LOAD_TIME )),
         AVG(TIMESTAMPDIFF('second',PIPE_RECEIVED_TIME, LAST_LOAD_TIME )),
         MIN(LAST_LOAD_TIME) min_load, MAX(LAST_LOAD_TIME) max_load, 
         MIN(PIPE_RECEIVED_TIME)min_rec , MAX(PIPE_RECEIVED_TIME) max_rec, 
         TIMESTAMPDIFF('second', min_rec, max_rec)--,
         --TIMESTAMPDIFF('second', max_rec, max_load)
  FROM  table(information_schema.copy_history(table_name=>'PRODUCT_SNOWPIPE', start_time=> dateadd(hours, -4, current_timestamp())));


---get stats for PRODUCT_KAFKA_CONNECT from COPY_HISTORY
  SELECT   MAX(TIMESTAMPDIFF('second',PIPE_RECEIVED_TIME, LAST_LOAD_TIME )),
         MIN( TIMESTAMPDIFF('second',PIPE_RECEIVED_TIME, LAST_LOAD_TIME )),
         AVG(TIMESTAMPDIFF('second',PIPE_RECEIVED_TIME, LAST_LOAD_TIME )),
         MIN(LAST_LOAD_TIME) min_load, MAX(LAST_LOAD_TIME) max_load, 
         MIN(PIPE_RECEIVED_TIME)min_rec , MAX(PIPE_RECEIVED_TIME) max_rec, 
         TIMESTAMPDIFF('second', min_rec, max_rec)--,
         --TIMESTAMPDIFF('second', max_rec, max_load)
  FROM  table(information_schema.copy_history(table_name=>'PRODUCT_KAFKA_CONNECT', start_time=> dateadd(hours, -4, current_timestamp())));



--##STATS from custom log table for PRODUCT_KAFKA_STREAMING
--this is needed because the PrPr version of Kafka Snowpipe Streaming does not contain load stats 

--get the min, max and avg between loads to snowflake
SELECT MIN(TIMEDIFF), MAX(TIMEDIFF), AVG(TIMEDIFF) FROM 
(SELECT  ROWCNT, MIN_PER_GROUP, MAX_PER_GROUP, LAG(MIN_PER_GROUP) OVER (ORDER BY MIN_PER_GROUP) LAG_MIN,
        TIMEDIFF('seconds', LAG_MIN, MIN_PER_GROUP ) as TIMEDIFF
FROM 
(SELECT  TIMESTMP, ROWCNT, MAX(TIMESTMP) OVER (PARTITION BY ROWCNT)MAX_PER_GROUP, MIN(TIMESTMP) OVER (PARTITION BY ROWCNT) MIN_PER_GROUP
FROM TABLE_LOAD_LOG ORDER BY TIMESTMP DESC) 
WHEERE
QUALIFY TIMEDIFF <> 0
ORDER BY ROWCNT DESC);


--get the total elapsed time of the run
SELECT 
       MAX(CASE WHEN ROWCNT =0 THEN MAX_PER_GROUP END) START_TIME, 
       MIN(CASE WHEN ROWCNT =9000 THEN MIN_PER_GROUP END) END_TIME,
       TIMEDIFF('seconds', START_TIME ,END_TIME) / 60
FROM 
(SELECT  TIMESTMP, ROWCNT, MAX(TIMESTMP) OVER (PARTITION BY ROWCNT)MAX_PER_GROUP, MIN(TIMESTMP) OVER (PARTITION BY ROWCNT) MIN_PER_GROUP
FROM TABLE_LOAD_LOG ORDER BY TIMESTMP DESC)
WHERE ROWCNT IN (0, 9000)
ORDER BY ROWCNT DESC;