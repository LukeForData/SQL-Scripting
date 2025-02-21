--original process here by Dan Flippo: https://medium.com/snowflake/loading-csv-as-semi-structured-files-in-snowflake-d7d76dfc37bf
--python function here: https://gist.github.com/sfc-gh-dflippo/a6163fc84b35ea735ee775d15f819799


---- A full example of this in use


---STEP 1: Create Python Function 'PARSE_CSV'
--python function here: https://gist.github.com/sfc-gh-dflippo/a6163fc84b35ea735ee775d15f819799

---STEP 2: Create File Format
CREATE FILE FORMAT IF NOT EXISTS DB.SCHEMA.TEXT_FORMAT_NOHEADERSKIP 
TYPE = 'CSV' 
FIELD_DELIMITER = NONE
SKIP_BLANK_LINES = TRUE
EMPTY_FIELD_AS_NULL = TRUE
ESCAPE_UNENCLOSED_FIELD = NONE;

---STEP 3: Create Stage
CREATE STAGE IF NOT EXISTS DB.SCHEMA_EXTERNAL_STAGES.CSV_DATA_STAGE 
    URL = 's3://or-other-storage'
    STORAGE_INTEGRATION = INTEGRATION_NAME
    FILE_FORMAT = DB.SCHEMA.TEXT_FORMAT_NOHEADERSKIP ;

---STEP 3:  Create Initial Landing Zone Table (and populate table)
CREATE TABLE IF NOT EXISTS DB.SCHEMA.RAW_CSV_DATA_TBL AS
SELECT
    STG.METADATA$START_SCAN_TIME AS LOADED_AT,
    STG.METADATA$FILENAME as FILENAME,
    STG.METADATA$FILE_ROW_NUMBER as FILE_ROW_NUMBER,
    STG.$1 AS ORIGINAL_TEXT_RAW
FROM @DB.SCHEMA_EXTERNAL_STAGES.CSV_DATA_STAGE 
(FILE_FORMAT => DB.SCHEMA.TEXT_FORMAT_NOHEADERSKIP, PATTERN => '.*.csv') STG;
----------------or as empty
CREATE TABLE IF NOT EXISTS DB.SCHEMA.RAW_CSV_DATA_TBL (
  LOADED_AT TIMESTAMP_LTZ,
	FILENAME VARCHAR,
	FILE_ROW_NUMBER NUMBER,
	ORIGINAL_TEXT_RAW VARCHAR
  );

--//Create a stream to detect new data appended to the table (used for later steps)
CREATE STREAM IF NOT EXISTS DB.SCHEMA.RAW_CSV_DATA_TBL_STREAM ON TABLE DB.SCHEMA.RAW_CSV_DATA_TBL APPEND_ONLY = TRUE;

---STEP 4:  Create the pipe to automatically load new files
CREATE PIPE IF NOT EXISTS DB.SCHEMA_PIPES.PIPE_CSV_DATA_RAW auto_ingest=true AS
COPY INTO DB.SCHEMA.RAW_CSV_DATA_TBL
FROM
(SELECT
    STG.METADATA$START_SCAN_TIME AS LOADED_AT,
    STG.METADATA$FILENAME as FILENAME,
    STG.METADATA$FILE_ROW_NUMBER as FILE_ROW_NUMBER,
    STG.$1 AS ORIGINAL_TEXT_RAW
FROM @DB.SCHEMA_EXTERNAL_STAGES.CSV_DATA_STAGE STG
)
PATTERN = '.*.csv';

--////PIPE Settings and Maintenance
--//pause pipes
ALTER PIPE DB.SCHEMA_PIPES.PIPE_CSV_DATA_RAW SET pipe_execution_paused = true;

--//unpause pipes
ALTER PIPE DB.SCHEMA_PIPES.PIPE_CSV_DATA_RAW SET pipe_execution_paused = false;

---STEP 5:  Create a table to transform csv into json records (here we convert the raw csvs to records)
CREATE TABLE IF NOT EXISTS DB_CURATED.SCHEMA_TRANSFORM.RAW_CSV_DATA_TO_JSON_TBL AS
SELECT
  SPLIT(STG.filename, '/')[1] || '-' || STG.file_row_number AS PKEY, --a unique key, name as you wish
  LOADED_AT,
  FILENAME, 
  FILE_ROW_NUMBER,
  STG.ORIGINAL_TEXT_RAW AS ORIGINAL_TEXT_RAW,
  CSV_PARSER.V AS DATA_CONVERTED_JSON
-- Query the stage for one file or use a pattern for multiple
FROM DB.SCHEMA.RAW_CSV_DATA_TBL STG
-- Lateral join to call our Python UDTF that parses CSV into JSON
  ---Input DELIMITER type
  ---The parser will remove the header row, so you will have one less count of rows
JOIN LATERAL DB.SCHEMA_FUNCTIONS.PARSE_CSV(STG.ORIGINAL_TEXT_RAW, '|', '"') 
  -- Partition by file to support multiple files at once
  OVER (PARTITION BY FILENAME 
  -- Order by row number to ensure headers are first in each window
  ORDER BY FILE_ROW_NUMBER) AS CSV_PARSER;

--//Create a Stream to detect new data appended to this table
CREATE STREAM IF NOT EXISTS DB_CURATED.SCHEMA_TRANSFORM.RAW_CSV_DATA_TO_JSON_TBL_STREAM ON TABLE DB_CURATED.SCHEMA_TRANSFORM.RAW_CSV_DATA_TO_JSON_TBL APPEND_ONLY = TRUE;


---STEP 6:  Create a final analytics table, that ensures uniqueness (stage the table or create a blank table)
CREATE TABLE IF NOT EXISTS DB_ANALYTICS.SCHEMA_TGT.ANALYTICS_TGT_TBL AS
SELECT DISTINCT
    PKEY,
    data_converted_json['COL1']::varchar AS COL1,
    data_converted_json['COL2']::varchar AS COL2,
    data_converted_json['COL3']::timestamp_ntz AS COL3,
    data_converted_json['COL4']::varchar AS COL4,
    data_converted_json['COL5']::varchar AS COL5,
    TRIM(SPLIT(data_converted_json['COL6_STRING']::varchar, ':')[0], '"') AS COL6_STRING_NUMBER,
    TRIM(SPLIT(data_converted_json['COL7_STRING']::varchar, ':')[1], '".') AS COL7_STRING_DESCRIPTION
FROM DB_CURATED.SCHEMA_TRANSFORM.RAW_CSV_DATA_TO_JSON_TBL;

---Step 7. Create Procedures and Tasks to Automate data Insertion to Curated Transform Table and Analytics Ready Table
--//Create Procedure for Task 1
CREATE PROCEDURE IF NOT EXISTS DB.SCHEMA.RAW_TO_CURATED_TBL_PROCEDURE()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    MERGE INTO DB_CURATED.SCHEMA_TRANSFORM.RAW_CSV_DATA_TO_JSON_TBL tgt
        --we select the stream as the stage, which should only have the latest records
        USING (SELECT
                SPLIT(STG.filename, '/')[1] || '-' || STG.file_row_number AS PKEY,
                LOADED_AT,
                FILENAME, 
                FILE_ROW_NUMBER,
                STG.ORIGINAL_TEXT_RAW AS ORIGINAL_TEXT_RAW,
                CSV_PARSER.V AS DATA_CONVERTED_JSON
                -- Query the stage for one file or use a pattern for multiple
                FROM DB.SCHEMA.RAW_CSV_DATA_TBL_STREAM STG
                -- Lateral join to call our Python UDTF that parses CSV into JSON
                JOIN LATERAL DB.SCHEMA_FUNCTIONS.PARSE_CSV(STG.ORIGINAL_TEXT_RAW, '|', '"') 
                -- Partition by file to support multiple files at once
                OVER (PARTITION BY FILENAME 
                -- Order by row number to ensure headers are first in each window
                ORDER BY FILE_ROW_NUMBER) AS CSV_PARSER) src
            ON tgt.PKEY = src.PKEY
        WHEN NOT MATCHED THEN
                    INSERT (tgt.PKEY,
	                          tgt.LOADED_AT,
                            tgt.FILENAME,
                        	  tgt.FILE_ROW_NUMBER,
                            tgt.ORIGINAL_TEXT_RAW,
                            tgt.DATA_CONVERTED_JSON
                           )
                    VALUES (src.PKEY,
	                          src.LOADED_AT,
                            src.FILENAME,
                        	  src.FILE_ROW_NUMBER,
                            src.ORIGINAL_TEXT_RAW,
                            src.DATA_CONVERTED_JSON
                            );
                            
    IF (SQLFOUND = true) THEN
            RETURN 'Updated ' || SQLROWCOUNT || ' rows. Merge operation completed';
        ELSEIF (SQLNOTFOUND = true) THEN
            RETURN 'No rows updated.';
        ELSE
            RETURN 'SQLFOUND and SQLNOTFOUND are not true.';
        END IF;
  
END;

--//Create Procedure for Task 2
CREATE PROCEDURE IF NOT EXISTS DB.SCHEMA.CURATED_TO_FINAL_TBL_PROCEDURE()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    MERGE INTO DB_ANALYTICS.SCHEMA_TGT.ANALYTICS_TGT_TBL tgt
        USING (SELECT
                PKEY,
                data_converted_json['COL1']::varchar AS SOURCE_HIO,
                data_converted_json['COL2']::varchar AS TARGET_HIO,
                data_converted_json['COL3']::timestamp_ntz AS TIME_CREATED,
                data_converted_json['COL4']::varchar AS XCA_TYPE,
                data_converted_json['COL5']::varchar AS SESSION_ID,
                TRIM(SPLIT(data_converted_json['COL6_STRING']::varchar, ':')[0], '"') AS COL6_STRING_NUMBER,
                TRIM(SPLIT(data_converted_json['COL6_STRING']::varchar, ':')[1], '".') AS COL6_STRING_DESCRIPTION
                FROM DB_CURATED.SCHEMA_TRANSFORM.RAW_CSV_DATA_TO_JSON_TBL_STREAM)  src
                    ON tgt.PKEY = src.PKEY
        WHEN NOT MATCHED THEN
                    INSERT (tgt.PKEY,
	                          tgt.COL1,
                            tgt.COL2,
                            tgt.COL3,
                        	  tgt.COL4,
                            tgt.COL5,
                            tgt.COL6_STRING_NUMBER,
                            tgt.COL6_STRING_DESCRIPTION
                           )
                    VALUES (src.PKEY,
	                          src.COL1,
                            src.COL2,
                            src.COL3,
                        	  src.COL4,
                            src.COL5,
                            src.COL6_STRING_NUMBER,
                            src.COL6_STRING_DESCRIPTION
                            );
                            
    IF (SQLFOUND = true) THEN
            RETURN 'Updated ' || SQLROWCOUNT || ' rows. Merge operation completed';
        ELSEIF (SQLNOTFOUND = true) THEN
            RETURN 'No rows updated.';
        ELSE
            RETURN 'SQLFOUND and SQLNOTFOUND are not true.';
        END IF;
  
END;

--//Create Tasks for Raw Table to Curated Table (First Task)
CREATE TASK IF NOT EXISTS DB.SCHEMA.CSV_TO_JSON_TRIGGER_TASK
    WAREHOUSE = TASK_WH
  WHEN system$stream_has_data('DB.SCHEMA.RAW_CSV_DATA_TBL_STREAM')
  AS
    CALL DB.SCHEMA.RAW_TO_CURATED_TBL_PROCEDURE();



--//Create Task for Curated Table to Analytics Table (Second Task)
CREATE TASK IF NOT EXISTS DB.SCHEMA.JSON_TO_FINAL_TRIGGER_TASK
    WAREHOUSE = TASK_WH
    AFTER DB.SCHEMA.CSV_TO_JSON_TRIGGER_TASK
  WHEN system$stream_has_data('DB_CURATED.SCHEMA_TRANSFORM.RAW_CSV_DATA_TO_JSON_TBL_STREAM')
  AS
    CALL DB.SCHEMA.CURATED_TO_FINAL_TBL_PROCEDURE();


    
--//enables all task in a task tree
----place the first task in tree here to enable all in tree
SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('DB.SCHEMA.CSV_TO_JSON_TRIGGER_TASK');
 
ALTER TASK DB.SCHEMA.CSV_TO_JSON_TRIGGER_TASK SUSPEND;
ALTER TASK DB.SCHEMA.JSON_TO_FINAL_TRIGGER_TASK SUSPEND;
