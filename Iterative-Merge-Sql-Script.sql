CREATE OR REPLACE PROCEDURE DATABASE.SCHEMA.ITERATIVE_MERGE_PROCEDURE()
RETURNS VARCHAR
LANGUAGE SQL
AS
DECLARE
    --Set CURSOR to iterate through all events, ideally ordered by a Unique Row ID and Row Number
    --This example is grabbing only a set of data as the Proc will run daily
    --The selection is taken from a staged data area
    --- (Rename anything where needed)
    cur CURSOR FOR
    SELECT PKEY_ID, UNIQUE_EVENT_ID, EVENT_DATE, ROW_NUMBER, ROW_TYPE -- 'CREATED' or 'CHANGED' or 'DELETED'
    FROM DATABASE.SCHEMA.HISTORICAL_EVENT_STAGE_TBL
    WHERE EVENT_DATE BETWEEN CURRENT_DATE()-2 AND CURRENT_DATE() -- Set interval as needed
    ORDER BY UNIQUE_EVENT_ID, ROW_NUMBER;
    --Define Iteration Variables
    v_UNIQUE_EVENT_ID STRING;
    v_EVENT_DATE TIMESTAMP;
    v_PKEY_ID STRING; --In this example, the stage had a primary key row id
    v_ROW_NUMBER NUMBER; --Useful for 'CHANGED' rows that share a profile id, the use case derived for this shared 'unique_event_id' for the type of actions
    v_ROW_TYPE STRING; --Example 'CREATED' or 'CHANGED' or 'DELETED'

BEGIN
    --Start by Opening the Cursor
    OPEN cur;

    --Loop through each row in the cursor
    --Ideally if the cursor is small, the iteration is faster
    ---For a very large selection, the iteration will be slower
    FOR r IN cur DO
        --Extract the necessary fields from the current record
        --And assign to the variable definitions
        v_UNIQUE_EVENT_ID := r.UNIQUE_EVENT_ID;
        v_EVENT_DATE := r.EVENT_DATE;
        v_PKEY_ID := r.PKEY_ID;
        v_ROW_NUMBER := r.ROW_NUMBER;
        v_ROW_TYPE := r.ROW_TYPE;
        
        MERGE INTO DATABASE.SCHEMA.FINAL_TABLE AS target
        USING (
            -- Filtering on variables of the table with the cursor
            SELECT * FROM DATABASE.SCHEMA.HISTORICAL_EVENT_STAGE_TBL
            WHERE UNIQUE_EVENT_ID = :v_UNIQUE_EVENT_ID AND PKEY_ID = :v_PKEY_ID AND ROW_NUMBER = :v_ROW_NUMBER
            ) AS source
        ON 
            target.UNIQUE_EVENT_ID = source.UNIQUE_EVENT_ID
        WHEN MATCHED AND source.ROW_TYPE = 'CHANGED' AND target.EVENT_DATE < source.EVENT_DATE THEN
            UPDATE SET
                --Update actions
        WHEN MATCHED AND source.ROW_TYPE = 'DELETED' AND target.EVENT_DATE < source.EVENT_DATE THEN
            DELETE
                --Delete actions
        WHEN NOT MATCHED AND source.ROW_TYPE = 'CREATED' THEN
                --Insert actions
            INSERT (target.COLNAME)
            VALUES (source.COLNAME);
    
    END FOR;

    CLOSE cur;
    
END;
