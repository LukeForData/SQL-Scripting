--create table to check if iteration occurs as you expect
CREATE OR REPLACE TABLE db.schema.debug_log_tbl_test (
    SITE_ID STRING,
    EVENT_DATE TIMESTAMP,
    EID STRING,
    RN NUMBER,
    SITE_STATUS STRING,
    LOG_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--use to debug iterative merge procedure, the idea is to see the rows it iterated through to make sure your logic is working
CREATE OR REPLACE PROCEDURE db.schema.debug_procedure_test()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {
    var sql_command = "SELECT ID, EVENT_DATE, PID, RN, STATUS FROM DB.SCHEMA.STAGE_TABLE WHERE to_date(event_date) BETWEEN 'xxxx-xx-xx' AND current_date() ORDER BY EVENT_DATE, RN;";
    var stmt = snowflake.createStatement({sqlText: sql_command});
    var rs = stmt.execute();
    
    while (rs.next()) {
        var id = rs.getColumnValue('ID');
        var event_date = rs.getColumnValue('EVENT_DATE');
        var pid = rs.getColumnValue('PID');
        var rn = rs.getColumnValue('RN');
        var status = rs.getColumnValue('STATUS');

        // Convert event_date to a recognized format
        var formatted_event_date = event_date.toISOString().replace('T', ' ').replace('Z', '');
        
        // Log the current values
        var log_command = `INSERT INTO DB.SCHEMA.debug_log_tbl_test (ID, EVENT_DATE, PID, RN, STATUS) VALUES ('${id}', '${formatted_event_date}', '${pid}', ${rn}, '${status}');`;
        var log_stmt = snowflake.createStatement({sqlText: log_command});
        log_stmt.execute();
        
        // Perform your merge logic here
        var merge_command = `
        MERGE INTO DB.SCHEMA.TABLESCDTYPE3 AS target
        USING (
          SELECT * FROM DB.SCHEMA.STAGING_TABLE
          WHERE ID = '${site_id}' AND PID = '${eid}' AND RN = ${rn}
        ) AS source
        ON target.id = source.id AND target.event_date = source.event_date
        WHEN MATCHED AND source.status = 'CHANGED' AND target.event_date <= source.event_date THEN
          UPDATE SET
            target.col = source.col -- or other logic
        WHEN NOT MATCHED AND source.status = 'CREATED' THEN
          INSERT (target columns to insert to)
          VALUES (source columns to insert);
        `;
        var merge_stmt = snowflake.createStatement({sqlText: merge_command});
        merge_stmt.execute();

    }
    
    return 'Procedure executed successfully.';
} catch (err) {
    return 'Error: ' + err.message;
}
$$;
