-- EXEC SQL BEGIN DECLARE SECTION;
-- const char *stmt = "INSERT INTO ? "
--                    "SELECT * FROM ? "
--                    "ON CONFLICT DO "
--                    "UPDATE ";
-- EXEC SQL END DECLARE SECTION;

-- EXEC SQL PREPARE mystmt FROM :stmt;
--  ...
-- EXEC SQL EXECUTE mystmt USING 42, 'foobar';

-- MERGE TABLES TYPE 1 ROWS
DROP PROCEDURE IF EXISTS logic.upsert_type_one(VARCHAR);
CREATE OR REPLACE PROCEDURE logic.upsert_type_one(sourcetable VARCHAR)
LANGUAGE plpgsql
AS $$
DECLARE 
    table_column VARCHAR;
    set_clause VARCHAR = 'SET etl_row_updated_datetime_utc = CURRENT_TIMESTMAP'; 
BEGIN 
    FOR table_column IN 
        SELECT column_name 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE table_name = sourcetable 
            AND table_schema = 'landing'
    LOOP
        set_clause = set_clause + ',SET a.% = %', column_name, column_name;
    END LOOP;
    RAISE NOTICE '%', set_clause;
END $$;
CALL logic.upsert_type_one('assets');

SELECT * FROM information_schema.routines


1. Pull Columns 
2. stmt = """
    INSERT INTO ods.table_name 
    SELECT * FROM landing.table_name
    ON CONFLICT DO UPDATE
    SET ods.table_name.col1 = landing.table_name.col1,
    SET ods.table_name.col2 = landing.table_name.col2,
    ...
    SET ods.table_name.etl_row_updated_datetime_utc = current_timestamp;
