SELECT * FROM ods.assets;

DROP TABLE ods.assets;
CREATE TABLE ods.assets 
AS SELECT *
FROM landing.assets 
WHERE 0 = 1;
ALTER TABLE ods.assets 
    ADD COLUMN asset_id SERIAL PRIMARY KEY,
    ADD UNIQUE (uuid),
    ADD COLUMN etl_row_updated_datetime_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
SELECT *
FROM ods.assets ;

INSERT INTO ods.assets 
SELECT * FROM landing.assets 
ON CONFLICT DO UPDATE;


EXEC SQL BEGIN DECLARE SECTION;
const char *stmt = "INSERT INTO ? "
                   "SELECT * FROM ? "
                   "ON CONFLICT DO "
                   "UPDATE ";
EXEC SQL END DECLARE SECTION;

EXEC SQL PREPARE mystmt FROM :stmt;
 ...
EXEC SQL EXECUTE mystmt USING 42, 'foobar';

-- MERGE TABLES TYPE 1 ROWS
CREATE OR REPLACE PROCEDURE upsert_type_one(sourcetable)
LANGUAGE plpgsql
AS $$
DECLARE table_column VARCHAR;
BEGIN 
    FOR column_name IN 
        SELECT column_name FROM INFORMATION_SCHEMA WHERE table_name = sourcetable AND table_schema = 'landing';

1. Pull Columns 
2. stmt = """
    INSERT INTO ods.table_name 
    SELECT * FROM landing.table_name
    ON CONFLICT DO UPDATE
    SET ods.table_name.col1 = landing.table_name.col1,
    SET ods.table_name.col2 = landing.table_name.col2,
    ...
    SET ods.table_name.etl_row_updated_datetime_utc = current_timestamp;
