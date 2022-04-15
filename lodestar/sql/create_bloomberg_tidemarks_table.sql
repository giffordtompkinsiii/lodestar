-- DROP TABLE IF EXISTS financial.bloomberg_tidemarks;
-- create table financial.bloomberg_tidemarks (
--     -- id SERIAL PRIMARY KEY
--     asset_id INT NOT NULL--REFERENCES financial.assets(id) NOT NULL
--     ,tidemark_id INT NOT NULL--REFERENCES financial.tidemarks(id) NOT NULL
--     ,date int NOT NULL 
--     ,tidemark_value FLOAT
-- );
-- --  CREATE UNIQUE INDEX bloomberg_tidemarks_asset_tidemark_date
-- -- ON financial.bloomberg_tidemarks(asset_id, tidemark_id, date)
-- --  ;

SELECT COUNT(*)
FROM financial.bloomberg_tidemarks b
INNER JOIN financial.tidemarks t ON t.id = b.tidemark_id

SELECT COUNT(*) FROM financial.tidemark_history;
