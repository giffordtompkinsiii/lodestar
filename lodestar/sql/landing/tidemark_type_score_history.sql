DROP TABLE IF EXISTS ods.tidemark_type_score_history;
WITH type_scores AS (
    SELECT *
    FROM CROSSTAB($$
        WITH weighted_scores AS (
            SELECT s.asset_id
                ,s.date_key
                ,tm.id AS tidemark_id
                ,tt.type AS tidemark_type
                ,(COUNT(s.tidemark_value) OVER w::FLOAT)/(COUNT(s.asset_id) OVER w::FLOAT) AS confidence
                ,CASE 5
                    WHEN tm.high_good THEN
                        s.tidemark_score * tt.believability_weighting
                    ELSE (1 - s.tidemark_score) * tt.believability_weighting
                END AS weighted_score
            FROM ods.tidemark_score_history s
                INNER JOIN financial.tidemarks tm ON tm.tidemark = s.tidemark
                INNER JOIN financial.typed_tidemarks typ ON typ.tidemark_id = tm.id 
                INNER JOIN financial.tidemark_types tt ON tt.id = typ.type_id
            WINDOW w AS (PARTITION BY s.asset_id ,s.date_key)
        ), grouped_scores AS (
            SELECT w.asset_id 
                ,w.date_key 
                ,w.confidence_count
                ,w.tidemark_type
                ,AVG(w.weighted_score) AS avg_weighted_score
            FROM weighted_scores w
            GROUP BY w.asset_id 
                ,w.date_key 
                ,w.tidemark_type
                ,w.confidence_count
        )
        SELECT g.asset_id, g.date_key, g.confidence_count, g.tidemark_type, g.avg_weighted_score
        FROM grouped_scores g
        ORDER BY 1,2,3,4$$  -- could also just be "ORDER BY 1" here
        ,$$VALUES('growth'), ('health'), ('term'), ('value')$$
    ) AS ("asset_id" int
            ,"date_key" int
            ,"confidence" float
            ,"growth" float
            ,"health" float
            ,"term" float
            ,"value" float)
) 
-- INSERT INTO ods.tidemark_type_score_history
SELECT *
-- s.asset_id
--     ,s.date_key
--     ,s.count
--     ,s.growth 
--     ,s.health
--     ,s.value 
--     ,(s.growth + s.health + s.value) / 3 AS believaility
FROM type_scores s;