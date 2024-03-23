INSERT INTO RAW.H_DATE (
	DATE_KEY,
	LOAD_DATE,
	RECORD_SOURCE,
	DATE
)
SELECT DISTINCT
	DATE_KEY,
	LOAD_DATE,
	STRING_AGG(SPLIT_PART(record_source , '_', 2), '||') AS RECORD_SOURCE,
	DATE
FROM 
	STAGE.STAGE_STOCK_COMBINED_VIEW
WHERE
	DATE_KEY <> '00000000000000000000000000000000' AND
	NOT EXISTS (
		SELECT 1
		FROM RAW.H_DATE 
		WHERE H_DATE.DATE_KEY = STAGE_STOCK_COMBINED_VIEW.DATE_KEY
	)
GROUP BY DATE_KEY,LOAD_DATE,DATE
;