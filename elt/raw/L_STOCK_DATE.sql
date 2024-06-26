INSERT INTO RAW.L_STOCK_DATE (
	STOCK_DATE_LINK_KEY,
	LOAD_DATE,
	RECORD_SOURCE,
	STOCK_KEY,
	STOCK_SYMBOL,
	DATE_KEY,
	DATE
)
SELECT DISTINCT
	STOCK_DATE_LINK_KEY,
	LOAD_DATE,
	record_source AS RECORD_SOURCE,
	STOCK_SYMBOL_KEY,
	STOCK_SYMBOL_BUSINESS_KEY,
	DATE_KEY,
	DATE
FROM
	STAGE.STAGE_STOCK_COMBINED_VIEW
WHERE
	NOT EXISTS (
		SELECT 1
		FROM RAW.L_STOCK_DATE
		WHERE L_STOCK_DATE.STOCK_DATE_LINK_KEY = STAGE_STOCK_COMBINED_VIEW.STOCK_DATE_LINK_KEY
	)
;