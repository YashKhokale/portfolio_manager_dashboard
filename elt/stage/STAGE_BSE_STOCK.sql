INSERT INTO STAGE.STAGE_BSE_STOCK (
	STOCK_SYMBOL,
	DATE,
	PRICE,
	LOAD_DATE,
	RECORD_SOURCE,
	STOCK_SYMBOL_BUSINESS_KEY,
	STOCK_SYMBOL_KEY,
	DATE_BUSINESS_KEY,
	DATE_KEY,
	STOCK_DATE_LINK_KEY
)
WITH CLEANED AS (
SELECT
	PUBLIC.UDF_REPLACE_CRN(candles_data ->> 'securityID') AS STOCK_SYMBOL,
	(candles_data ->> 'currentValue')::NUMERIC AS price, 
	TO_CHAR(TO_DATE(SPLIT_PART(candles_data ->> 'updatedOn' , ' | ',1), 'DD Mon YY'), 'YYYY-MM-DD')::DATE AS date,
	record_source,
	load_date 
FROM
    load.load_stock,
    jsonb_array_elements(json_data) AS candles_data
WHERE record_source ILIKE 'latest_combined%'
)
SELECT
    STOCK_SYMBOL,
    DATE,
    PRICE,
    LOAD_DATE,
    RECORD_SOURCE,
    CASE WHEN STOCK_SYMBOL = '' THEN NULL ELSE UPPER(STOCK_SYMBOL) END AS STOCK_SYMBOL_BUSINESS_KEY,
    CASE WHEN STOCK_SYMBOL IS NULL THEN '00000000000000000000000000000000' ELSE MD5(UPPER(STOCK_SYMBOL)) END AS STOCK_SYMBOL_KEY,
    DATE AS DATE_BUSINESS_KEY,
    CASE WHEN DATE IS NULL THEN '00000000000000000000000000000000' ELSE MD5(DATE::TEXT) END AS DATE_KEY,
    MD5(
        COALESCE(CASE WHEN STOCK_SYMBOL = '' THEN NULL ELSE UPPER(STOCK_SYMBOL) END, '') || '||' ||
        COALESCE(DATE::TEXT, '')
    ) AS STOCK_DATE_LINK_KEY
FROM
    CLEANED
;