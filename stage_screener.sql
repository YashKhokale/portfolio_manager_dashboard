INSERT INTO yash_schema.stage_screener
SELECT
	upper(SPLIT_PART(PUBLIC.UDF_REPLACE_CRN("1"), '/', 3)) AS link,
	PUBLIC.UDF_REPLACE_CRN("2") AS name,
	PUBLIC.UDF_REPLACE_CRN("3") AS CMP,
	PUBLIC.UDF_REPLACE_CRN("4") AS PE,
	PUBLIC.UDF_REPLACE_CRN("5") AS mar_cap,
	PUBLIC.UDF_REPLACE_CRN("6") AS div_yv,
	PUBLIC.UDF_REPLACE_CRN("8") AS qtr_profit_var,
	PUBLIC.UDF_REPLACE_CRN("9") AS sales,
	PUBLIC.UDF_REPLACE_CRN("11") AS ROCE,
	PUBLIC.UDF_REPLACE_CRN("12") AS PEG,
	PUBLIC.UDF_REPLACE_CRN("13") AS "5yr_OPM",
	load_date AS load_date
FROM
	yash_schema.load_screener
WHERE 
NOT EXISTS 
(
SELECT 1 FROM yash_schema.stage_screener
WHERE stage_screener.link=upper(SPLIT_PART(PUBLIC.UDF_REPLACE_CRN("1"), '/', 3))
)
AND 
upper(SPLIT_PART(PUBLIC.UDF_REPLACE_CRN("1"), '/', 3)) != ''
--OFFSET 1
;