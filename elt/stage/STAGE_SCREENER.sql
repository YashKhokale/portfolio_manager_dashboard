INSERT INTO STAGE.STAGE_SCREENER
SELECT
	PUBLIC.UDF_REPLACE_CRN(DATA_LIST[1]) AS "S.No.",
	UPPER(SPLIT_PART(PUBLIC.UDF_REPLACE_CRN(DATA_LIST[2]), '/', 3)) AS link,
	PUBLIC.UDF_REPLACE_CRN(DATA_LIST[3]) AS name,
	PUBLIC.UDF_REPLACE_CRN(DATA_LIST[4]) AS CMP,
	PUBLIC.UDF_REPLACE_CRN(DATA_LIST[5]) AS PE,
	PUBLIC.UDF_REPLACE_CRN(DATA_LIST[6]) AS mar_cap,
	PUBLIC.UDF_REPLACE_CRN(DATA_LIST[7]) AS div_yv,
	PUBLIC.UDF_REPLACE_CRN(DATA_LIST[8]) AS ROCE,
	PUBLIC.UDF_REPLACE_CRN(DATA_LIST[9]) AS PEG,
	PUBLIC.UDF_REPLACE_CRN(DATA_LIST[10]) AS "5yr_OPM",
	PUBLIC.UDF_REPLACE_CRN(DATA_LIST[11]) AS "5yr_ROCE",
	load_date AS load_date
FROM
	LOAD.LOAD_SCREENER
WHERE 
NOT EXISTS 
(
SELECT 1 FROM STAGE.STAGE_SCREENER
WHERE stage_screener.link=upper(SPLIT_PART(PUBLIC.UDF_REPLACE_CRN(DATA_LIST[1]), '/', 3))
)
AND 
UPPER(SPLIT_PART(PUBLIC.UDF_REPLACE_CRN(DATA_LIST[2]), '/', 3)) != ''
--OFFSET 1
;