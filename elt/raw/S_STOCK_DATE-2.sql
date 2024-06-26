INSERT INTO RAW.S_STOCK_DATE (
	STOCK_DATE_LINK_KEY ,
	LOAD_DATE ,
	LOAD_END_DATE ,
	IS_CURRENT ,
	RECORD_SOURCE ,
	PRICE 
)
SELECT DISTINCT
	scv.STOCK_DATE_LINK_KEY,
	scv.LOAD_DATE,
	'9999-01-01'::TIMESTAMP,
	TRUE,
	ARRAY_AGG(DISTINCT scv.record_source) AS RECORD_SOURCE,
	scv.PRICE
FROM 
	STAGE.STAGE_STOCK_COMBINED_VIEW scv
LEFT JOIN
    RAW.S_STOCK_DATE ssd ON scv.STOCK_DATE_LINK_KEY = ssd.STOCK_DATE_LINK_KEY
                         AND (scv.PRICE IS NULL OR scv.PRICE = ssd.PRICE)
                         AND ssd.IS_CURRENT
WHERE
    ssd.STOCK_DATE_LINK_KEY IS NULL
GROUP BY scv.STOCK_DATE_LINK_KEY,scv.LOAD_DATE,scv.PRICE
;