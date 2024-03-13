UPDATE
	RAW.S_STOCK_STATUS
SET 
	LOAD_END_DATE = (SELECT MAX(LOAD_DATE) FROM STAGE.STAGE_STOCK_COMBINED_VIEW),
	IS_CURRENT = FALSE
FROM
	RAW.H_STOCK
LEFT OUTER JOIN
	STAGE.STAGE_STOCK_COMBINED_VIEW ON
		STAGE_STOCK_COMBINED_VIEW.STOCK_SYMBOL_KEY = H_STOCK.STOCK_KEY
WHERE
	S_STOCK_STATUS.STOCK_KEY = H_STOCK.STOCK_KEY AND
	S_STOCK_STATUS.IS_CURRENT AND
	S_STOCK_STATUS.IS_ACTIVE AND
	(STAGE_STOCK_COMBINED_VIEW.STOCK_SYMBOL_KEY IS NULL OR S_STOCK_STATUS.STOCK_KEY <> STAGE_STOCK_COMBINED_VIEW.STOCK_SYMBOL_KEY)
;