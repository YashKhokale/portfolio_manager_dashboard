INSERT INTO RAW.S_STOCK_STATUS (
  STOCK_KEY,
  LOAD_DATE,
  LOAD_END_DATE,
  IS_CURRENT,
  RECORD_SOURCE,
  IS_ACTIVE
)
SELECT DISTINCT
  STOCK_SYMBOL_KEY,
  LOAD_DATE,
  '9999-01-01'::TIMESTAMP,
  TRUE,
	ARRAY_AGG(DISTINCT STAGE_STOCK_COMBINED_VIEW.record_source) AS RECORD_SOURCE,
  TRUE
FROM
  STAGE.STAGE_STOCK_COMBINED_VIEW
WHERE
  NOT EXISTS (
    SELECT 1
    FROM RAW.S_STOCK_STATUS
    WHERE
      S_STOCK_STATUS.STOCK_KEY = STAGE_STOCK_COMBINED_VIEW.STOCK_SYMBOL_KEY AND
      S_STOCK_STATUS.IS_CURRENT AND
      S_STOCK_STATUS.IS_ACTIVE
  )
GROUP BY STOCK_SYMBOL_KEY,LOAD_DATE
;