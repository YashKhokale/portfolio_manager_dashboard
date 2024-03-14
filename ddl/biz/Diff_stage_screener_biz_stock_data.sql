WITH CODE_SYM_MAPPING AS
(
SELECT
	COALESCE(eq1."Security Id",
	eq2."Security Id") AS symbol,
	COALESCE(eq1."ISIN No",
	eq2."ISIN No") AS ISIN_num,
	COALESCE(eq1."Security Code",
	eq2."Security Code") AS code,
	stage_screener.name
FROM
	yash_schema.stage_screener
LEFT JOIN yash_schema.equity AS eq1
ON
	eq1."Security Id" = stage_screener.link
LEFT JOIN yash_schema.equity AS eq2
ON
	eq2."Security Code"::TEXT = stage_screener.link
WHERE
	(
eq1."Security Id" IS NOT NULL
		OR 
eq2."Security Code" 
IS NOT NULL
))
SELECT
	symbol,
	isin_num,
	code,
	name
FROM
	CODE_SYM_MAPPING
WHERE
	NOT EXISTS 
(
WITH YEAR_AGG AS
(
	SELECT
		stock_symbol,
		EXTRACT(YEAR
	FROM
		max(date)) AS max_year,
		EXTRACT(YEAR
	FROM
		min(date)) AS min_year
	FROM
		biz.stock_view
	GROUP BY
		stock_symbol
)
	SELECT
		*
	FROM
		YEAR_AGG
	WHERE
		min_year != '2024'
		AND YEAR_AGG.stock_symbol = CODE_SYM_MAPPING.symbol
)
ORDER BY
	CODE_SYM_MAPPING.symbol
;
