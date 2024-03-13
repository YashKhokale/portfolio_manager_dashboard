WITH CODE_SYM_MAPPING AS
(
SELECT 
COALESCE(eq1."Security Id",eq2."Security Id") AS symbol,
COALESCE(eq1."ISIN No",eq2."ISIN No") AS ISIN_num,
COALESCE(eq1."Security Code",eq2."Security Code") AS code,
stage_screener.name
FROM yash_schema.stage_screener
LEFT JOIN yash_schema.equity AS eq1
ON eq1."Security Id"=stage_screener.link
LEFT JOIN yash_schema.equity AS eq2
ON eq2."Security Code"::text=stage_screener.link
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
FROM CODE_SYM_MAPPING
WHERE NOT EXISTS 
(
SELECT 1 FROM yash_schema.biz_stock_data 
WHERE biz_stock_data.symbol = CODE_SYM_MAPPING.symbol
)
ORDER BY CODE_SYM_MAPPING.symbol
;