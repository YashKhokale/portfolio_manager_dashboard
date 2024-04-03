CREATE VIEW BIZ.STOCK_VIEW AS
WITH BIZ_VIEW AS
(
SELECT 
	h_stock.stock_symbol,
	s_stock_date.price,
	h_date.date
FROM
	raw.l_stock_date
JOIN raw.h_date ON
	h_date.date_key = l_stock_date.date_key
JOIN raw.h_stock ON
	h_stock.stock_key = l_stock_date.stock_key
JOIN raw.s_stock_status ON
	s_stock_status.stock_key = h_stock.stock_key
	AND s_stock_status.is_current
	AND s_stock_status.is_active
JOIN raw.s_stock_date ON
	s_stock_date.stock_date_link_key = l_stock_date.stock_date_link_key
	AND s_stock_date.is_current
WHERE h_date.date !='1900-01-01'
ORDER BY h_date.date
)
SELECT BIZ.*
,equity."Industry"
,equity."Sector Name"
,equity."Industry New Name"
,equity."Igroup Name"
,equity."ISubgroup Name"
FROM BIZ_VIEW AS BIZ
JOIN yash_schema.equity ON
equity."Security Id" = BIZ.stock_symbol
WHERE BIZ.stock_symbol IN (
SELECT DISTINCT stock_symbol FROM BIZ_VIEW
WHERE EXTRACT(YEAR FROM BIZ_VIEW.DATE) <= EXTRACT(YEAR FROM current_date) - 7
)
ORDER BY BIZ.DATE ASC
;