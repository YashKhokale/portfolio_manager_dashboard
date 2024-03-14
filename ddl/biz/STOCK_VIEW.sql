CREATE VIEW BIZ.STOCK_VIEW AS 
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
;