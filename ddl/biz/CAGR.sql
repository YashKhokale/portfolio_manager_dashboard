-- DROP FUNCTION PUBLIC.process_string_array(input_array TEXT[]);
--
CREATE OR REPLACE FUNCTION PUBLIC.process_string_array(input_array TEXT[])
RETURNS int
AS $$
BEGIN
    -- Iterate over each element of the input array
    FOR i IN 1..array_length(input_array, 1) LOOP
        -- Perform some operation on each element (printing it in this example)
        RAISE NOTICE 'Element %: %', i, input_array[i];
--       
       input_array[i]= (WITH GET_YEAR AS
(
SELECT stock_symbol,MAX(STOCK_VIEW.date) AS MAX_SYM_YEAR
FROM BIZ.STOCK_VIEW
WHERE stock_symbol=input_array[i]
GROUP BY EXTRACT(YEAR FROM STOCK_VIEW.date),stock_symbol
ORDER BY MAX(STOCK_VIEW.date) ASC
),
YEAR_PRICE AS 
(
SELECT 
	GET_YEAR.MAX_SYM_YEAR,
	STOCK_VIEW.stock_symbol,
	STOCK_VIEW.PRICE 
FROM BIZ.STOCK_VIEW
JOIN GET_YEAR ON
GET_YEAR.MAX_SYM_YEAR = STOCK_VIEW.date
AND GET_YEAR.stock_symbol = STOCK_VIEW.stock_symbol
),
GET_DIFF AS
(
SELECT 
	EXTRACT(YEAR FROM YEAR_PRICE.MAX_SYM_YEAR) AS EX_YEAR,
	AVG(YEAR_PRICE.PRICE) AS AVG_PRICE,
	LEAD(AVG(YEAR_PRICE.PRICE)) OVER (order by EXTRACT(YEAR FROM YEAR_PRICE.MAX_SYM_YEAR)) - AVG(YEAR_PRICE.PRICE)
	AS PRICE_DIFF
FROM YEAR_PRICE
GROUP BY EXTRACT(YEAR FROM YEAR_PRICE.MAX_SYM_YEAR)
)
SELECT 
	(SELECT 
		(POWER((SELECT AVG_PRICE FROM GET_DIFF WHERE EX_YEAR= (SELECT MAX(EX_YEAR) FROM GET_DIFF)) / (SELECT AVG_PRICE FROM GET_DIFF WHERE EX_YEAR= (SELECT MIN(EX_YEAR) FROM GET_DIFF)),1/(SELECT (SELECT MAX(EX_YEAR) FROM GET_DIFF) - (SELECT MIN(EX_YEAR) FROM GET_DIFF)))-1)*100
	) AS CAGR)
;
--       
--       
--       
    END LOOP;
--   SELECT avg(val::float) AS average_value FROM unnest(input_array) AS val;
--
   RETURN (SELECT avg(CAST(val AS DECIMAL)) AS average_value FROM unnest(input_array) AS val);
END;
$$
LANGUAGE plpgsql;
-- SELECT PUBLIC.process_string_array(array_agg(DISTINCT stock_symbol))
-- FROM biz.stock_view
-- LIMIT 50000;

