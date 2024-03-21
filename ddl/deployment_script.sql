--
CREATE SCHEMA IF NOT EXISTS LOAD;
-- "load".load_stock definition
CREATE TABLE IF NOT EXISTS "load".load_stock (
	json_data jsonb NULL,
	record_source varchar NULL,
	load_date timestamp DEFAULT CURRENT_TIMESTAMP NULL
);
--
CREATE OR REPLACE FUNCTION PUBLIC.UDF_REPLACE_CRN(Convert_param VARCHAR)
RETURNS VARCHAR AS $$
BEGIN
    RETURN TRIM(regexp_replace(regexp_replace(regexp_replace(regexp_replace(COALESCE(Convert_param, ''), '\n', '','g'), '\r', ''), '\s\s*', ' ','g'), '\xa0', ''));
END;
$$ LANGUAGE plpgsql;
--
CREATE SCHEMA IF NOT EXISTS STAGE;
--
CREATE TABLE IF NOT EXISTS STAGE.STAGE_UPSTOX_STOCK (
	STOCK_SYMBOL VARCHAR,
	DATE DATE,
	PRICE NUMERIC,
	LOAD_DATE TIMESTAMP NOT NULL,
	RECORD_SOURCE VARCHAR NOT NULL,
	STOCK_SYMBOL_BUSINESS_KEY VARCHAR,
	STOCK_SYMBOL_KEY CHAR(32) NOT NULL,
	DATE_BUSINESS_KEY VARCHAR,
	DATE_KEY CHAR(32) NOT NULL,
	STOCK_DATE_LINK_KEY CHAR(32) NOT NULL
);
--
CREATE TABLE IF NOT EXISTS STAGE.STAGE_BSE_STOCK (
	STOCK_SYMBOL VARCHAR,
	DATE DATE,
	PRICE NUMERIC,
	LOAD_DATE TIMESTAMP NOT NULL,
	RECORD_SOURCE VARCHAR NOT NULL,
	STOCK_SYMBOL_BUSINESS_KEY VARCHAR,
	STOCK_SYMBOL_KEY CHAR(32) NOT NULL,
	DATE_BUSINESS_KEY VARCHAR,
	DATE_KEY CHAR(32) NOT NULL,
	STOCK_DATE_LINK_KEY CHAR(32) NOT NULL
);
--
CREATE VIEW STAGE.STAGE_STOCK_COMBINED_VIEW AS
   WITH combined_cte AS
(
SELECT 	
	STOCK_SYMBOL,
	DATE,
	PRICE,
	LOAD_DATE,
	RECORD_SOURCE,
	STOCK_SYMBOL_BUSINESS_KEY,
	STOCK_SYMBOL_KEY,
	DATE_BUSINESS_KEY,
	DATE_KEY,
	STOCK_DATE_LINK_KEY 
FROM stage.stage_upstox_stock
UNION ALL
SELECT 	
	STOCK_SYMBOL,
	DATE,
	PRICE,
	LOAD_DATE,
	RECORD_SOURCE,
	STOCK_SYMBOL_BUSINESS_KEY,
	STOCK_SYMBOL_KEY,
	DATE_BUSINESS_KEY,
	DATE_KEY,
	STOCK_DATE_LINK_KEY 
FROM stage.stage_bse_stock
),
rank_date_sym AS
(
SELECT 	
	STOCK_SYMBOL,
	DATE,
	PRICE,
	LOAD_DATE,
	RECORD_SOURCE,
	STOCK_SYMBOL_BUSINESS_KEY,
	STOCK_SYMBOL_KEY,
	DATE_BUSINESS_KEY,
	DATE_KEY,
	STOCK_DATE_LINK_KEY,
	ROW_NUMBER () OVER (PARTITION BY STOCK_SYMBOL_BUSINESS_KEY,DATE_BUSINESS_KEY ORDER BY LOAD_DATE DESC) AS latest_rec
FROM combined_cte
)
SELECT * FROM rank_date_sym
WHERE latest_rec=1
;
--
CREATE SCHEMA IF NOT EXISTS RAW;
--
CREATE TABLE IF NOT EXISTS RAW.H_DATE (
	DATE_KEY CHAR(32) NOT NULL,
	LOAD_DATE TIMESTAMP NOT NULL,
	RECORD_SOURCE VARCHAR NOT NULL,
	DATE DATE NOT NULL,
	PRIMARY KEY (DATE_KEY)
);
-- INSERT INTO RAW.H_DATE
-- 	(DATE_KEY, LOAD_DATE, RECORD_SOURCE, DATE)
-- VALUES
-- 	('00000000000000000000000000000000', '1900-01-01', 'System', '1900-01-01')
-- ;
--
CREATE TABLE IF NOT EXISTS RAW.H_STOCK (
	STOCK_KEY CHAR(32) NOT NULL,
	LOAD_DATE TIMESTAMP NOT NULL,
	RECORD_SOURCE VARCHAR NOT NULL,
	STOCK_SYMBOL VARCHAR NOT NULL,
	PRIMARY KEY (STOCK_KEY)
);
-- INSERT INTO RAW.H_STOCK
-- 	(STOCK_KEY, LOAD_DATE, RECORD_SOURCE, STOCK_SYMBOL)
-- VALUES
-- 	('00000000000000000000000000000000', '1900-01-01', 'System', 'UNKNOWN')
-- ;
--
CREATE TABLE IF NOT EXISTS RAW.L_STOCK_DATE (
	STOCK_DATE_LINK_KEY CHAR(32) NOT NULL,
	LOAD_DATE TIMESTAMP NOT NULL,
	RECORD_SOURCE VARCHAR NOT NULL,
	STOCK_KEY CHAR(32) NOT NULL,
	STOCK_SYMBOL VARCHAR NOT NULL,
	DATE_KEY CHAR(32) NOT NULL,
	DATE DATE NOT NULL,
	PRIMARY KEY (STOCK_DATE_LINK_KEY),
	FOREIGN KEY (STOCK_KEY) REFERENCES RAW.H_STOCK (STOCK_KEY),
	FOREIGN KEY (DATE_KEY) REFERENCES RAW.H_DATE (DATE_KEY)
);
--
CREATE TABLE IF NOT EXISTS RAW.S_STOCK_DATE_STATUS (
  STOCK_DATE_LINK_KEY CHAR(32) NOT NULL,
  LOAD_DATE TIMESTAMP NOT NULL,
  LOAD_END_DATE TIMESTAMP NOT NULL,
  IS_CURRENT BOOLEAN NOT NULL,
  RECORD_SOURCE VARCHAR NOT NULL,
  IS_ACTIVE BOOLEAN NOT NULL,
  PRIMARY KEY (STOCK_DATE_LINK_KEY, LOAD_DATE),
  FOREIGN KEY (STOCK_DATE_LINK_KEY) REFERENCES RAW.L_STOCK_DATE (STOCK_DATE_LINK_KEY)
);
--
CREATE TABLE IF NOT EXISTS RAW.S_STOCK_DATE (
	STOCK_DATE_LINK_KEY CHAR(32) NOT NULL,
	LOAD_DATE TIMESTAMP NOT NULL,
	LOAD_END_DATE TIMESTAMP NOT NULL,
	IS_CURRENT BOOLEAN NOT NULL,
	RECORD_SOURCE VARCHAR,
	PRICE NUMERIC,
	PRIMARY KEY (STOCK_DATE_LINK_KEY, LOAD_DATE),
	FOREIGN KEY (STOCK_DATE_LINK_KEY) REFERENCES RAW.L_STOCK_DATE (STOCK_DATE_LINK_KEY)
);
--
CREATE TABLE IF NOT EXISTS RAW.S_STOCK_STATUS (
  STOCK_KEY CHAR(32) NOT NULL,
  LOAD_DATE TIMESTAMP NOT NULL,
  LOAD_END_DATE TIMESTAMP NOT NULL,
  IS_CURRENT BOOLEAN NOT NULL,
  RECORD_SOURCE VARCHAR NOT NULL,
  IS_ACTIVE BOOLEAN NOT NULL,
  PRIMARY KEY (STOCK_KEY, LOAD_DATE),
  FOREIGN KEY (STOCK_KEY) REFERENCES RAW.H_STOCK (STOCK_KEY)
);
--
CREATE SCHEMA IF NOT EXISTS BIZ;
--
CREATE OR REPLACE VIEW BIZ.STOCK_VIEW AS 
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
--








