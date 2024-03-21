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