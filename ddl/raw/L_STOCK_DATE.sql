CREATE TABLE IF NOT EXISTS RAW.L_STOCK_DATE (
	STOCK_DATE_LINK_KEY CHAR(32) NOT NULL,
	LOAD_DATE TIMESTAMP NOT NULL,
	RECORD_SOURCE VARCHAR NOT NULL,
	STOCK_KEY CHAR(32) NOT NULL,
	STOCK_SYMBOL VARCHAR,
	DATE_KEY CHAR(32) NOT NULL,
	DATE DATE,
	PRIMARY KEY (STOCK_DATE_LINK_KEY),
	FOREIGN KEY (STOCK_KEY) REFERENCES RAW.H_STOCK (STOCK_KEY),
	FOREIGN KEY (DATE_KEY) REFERENCES RAW.H_DATE (DATE_KEY)
);