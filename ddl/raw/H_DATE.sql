CREATE TABLE IF NOT EXISTS RAW.H_DATE (
	DATE_KEY CHAR(32) NOT NULL,
	LOAD_DATE TIMESTAMP NOT NULL,
	RECORD_SOURCE VARCHAR NOT NULL,
	DATE DATE,
	PRIMARY KEY (DATE_KEY)
);
INSERT INTO RAW.H_DATE
	(DATE_KEY, LOAD_DATE, RECORD_SOURCE, DATE)
VALUES
	('00000000000000000000000000000000', '1900-01-01', 'System', '1900-01-01')
;