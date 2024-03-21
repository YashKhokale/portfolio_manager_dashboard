CREATE TABLE LOAD.LOAD_STOCK (
	json_data jsonb NULL,
	record_source varchar NULL,
	load_date timestamp DEFAULT CURRENT_TIMESTAMP NULL
);