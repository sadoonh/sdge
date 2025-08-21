CREATE SCHEMA IF NOT EXISTS production;

DROP TABLE IF EXISTS production.utility_data;

CREATE TABLE production.utility_data (
    id BIGINT PRIMARY KEY,
    file_name TEXT,
    zip_code VARCHAR(10),        
    month SMALLINT CHECK (month BETWEEN 1 AND 12),
    year SMALLINT,
    customer_class CHAR(1) CHECK (customer_class IN ('A','C','I','R')),
    combined BOOLEAN,
    total_customer INTEGER,
    total_kwh BIGINT,
    average_kwh NUMERIC
);