CREATE SCHEMA IF NOT EXISTS production;

CREATE TABLE production.utility_data (
    zipcode VARCHAR(10),        
    month SMALLINT CHECK (month BETWEEN 1 AND 12),
    year SMALLINT,
    customer_class CHAR(1) CHECK (customer_class IN ('A','C','I','R')),
    combined BOOLEAN,
    total_customer INTEGER,
    totalkwh BIGINT,
    averagekwh NUMERIC
);