CREATE SCHEMA IF NOT EXISTS landing;

DROP TABLE IF EXISTS landing.utility_data;

CREATE TABLE landing.utility_data (
    id BIGINT PRIMARY KEY,
    file_name TEXT,
    zip_code TEXT,
    month TEXT,
    year TEXT,
    customer_class TEXT,
    combined TEXT,
    total_customer TEXT,
    total_kwh TEXT,
    average_kwh TEXT
);