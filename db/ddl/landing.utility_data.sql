CREATE SCHEMA IF NOT EXISTS landing;

CREATE TABLE landing.utility_data (
    zipcode TEXT,
    month TEXT,
    year TEXT,
    customer_class TEXT,
    combined TEXT,
    total_customer TEXT,
    totalkwh TEXT,
    averagekwh TEXT
);