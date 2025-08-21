CREATE SCHEMA IF NOT EXISTS production;

DROP TABLE IF EXISTS production.fact_monthly_usage;

CREATE TABLE IF NOT EXISTS production.fact_monthly_usage (
  id                 BIGINT   PRIMARY KEY,  -- from staging.utility_data
  date_key           INTEGER  NOT NULL REFERENCES production.dim_date_month(date_key),
  zip_key            INTEGER  NOT NULL REFERENCES production.dim_zip(zip_key),

  customer_class     CHAR(1)  NOT NULL CHECK (customer_class IN ('A','C','I','R')), -- degenerate dim
  combined           BOOLEAN,  -- nullable to mirror staging

  total_customers    INTEGER  NOT NULL CHECK (total_customers >= 0),
  total_kwh          BIGINT   NOT NULL CHECK (total_kwh >= 0),
  average_kwh        NUMERIC  NOT NULL CHECK (average_kwh >= 0)
);