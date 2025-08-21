CREATE SCHEMA IF NOT EXISTS production;

DROP TABLE IF EXISTS production.dim_date_month;

CREATE TABLE IF NOT EXISTS production.dim_date_month (
  date_key      INTEGER  PRIMARY KEY,                           -- e.g., 202501
  month_start   DATE     NOT NULL,                               -- first day of month
  month_end     DATE     NOT NULL,                               -- last day of month
  month         SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
  quarter       SMALLINT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
  year          SMALLINT NOT NULL,
  month_name    TEXT     NOT NULL,
  days_in_month SMALLINT NOT NULL CHECK (days_in_month BETWEEN 28 AND 31) -- 28..31
);