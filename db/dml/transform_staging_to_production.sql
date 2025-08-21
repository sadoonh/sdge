-- 1) production.dim_date_month
WITH bounds AS (
  SELECT make_date(MIN(year), MIN(month), 1) AS min_m,
         make_date(MAX(year), MAX(month), 1) AS max_m
  FROM staging.utility_data
),
series AS (
  SELECT date_trunc('month', dd)::date AS month_start
  FROM bounds, generate_series(
         (SELECT min_m FROM bounds),
         (SELECT max_m FROM bounds),
         interval '1 month') AS dd
)
INSERT INTO production.dim_date_month (date_key, month_start, month, quarter, year, month_name)
SELECT (EXTRACT(YEAR FROM month_start)::int * 100 + EXTRACT(MONTH FROM month_start)::int) AS date_key,
       month_start,
       EXTRACT(MONTH FROM month_start)::int                                              AS month,
       EXTRACT(QUARTER FROM month_start)::int                                            AS quarter,
       EXTRACT(YEAR FROM month_start)::int                                               AS year,
       TO_CHAR(month_start, 'Mon')                                                       AS month_name
FROM series
ON CONFLICT (date_key) DO NOTHING;

-- 2) production.dim_zip
INSERT INTO production.dim_zip (zip_code)
SELECT DISTINCT zip_code
FROM staging.utility_data
WHERE zip_code IS NOT NULL
ON CONFLICT (zip_code) DO NOTHING;

-- 3) production.fact_monthly_usage
INSERT INTO production.fact_monthly_usage (
  id, date_key, zip_key, customer_class, combined,
  total_customers, total_kwh, average_kwh
)
SELECT
  ud.id,
  (ud.year * 100 + ud.month)          AS date_key,
  dz.zip_key,
  ud.customer_class,
  ud.combined,
  ud.total_customer,
  ud.total_kwh,
  ud.average_kwh
FROM staging.utility_data ud
JOIN production.dim_zip dz
  ON dz.zip_code = ud.zip_code
JOIN production.dim_date_month dd
  ON dd.date_key = (ud.year * 100 + ud.month)
ON CONFLICT (id) DO UPDATE
SET date_key        = EXCLUDED.date_key,
    zip_key         = EXCLUDED.zip_key,
    customer_class  = EXCLUDED.customer_class,
    combined        = EXCLUDED.combined,
    total_customers = EXCLUDED.total_customers,
    total_kwh       = EXCLUDED.total_kwh,
    average_kwh     = EXCLUDED.average_kwh;