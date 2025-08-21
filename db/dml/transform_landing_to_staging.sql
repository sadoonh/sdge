INSERT INTO staging.utility_data (
    id,
    file_name,
    zip_code,
    month,
    year,
    customer_class,
    combined,
    total_customer,
    total_kwh,
    average_kwh
)
SELECT
    id,                                          -- surrogate key from merge
    file_name,                                   -- original file stem
    zip_code,                                    -- stays text
    CAST(month AS SMALLINT),                     -- 1–12
    CAST(year AS SMALLINT),                      -- e.g., 2020
    UPPER(TRIM(customer_class)),                 -- normalize A/C/I/R
    CASE 
        WHEN combined ILIKE 'Y' THEN TRUE
        WHEN combined ILIKE 'N' THEN FALSE
        ELSE NULL 
    END,
    CAST(total_customer AS INTEGER),             -- integer
    CAST(total_kwh AS BIGINT),                   -- large values
    CAST(average_kwh AS NUMERIC)                 -- decimals
FROM landing.utility_data;