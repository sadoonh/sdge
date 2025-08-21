INSERT INTO production.utility_data (
    zipcode,
    month,
    year,
    customer_class,
    combined,
    total_customer,
    totalkwh,
    averagekwh
)
SELECT
    zipcode,                                   -- stays text, target is VARCHAR(10)
    CAST(month AS SMALLINT),                   -- convert to number 1–12
    CAST(year AS SMALLINT),                    -- e.g. 2020, 2021
    UPPER(TRIM(customer_class)),               -- normalize A/C/I/R
    CASE 
        WHEN combined ILIKE 'Y' THEN TRUE
        WHEN combined ILIKE 'N' THEN FALSE
        ELSE NULL 
    END,
    CAST(total_customer AS INTEGER),           -- safe integer cast
    CAST(totalkwh AS BIGINT),                  -- large consumption values
    CAST(averagekwh AS NUMERIC)                -- can hold decimals
FROM landing.utility_data;
