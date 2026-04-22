-- Truncate and rebuild the mart table
TRUNCATE TABLE mart_fx_daily_summary;

INSERT INTO mart_fx_daily_summary (
    target_currency,
    rate_date,
    avg_rate,
    min_rate,
    max_rate,
    rate_vs_prev
)
WITH daily_rates AS (
    SELECT
        target_currency,
        rate_date,
        AVG(rate)  AS avg_rate,
        MIN(rate)  AS min_rate,
        MAX(rate)  AS max_rate
    FROM raw_fx_rates
    GROUP BY target_currency, rate_date
),
with_change AS (
    SELECT
        target_currency,
        rate_date,
        avg_rate,
        min_rate,
        max_rate,
        ROUND(
            (avg_rate - LAG(avg_rate) OVER (
                PARTITION BY target_currency ORDER BY rate_date
            )) /
            NULLIF(LAG(avg_rate) OVER (
                PARTITION BY target_currency ORDER BY rate_date
            ), 0) * 100,
        4) AS rate_vs_prev
    FROM daily_rates
)
SELECT * FROM with_change;
