WITH daily_data AS (
    SELECT
        id,
        symbol,
        DATE_TRUNC('day', last_updated) AS snapshot_date,
        MAX(current_price) AS close_price,
        MIN(current_price) AS low_price,
        MAX(current_price) AS high_price,
        MAX(roi_percentage) AS roi_percentage,
        MAX(roi_times) AS roi_times
    FROM {{ ref('st_crypto_model') }}
    GROUP BY id, symbol, DATE_TRUNC('day', last_updated)
)

SELECT
    *,
    close_price - LAG(close_price) OVER (PARTITION BY symbol ORDER BY snapshot_date) AS price_change,
    CASE 
        WHEN LAG(close_price) OVER (PARTITION BY symbol ORDER BY snapshot_date) IS NULL THEN NULL
        ELSE (close_price - LAG(close_price) OVER (PARTITION BY symbol ORDER BY snapshot_date)) / LAG(close_price) OVER (PARTITION BY symbol ORDER BY snapshot_date) * 100
    END AS price_change_pct
FROM daily_data
