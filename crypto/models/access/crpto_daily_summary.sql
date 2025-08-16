WITH daily AS (
    SELECT *
    FROM {{ ref('b_crypto_daily') }}
),

price_change AS (
    SELECT *
    FROM {{ ref('b_crypto_price_changes') }}
),

market_metrics AS (
    SELECT *
    FROM {{ ref('b_crypto_market_metrics') }}
)

SELECT
    d.snapshot_date,
    d.symbol,
    d.name,
    
    -- Daily snapshot metrics
    d.avg_price_day,
    d.max_market_cap_day,
    d.total_volume_day,
    d.avg_roi_percentage_day,
    
    -- Price change metrics
    pc.close_price,
    pc.low_price,
    pc.high_price,
    pc.price_change,
    pc.price_change_pct,
    pc.roi_times AS roi_times_daily,
    
    -- Market metrics
    mm.circulating_supply,
    mm.total_supply,
    mm.circulating_supply_pct,
    mm.market_cap_to_fdv_pct,
    mm.roi_currency,
    mm.roi_percentage AS roi_percentage_daily

FROM daily d
LEFT JOIN price_change pc
    ON d.symbol = pc.symbol AND d.snapshot_date = pc.snapshot_date
LEFT JOIN market_metrics mm
    ON d.symbol = mm.symbol AND d.snapshot_date = mm.snapshot_date

ORDER BY d.snapshot_date DESC, d.symbol
