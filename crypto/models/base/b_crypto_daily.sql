-- models/base/base_crypto_daily.sql

with final as (
SELECT
    id,
    symbol,
    name,
    DATE_TRUNC('day', last_updated) AS snapshot_date,
    AVG(current_price) AS avg_price_day,
    MAX(market_cap) AS max_market_cap_day,
    SUM(total_volume) AS total_volume_day,
    AVG(roi_percentage) AS avg_roi_percentage_day
FROM {{ ref('st_crypto_model') }}
GROUP BY 1,2,3, DATE_TRUNC('day', last_updated)
)

select * from final
