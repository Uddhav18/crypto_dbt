-- models/staging/stg_crypto.sql
with final as (

SELECT

    data:id::string                           AS id,
    data:symbol::string                       AS symbol,
    data:name::string                         AS name,
    data:image::string                        AS image,
    data:current_price::float                 AS current_price,
    data:market_cap::float                    AS market_cap,
    data:market_cap_rank::int                 AS market_cap_rank,
    data:fully_diluted_valuation::float       AS fully_diluted_valuation,
    data:total_volume::float                  AS total_volume,
    data:circulating_supply::float            AS circulating_supply,
    data:total_supply::float                  AS total_supply,
    data:max_supply::float                    AS max_supply,
    data:high_24h::float                      AS high_24h,
    data:low_24h::float                       AS low_24h,
    data:price_change_24h::float              AS price_change_24h,
    data:price_change_percentage_24h::float   AS price_change_percentage_24h,
    data:market_cap_change_24h::float         AS market_cap_change_24h,
    data:market_cap_change_percentage_24h::float AS market_cap_change_percentage_24h,
    data:ath::float                           AS ath,
    data:ath_change_percentage::float         AS ath_change_percentage,
    TRY_TO_TIMESTAMP(data:ath_date::string)   AS ath_date,
    data:atl::float                           AS atl,
    data:atl_change_percentage::float         AS atl_change_percentage,
    TRY_TO_TIMESTAMP(data:atl_date::string)   AS atl_date,
    data:roi:currency::string                 AS roi_currency,
    data:roi:percentage::float                AS roi_percentage,
    data:roi:times::float                     AS roi_times,
    TRY_TO_TIMESTAMP(data:last_updated::string) AS last_updated,
    ingestion_ts

FROM {{ source('landing', 'raw_crypto_data') }}
)
select * from final