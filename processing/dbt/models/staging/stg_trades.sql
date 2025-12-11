{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for Silver trades.
    Light transformation layer that prepares data for Gold aggregations.
*/

SELECT
    trade_id,
    product_id,
    price,
    size,
    side,
    trade_time,
    ingested_at,
    _is_late_arrival,
    _source_latency_ms,
    _trade_date,

    -- Computed fields
    price * size AS trade_value,
    EXTRACT(HOUR FROM trade_time) AS trade_hour,
    EXTRACT(MINUTE FROM trade_time) AS trade_minute,
    DATE_TRUNC('minute', trade_time) AS minute_bucket,
    DATE_TRUNC('hour', trade_time) AS hour_bucket

FROM {{ source('silver', 'trades') }}
WHERE trade_time IS NOT NULL
  AND price > 0
  AND size > 0
