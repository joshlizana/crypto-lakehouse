{{
    config(
        materialized='table'
    )
}}

/*
    Latest price for each trading pair.
    Rebuilt on each run (not incremental).
*/

WITH latest_trades AS (
    SELECT
        product_id,
        price,
        trade_time,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY trade_time DESC
        ) AS rn
    FROM {{ ref('stg_trades') }}
),

latest_1m AS (
    SELECT
        product_id,
        close AS price_1m_ago,
        window_start
    FROM {{ ref('ohlcv_1m') }}
    WHERE window_start >= CURRENT_TIMESTAMP - INTERVAL '2' MINUTE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY product_id
        ORDER BY window_start DESC
    ) = 2  -- Get the previous minute candle
),

latest_1h AS (
    SELECT
        product_id,
        close AS price_1h_ago,
        window_start
    FROM {{ ref('ohlcv_1h') }}
    WHERE window_start >= CURRENT_TIMESTAMP - INTERVAL '2' HOUR
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY product_id
        ORDER BY window_start DESC
    ) = 2  -- Get the previous hour candle
),

latest_24h AS (
    SELECT
        product_id,
        open AS price_24h_ago,
        high AS high_24h,
        low AS low_24h,
        total_volume AS volume_24h
    FROM {{ ref('daily_metrics') }}
    WHERE date = CURRENT_DATE - INTERVAL '1' DAY
)

SELECT
    lt.product_id,
    CAST(lt.price AS DECIMAL(18, 8)) AS price,
    lt.trade_time AS updated_at,

    -- Price changes
    CAST(
        (lt.price - COALESCE(l1m.price_1m_ago, lt.price)) / NULLIF(l1m.price_1m_ago, 0) * 100
        AS DECIMAL(10, 4)
    ) AS change_1m_pct,

    CAST(
        (lt.price - COALESCE(l1h.price_1h_ago, lt.price)) / NULLIF(l1h.price_1h_ago, 0) * 100
        AS DECIMAL(10, 4)
    ) AS change_1h_pct,

    CAST(
        (lt.price - COALESCE(l24.price_24h_ago, lt.price)) / NULLIF(l24.price_24h_ago, 0) * 100
        AS DECIMAL(10, 4)
    ) AS change_24h_pct,

    -- 24h stats
    CAST(l24.high_24h AS DECIMAL(18, 8)) AS high_24h,
    CAST(l24.low_24h AS DECIMAL(18, 8)) AS low_24h,
    CAST(l24.volume_24h AS DECIMAL(18, 8)) AS volume_24h,

    CURRENT_TIMESTAMP AS snapshot_time

FROM latest_trades lt
LEFT JOIN latest_1m l1m ON lt.product_id = l1m.product_id
LEFT JOIN latest_1h l1h ON lt.product_id = l1h.product_id
LEFT JOIN latest_24h l24 ON lt.product_id = l24.product_id
WHERE lt.rn = 1
