{{
    config(
        materialized='incremental',
        unique_key=['product_id', 'window_start'],
        incremental_strategy='merge',
        partition_by=['_partition_date', 'product_id']
    )
}}

/*
    1-minute OHLCV (Open, High, Low, Close, Volume) candles.
    Incrementally built from staging trades.
*/

WITH trades AS (
    SELECT *
    FROM {{ ref('stg_trades') }}
    {% if is_incremental() %}
    WHERE minute_bucket >= (
        SELECT COALESCE(MAX(window_start), TIMESTAMP '1970-01-01')
        FROM {{ this }}
    ) - INTERVAL '1' HOUR
    {% endif %}
),

-- Get first and last trade per minute for open/close
first_last AS (
    SELECT
        product_id,
        minute_bucket,
        FIRST_VALUE(price) OVER (
            PARTITION BY product_id, minute_bucket
            ORDER BY trade_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS open_price,
        LAST_VALUE(price) OVER (
            PARTITION BY product_id, minute_bucket
            ORDER BY trade_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS close_price,
        trade_id
    FROM trades
),

aggregated AS (
    SELECT
        t.product_id,
        t.minute_bucket AS window_start,
        t.minute_bucket + INTERVAL '1' MINUTE AS window_end,

        -- OHLC
        fl.open_price AS open,
        MAX(t.price) AS high,
        MIN(t.price) AS low,
        fl.close_price AS close,

        -- Volume metrics
        SUM(t.size) AS volume,
        COUNT(*) AS trade_count,

        -- VWAP (Volume Weighted Average Price)
        SUM(t.trade_value) / NULLIF(SUM(t.size), 0) AS vwap,

        -- Partition column
        DATE(t.minute_bucket) AS _partition_date

    FROM trades t
    JOIN first_last fl
        ON t.trade_id = fl.trade_id
        AND t.product_id = fl.product_id
        AND t.minute_bucket = fl.minute_bucket
    GROUP BY
        t.product_id,
        t.minute_bucket,
        fl.open_price,
        fl.close_price
)

SELECT
    product_id,
    window_start,
    window_end,
    CAST(open AS DECIMAL(18, 8)) AS open,
    CAST(high AS DECIMAL(18, 8)) AS high,
    CAST(low AS DECIMAL(18, 8)) AS low,
    CAST(close AS DECIMAL(18, 8)) AS close,
    CAST(volume AS DECIMAL(18, 8)) AS volume,
    trade_count,
    CAST(vwap AS DECIMAL(18, 8)) AS vwap,
    _partition_date
FROM aggregated
