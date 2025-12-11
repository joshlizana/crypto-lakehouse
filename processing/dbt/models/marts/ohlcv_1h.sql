{{
    config(
        materialized='incremental',
        unique_key=['product_id', 'window_start'],
        incremental_strategy='merge',
        partition_by=['_partition_date', 'product_id']
    )
}}

/*
    1-hour OHLCV candles.
    Built from 1-minute candles for efficiency.
*/

WITH minute_candles AS (
    SELECT *
    FROM {{ ref('ohlcv_1m') }}
    {% if is_incremental() %}
    WHERE window_start >= (
        SELECT COALESCE(MAX(window_start), TIMESTAMP '1970-01-01')
        FROM {{ this }}
    ) - INTERVAL '2' HOUR
    {% endif %}
),

-- Get first and last minute candle per hour for open/close
first_last AS (
    SELECT
        product_id,
        DATE_TRUNC('hour', window_start) AS hour_bucket,
        FIRST_VALUE(open) OVER (
            PARTITION BY product_id, DATE_TRUNC('hour', window_start)
            ORDER BY window_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS open_price,
        LAST_VALUE(close) OVER (
            PARTITION BY product_id, DATE_TRUNC('hour', window_start)
            ORDER BY window_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS close_price,
        window_start
    FROM minute_candles
),

aggregated AS (
    SELECT
        mc.product_id,
        DATE_TRUNC('hour', mc.window_start) AS window_start,
        DATE_TRUNC('hour', mc.window_start) + INTERVAL '1' HOUR AS window_end,

        -- OHLC (from first/last minute candles)
        fl.open_price AS open,
        MAX(mc.high) AS high,
        MIN(mc.low) AS low,
        fl.close_price AS close,

        -- Volume metrics
        SUM(mc.volume) AS volume,
        SUM(mc.trade_count) AS trade_count,

        -- VWAP (volume-weighted from minute candles)
        SUM(mc.vwap * mc.volume) / NULLIF(SUM(mc.volume), 0) AS vwap,

        -- Partition column
        DATE(mc.window_start) AS _partition_date

    FROM minute_candles mc
    JOIN first_last fl
        ON mc.product_id = fl.product_id
        AND mc.window_start = fl.window_start
    GROUP BY
        mc.product_id,
        DATE_TRUNC('hour', mc.window_start),
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
