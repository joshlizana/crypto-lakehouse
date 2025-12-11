{{
    config(
        materialized='incremental',
        unique_key=['product_id', 'date'],
        incremental_strategy='merge',
        partition_by=['_partition_month']
    )
}}

/*
    Daily trading metrics including OHLCV, volatility, and drawdown.
*/

WITH hourly_candles AS (
    SELECT *
    FROM {{ ref('ohlcv_1h') }}
    {% if is_incremental() %}
    WHERE DATE(window_start) >= (
        SELECT COALESCE(MAX(date), DATE '1970-01-01')
        FROM {{ this }}
    ) - INTERVAL '1' DAY
    {% endif %}
),

-- Get first and last hour candle per day for open/close
first_last AS (
    SELECT
        product_id,
        DATE(window_start) AS trade_date,
        FIRST_VALUE(open) OVER (
            PARTITION BY product_id, DATE(window_start)
            ORDER BY window_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS open_price,
        LAST_VALUE(close) OVER (
            PARTITION BY product_id, DATE(window_start)
            ORDER BY window_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS close_price,
        window_start
    FROM hourly_candles
),

daily_agg AS (
    SELECT
        hc.product_id,
        DATE(hc.window_start) AS date,

        -- OHLC
        fl.open_price AS open,
        MAX(hc.high) AS high,
        MIN(hc.low) AS low,
        fl.close_price AS close,

        -- Volume metrics
        SUM(hc.volume) AS total_volume,
        SUM(hc.trade_count) AS total_trades,

        -- For volatility calculation
        COLLECT_LIST(hc.close) AS closes

    FROM hourly_candles hc
    JOIN first_last fl
        ON hc.product_id = fl.product_id
        AND hc.window_start = fl.window_start
    GROUP BY
        hc.product_id,
        DATE(hc.window_start),
        fl.open_price,
        fl.close_price
),

with_metrics AS (
    SELECT
        product_id,
        date,
        open,
        high,
        low,
        close,
        total_volume,
        total_trades,

        -- Daily return
        (close - open) / NULLIF(open, 0) AS daily_return,

        -- Volatility (simple: high-low range as % of open)
        (high - low) / NULLIF(open, 0) AS volatility,

        -- Max drawdown (simplified: from high to low as %)
        (high - low) / NULLIF(high, 0) AS max_drawdown,

        -- Partition column (monthly)
        DATE_TRUNC('month', date) AS _partition_month

    FROM daily_agg
)

SELECT
    product_id,
    date,
    CAST(open AS DECIMAL(18, 8)) AS open,
    CAST(high AS DECIMAL(18, 8)) AS high,
    CAST(low AS DECIMAL(18, 8)) AS low,
    CAST(close AS DECIMAL(18, 8)) AS close,
    CAST(total_volume AS DECIMAL(18, 8)) AS total_volume,
    total_trades,
    CAST(daily_return AS DECIMAL(18, 8)) AS daily_return,
    CAST(volatility AS DECIMAL(18, 8)) AS volatility,
    CAST(max_drawdown AS DECIMAL(18, 8)) AS max_drawdown,
    _partition_month
FROM with_metrics
