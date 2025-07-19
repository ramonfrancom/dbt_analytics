{{
    config(
        materialized = 'incremental',
        unique_key = 'curr_first_week_day',
        incremental_strategy = 'merge'
    )
}}

WITH base_table AS (
    SELECT 
        *,
        LAST_VALUE(USDMXN) OVER (PARTITION BY DATE_TRUNC('WEEK', DATE_VAL) ORDER BY DATE_VAL ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_weekly_value,
        DATE_TRUNC('WEEK', DATE_VAL) AS first_week_day
    FROM {{ ref("stg_cv_exchange_rates" )}}
    {% if is_incremental() %}
        WHERE UPDATED_AT >= ( SELECT coalesce(MAX(last_updated_run),'1990-01-01') FROM {{this}})
    {% endif %}
), weekly_values AS (
    SELECT  
        first_week_day,
        last_weekly_value,
        MAX(USDMXN) AS MAX_VAL,
        AVG(USDMXN) as AVG_VAL,
        MAX(UPDATED_AT) as MAX_UPDATED_AT
    FROM base_table
    GROUP BY first_week_day,last_weekly_value
), weekly_comparison AS (
    SELECT
        prev.first_week_day || '/' || curr.first_week_day AS week_comparison_text,
        prev.first_week_day AS prev_first_week_day,
        curr.first_week_day AS curr_first_week_day,
        curr.last_weekly_value AS curr_last_val,
        prev.last_weekly_value AS prev_last_val,
        (prev.last_weekly_value - curr.last_weekly_value) AS diff_last_val,
        ((prev.last_weekly_value - curr.last_weekly_value)/prev.last_weekly_value) * 100 AS percentage_diff_last_val,
        curr.MAX_VAL AS curr_max_val,
        prev.MAX_VAL as prev_max_val,
        (prev.MAX_VAL - curr.MAX_VAL) AS diff_max_val,
        ((prev.MAX_VAL - curr.MAX_VAL)/prev.MAX_VAL) * 100 AS percentage_diff_max_val,
        curr.AVG_VAL AS curr_AVG_VAL,
        prev.AVG_VAL AS prev_AVG_VAL,
        (prev.AVG_VAL - curr.AVG_VAL) AS diff_avg_val,
        ((prev.AVG_VAL - curr.AVG_VAL)/prev.AVG_VAL) * 100 AS percentage_diff_AVG_VAL,
        curr.MAX_UPDATED_AT AS last_updated_run,
        CURRENT_TIMESTAMP AS run_timestamp
    FROM weekly_values AS curr
    JOIN weekly_values AS prev
        ON curr.first_week_day = DATEADD(WEEK, +1,prev.first_week_day)
)

SELECT * FROM weekly_comparison
ORDER BY PREV_FIRST_WEEK_DAY ASC