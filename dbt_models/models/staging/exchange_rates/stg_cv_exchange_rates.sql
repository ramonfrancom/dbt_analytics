WITH stg_cv_exchange_rates AS (
    SELECT
        *
    FROM {{ source('exchange_rates', 'cv_exchange_rates')  }}
)

SELECT * FROM stg_cv_exchange_rates