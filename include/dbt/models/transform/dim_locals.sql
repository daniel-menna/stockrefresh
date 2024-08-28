-- Create the dimension table
WITH localization_cte AS (
    SELECT DISTINCT
        symbol AS company_id,
        CAST(lat AS STRING) AS lat,
        CAST(long AS STRING) AS long
    FROM {{ source('stock', 'raw_locals') }}
    WHERE symbol IS NOT NULL
)
SELECT
    *
FROM localization_cte