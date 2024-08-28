-- Create the dimension table
WITH localization_cte AS (
    SELECT DISTINCT
        symbol AS company_id,
        CAST(lat AS float64) AS lat,
        CAST(long AS float64) AS long
    FROM {{ source('stock', 'raw_locals') }}
    WHERE symbol IS NOT NULL
)
SELECT
    *
FROM localization_cte