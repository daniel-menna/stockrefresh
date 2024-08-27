-- Create the dimension table
WITH companies_cte AS (
	SELECT DISTINCT
	    symbol as company_id,
		shortName as company_name,
	    zip,
		country,
		industry,
		enterpriseValue as valuation
	FROM {{ source('stock', 'raw_companies') }}
	WHERE symbol IS NOT NULL
)
SELECT
    *
FROM companies_cte