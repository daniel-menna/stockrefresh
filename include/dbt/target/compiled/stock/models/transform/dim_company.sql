-- Create the dimension table
WITH companies_cte AS (
	SELECT DISTINCT
	    simbolo as company_id,
	    zip AS zip_code,
		country,
		industry,
		enterpriseValue as valuation
	FROM `stockprice-433416`.`stock`.`raw_companies`
	WHERE simbolo IS NOT NULL
)
SELECT
    *
FROM companies_cte