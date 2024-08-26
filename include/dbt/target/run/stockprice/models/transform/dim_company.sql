

  create or replace view `stockprice-433416`.`stock`.`dim_company`
  OPTIONS()
  as -- Create the dimension table
WITH companies_cte AS (
	SELECT DISTINCT
	    simbolo as company_id,
	    zip AS zip_code,
		country,
		industry,
		enterpriseValue as valuation
	FROM `stock`.`stock`.`raw_companies`
	WHERE simbolo IS NOT NULL
)
SELECT
    *
FROM companies_cte;

