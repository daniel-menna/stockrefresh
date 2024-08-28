
  
    

    create or replace table `stockprice-433416`.`stock`.`dim_company`
      
    
    

    OPTIONS()
    as (
      -- Create the dimension table
WITH companies_cte AS (
	SELECT DISTINCT
	    symbol as company_id,
		shortName as company_name,
	    zip,
		country,
		industry,
		enterpriseValue as valuation
	FROM `stockprice-433416`.`stock`.`raw_companies`
	WHERE symbol IS NOT NULL
)
SELECT
    *
FROM companies_cte
    );
  