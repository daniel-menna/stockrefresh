
  
    

    create or replace table `stockprice-433416`.`stock`.`report_top_companies`
      
    
    

    OPTIONS()
    as (
      with top_companies as (
    SELECT company_id as Symbol,
        company_name as Name,
        valuation as Value
    FROM `stockprice-433416`.`stock`.`dim_company`
	ORDER BY valuation DESC
)
SELECT
    *
FROM top_companies
    );
  