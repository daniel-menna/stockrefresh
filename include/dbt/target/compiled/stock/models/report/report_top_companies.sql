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