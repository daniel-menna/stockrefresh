with companies_localization_cte as (
    SELECT cmp.company_id as Symbol,
        cmp.company_name as Name,
        cmp.valuation as Value,
        loc.lat as latitud,
        loc.long as longitud
    FROM `stockprice-433416`.`stock`.`dim_company` cmp
    INNER JOIN `stockprice-433416`.`stock`.`dim_locals` loc
    ON loc.company_id = cmp.company_id
)
SELECT
    *
FROM companies_localization_cte