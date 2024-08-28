-- Crie a tabela fato juntando as chaves relevantes da tabela de dimensão
WITH fct_stock_cte AS (
    SELECT
        symbol AS company_id,
        CAST(Date AS STRING) AS datetime_id,
        Close as price
    FROM `stockprice-433416`.`stock`.`raw_stocks`
)

SELECT
    dc.company_id,
    dt.datetime_id,
    fs.price
FROM fct_stock_cte fs
INNER JOIN `stockprice-433416`.`stock`.`dim_datetime` dt ON fs.datetime_id = dt.datetime_id
INNER JOIN `stockprice-433416`.`stock`.`dim_company` dc ON fs.company_id = dc.company_id