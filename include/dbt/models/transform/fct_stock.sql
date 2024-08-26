-- Crie a tabela fato juntando as chaves relevantes da tabela de dimensão
WITH fct_stock_cte AS (
    SELECT
        simbolo AS company_id,
        CAST(Date AS STRING) AS datetime_id,
        Close as value
    FROM {{ source('stock', 'raw_stocks') }}
)

SELECT
    dc.company_id,
    dt.datetime_id,
    fs.value
FROM fct_stock_cte fs
INNER JOIN {{ ref('dim_datetime') }} dt ON fs.datetime_id = dt.datetime_id
INNER JOIN {{ ref('dim_company') }} dc ON fs.company_id = dc.company_id

