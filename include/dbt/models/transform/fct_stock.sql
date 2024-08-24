-- Crie a tabela fato juntando as chaves relevantes da tabela de dimensão
WITH fct_stock_cte AS (
    SELECT
        simbolo AS company_id,
        CAST(Date AS STRING) AS datetime_id,
        Close as value
    FROM {{ source('retail', 'raw_stocks') }}
)

SELECT
    dc.company_id,
    dt.datetime_id,
    fs.value
FROM fct_stock_cte fs
INNER JOIN {{ ref('dim_datetime') }} dt ON fs.datetime_id = dt.datetime_id
INNER JOIN {{ ref('dim_customer') }} dc ON fs.company_id = dc.company_id

{% if is_incremental() %}
  -- Filtrar apenas linhas novas ou modificadas
  WHERE (dc.company_id, fs.value) NOT IN (
    SELECT company_id, value FROM {{ this }}
  )
{% endif %}