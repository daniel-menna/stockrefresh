-- Crie uma CTE para extrair componentes de data e hora como strings
WITH datetime_cte AS (
  SELECT DISTINCT
    CAST(Date AS STRING) AS datetime_id,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', Date) AS date_part
  FROM `stockprice-433416`.`stock`.`raw_stocks`
  WHERE Date IS NOT NULL
)

SELECT
  datetime_id,
  CAST(date_part AS DATETIME) AS datetime,
  SUBSTR(date_part, 9, 2) AS day,
  SUBSTR(date_part, 6, 2) AS month,
  SUBSTR(date_part, 1, 4) AS year,
  SUBSTR(date_part, 12, 2) AS hour,
  SUBSTR(date_part, 15, 2) AS minute,
  EXTRACT(DAYOFWEEK FROM TIMESTAMP(date_part)) AS weekday
FROM datetime_cte