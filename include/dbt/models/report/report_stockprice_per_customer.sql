with stockprice as(
    SELECT 
    datetime_id as Date,
    MAX(CASE WHEN company_id = 'AAPL' THEN price ELSE NULL END) AS APPLE,
    MAX(CASE WHEN company_id = 'GOOGL' THEN price ELSE NULL END) AS GOOGLE,
    MAX(CASE WHEN company_id = 'AMZN' THEN price ELSE NULL END) AS AMAZON,
    MAX(CASE WHEN company_id = 'META' THEN price ELSE NULL END) AS META,
    MAX(CASE WHEN company_id = 'MSFT' THEN price ELSE NULL END) AS MICROSOFT,
    MAX(CASE WHEN company_id = 'NVDA' THEN price ELSE NULL END) AS NVIDIA,
    MAX(CASE WHEN company_id = 'DELL' THEN price ELSE NULL END) AS DELL,
    MAX(CASE WHEN company_id = 'SAP' THEN price ELSE NULL END) AS SAP,
    MAX(CASE WHEN company_id = 'CRM' THEN price ELSE NULL END) AS SALESFORCE,
    MAX(CASE WHEN company_id = 'ORCL' THEN price ELSE NULL END) AS ORACLE,
    MAX(CASE WHEN company_id = 'IBM' THEN price ELSE NULL END) AS IBM
FROM 
    {{ ref('fct_stock') }}
GROUP BY 
    datetime_id
)

SELECT
    *
FROM stockprice