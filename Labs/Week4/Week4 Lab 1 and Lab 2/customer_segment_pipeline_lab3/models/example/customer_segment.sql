SELECT
    customer,
    total_sales,
    CASE
        WHEN total_sales > 100000 THEN 'High Value Customer'
        WHEN total_sales BETWEEN 50000 AND 100000 THEN 'Medium value Customer'
        ELSE 'Low Value Customer'
    END AS customer_segment
FROM {{ ref('customer_sales') }}
