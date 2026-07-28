SELECT
    customer,
    SUM(amount) AS total_sales
FROM "dev"."main"."sales_data"
GROUP BY customer