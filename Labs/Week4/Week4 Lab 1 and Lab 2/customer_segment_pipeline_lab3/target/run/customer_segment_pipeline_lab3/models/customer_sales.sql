
  
  create view "dev"."main"."customer_sales__dbt_tmp" as (
    SELECT
    customer,
    SUM(amount) AS total_sales
FROM "dev"."main"."sales_data"
GROUP BY customer
  );
