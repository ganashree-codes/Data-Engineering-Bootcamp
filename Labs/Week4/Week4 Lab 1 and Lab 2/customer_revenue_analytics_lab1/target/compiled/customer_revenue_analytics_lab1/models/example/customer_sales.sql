SELECT
     customer,
     COUNT(order_id) AS total_orders,
     SUM(amount) AS total_sales,
     AVG(amount) AS average_order_value,
     SUM(amount) AS revenue
FROM "dev"."main"."sales_data"
GROUP BY customer
ORDER BY revenue DESC