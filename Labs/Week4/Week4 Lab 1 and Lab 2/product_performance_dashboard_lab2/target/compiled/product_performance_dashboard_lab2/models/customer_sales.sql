SELECT
    product,
    SUM(amount) AS product_revenue,
    COUNT(order_id) AS total_orders
FROM "dev"."main"."sales_data"
GROUP BY product
ORDER BY product_revenue DESC