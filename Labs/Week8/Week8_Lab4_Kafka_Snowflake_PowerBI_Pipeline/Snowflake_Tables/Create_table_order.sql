
CREATE OR REPLACE TABLE orders (
    order_id INT,
    customer_name STRING,
    city STRING,
    product STRING,
    amount FLOAT,
    payment_mode STRING,
    order_time TIMESTAMP
);


SELECT * FROM orders