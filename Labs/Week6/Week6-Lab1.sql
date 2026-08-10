
--Lab 4 Transforming and modelling data

CREATE OR REPLACE VIEW analytics.v_all_line_items AS
SELECT
  order_id, customer_name, product, quantity AS qty,
  unit_price, order_date, region, 'csv' AS source
FROM raw.orders
 
UNION ALL
 
SELECT
  data:order_id::INT, data:customer.name::STRING,
  f.value:product::STRING, f.value:qty::INT,
  f.value:price::NUMBER(10,2), CURRENT_DATE(),
  data:customer.city::STRING, 'json' AS source
FROM raw.orders_json, LATERAL FLATTEN(input => data:items) f;


SELECT * FROM analytics.v_all_line_items;


SELECT
  region,
  SUM(qty * unit_price) AS revenue,
  COUNT(DISTINCT order_id) AS orders
FROM analytics.v_all_line_items
GROUP BY region
ORDER BY revenue DESC;


--Lab 5 Time travel and zero copy cloning 
--Time travel
-- note the row count before
SELECT COUNT(*) FROM raw.orders;
 
-- accidentally wipe out a region's data
DELETE FROM raw.orders WHERE region = 'India';

 
SELECT COUNT(*) FROM raw.orders;



-- look at the table as it was 5 minutes ago
SELECT * FROM raw.orders AT(OFFSET => -60*5);
 
-- restore it properly
CREATE OR REPLACE TABLE raw.orders AS
SELECT * FROM raw.orders BEFORE(STATEMENT => '01c64c31-0207-d10f-0005-49560012618a');
 
-- alternative: UNDROP if you'd dropped the whole table instea01c64bae-0207-d114-0005-49560011de16d
-- UNDROP TABLE raw.orders;

--Zero copy clone

CREATE OR REPLACE TABLE raw.orders_backup CLONE raw.orders;
 
-- prove it's independent: changes to the clone don't affect the original
DELETE FROM raw.orders_backup WHERE region = 'India';
SELECT COUNT(*) FROM raw.orders;         -- unaffected
SELECT COUNT(*) FROM raw.orders_backup;  -- reduced





--Lab 6 Access Control (RBAC)


CREATE OR REPLACE ROLE analyst_ga;
 
GRANT USAGE ON WAREHOUSE lab_wh_gmd TO ROLE analyst_ga;
GRANT USAGE ON DATABASE retail_db_gmd TO ROLE analyst_ga;
GRANT USAGE ON SCHEMA retail_db_gmd.analytics TO ROLE analyst_ga;
GRANT SELECT ON ALL VIEWS IN SCHEMA retail_db_gmd.analytics
  TO ROLE analyst_ga;
 
-- make sure FUTURE views are covered too, not just existing ones
GRANT SELECT ON FUTURE VIEWS IN SCHEMA retail_db_gmd.analytics
  TO ROLE analyst_ga;


GRANT ROLE analyst_ga TO USER GANASHREEMD;
GRANT ROLE analyst_ga TO USER GANASHREEMD;
-- switch role in the Snowsight worksheet role-picker (top right), then:
USE ROLE analyst_ga;
SELECT * FROM retail_db_gmd.analytics.v_all_line_items LIMIT 5;  -- works

USE SECONDARY ROLES NONE; -- it was able to access raw.orders as secondary rols were enabled
SELECT * FROM retail_db_gmd.raw.orders;                          -- should fail

-- Lab 7 Performance and query profile

SELECT
  o.o_orderpriority,
  COUNT(*)               AS order_count,
  SUM(o.o_totalprice)    AS total_value
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.ORDERS o
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.CUSTOMER c
  ON o.o_custkey = c.c_custkey
GROUP BY o.o_orderpriority
ORDER BY total_value DESC;


ALTER WAREHOUSE lab_wh_gmd SET WAREHOUSE_SIZE = 'SMALL';
-- re-run the exact same query and compare its duration in Query History
ALTER WAREHOUSE lab_wh_gmd SET WAREHOUSE_SIZE = 'XSMALL';  -- scale back down


--Lab 8 Streams and tasks (Change data capture)

CREATE OR REPLACE STREAM raw.orders_stream ON TABLE raw.orders;
 
-- the stream starts empty relative to now
SELECT * FROM raw.orders_stream;

INSERT INTO raw.orders VALUES
  (2001, 'Meera Nair', 'Webcam', 'Electronics', 1, 2499.00, CURRENT_DATE(), 'West');
 
SELECT * FROM raw.orders_stream;   -- now shows the new row with METADATA$ACTION = 'INSERT'

CREATE OR REPLACE TABLE analytics.new_orders_log (
  order_id INT, customer_name STRING, product STRING,
  logged_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
 
CREATE OR REPLACE TASK raw.process_new_orders
  WAREHOUSE = lab_wh_gmd
  SCHEDULE = '1 MINUTE'
WHEN
  SYSTEM$STREAM_HAS_DATA('raw.orders_stream')
AS
  INSERT INTO analytics.new_orders_log (order_id, customer_name, product)
  SELECT order_id, customer_name, product
  FROM raw.orders_stream
  WHERE METADATA$ACTION = 'INSERT';
 
ALTER TASK raw.process_new_orders RESUME;

SELECT * FROM analytics.new_orders_log;
SELECT * FROM raw.orders_stream;  -- empty again
 
-- IMPORTANT: suspend the task when you're done so it doesn't run forever
ALTER TASK raw.process_new_orders SUSPEND;


--Lab 9 Connected Python locally and executed the TASK

--- clean up

ALTER TASK raw.process_new_orders SUSPEND;
DROP DATABASE retail_db_<YOURNAME>;
DROP WAREHOUSE lab_wh_<YOURNAME>;
DROP ROLE analyst_<YOURNAME>;
