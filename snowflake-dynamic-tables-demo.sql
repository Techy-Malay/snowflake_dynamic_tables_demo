/**********************************************************
 Snowflake Dynamic Tables Demo
 Beginner → Intermediate Data Engineering
 Author: Malaya Kumar Padhi
**********************************************************/

/* ============================
   1️⃣ RAW TABLE (Ingestion Layer)
   Stores raw e-commerce orders
============================ */
CREATE TABLE orders_raw (
    order_id NUMBER,
    order_ts TIMESTAMP_NTZ,
    customer_id NUMBER,
    amount NUMBER(10,2),
    status STRING
);

/* ============================
   2️⃣ CLEAN TABLE (Dynamic Table)
   Filters only completed orders
   Incremental refresh handled automatically
============================ */
CREATE OR REPLACE DYNAMIC TABLE orders_clean
TARGET_LAG = '1 minute'
WAREHOUSE = compute_wh
AS
SELECT
    order_id,
    order_ts,
    customer_id,
    amount
FROM orders_raw
WHERE status = 'COMPLETED';

/* ============================
   3️⃣ ANALYTICS TABLE (Dynamic Table)
   Aggregates daily sales metrics
============================ */
CREATE OR REPLACE DYNAMIC TABLE daily_sales
TARGET_LAG = '5 minutes'
WAREHOUSE = compute_wh
AS
SELECT
    DATE(order_ts) AS order_date,
    COUNT(*) AS total_orders,
    SUM(amount) AS total_sales
FROM orders_clean
GROUP BY DATE(order_ts);

/* ============================
   4️⃣ MONITORING
   Check refresh status and dependencies
============================ */
SHOW DYNAMIC TABLES;
DESCRIBE DYNAMIC TABLE orders_clean;
