/*==========================================================
 Project: Snowflake Dynamic Tables – End-to-End Demo
 Author : Malaya Kumar Padhi
 LinkedIn: https://www.linkedin.com/in/mkpadhi/
 Purpose:
   Demonstrate how to build a declarative, incremental
   data pipeline in Snowflake using Dynamic Tables.

 Key Concepts Covered:
   - Raw → Clean → Analytics layers
   - SLA-based freshness (TARGET_LAG)
   - Dependency-aware refresh
==========================================================*/


/*==========================================================
 STEP 1: DATABASE & SCHEMA SETUP
------------------------------------------------------------
 We separate schemas to represent logical layers.
 This mirrors real-world medallion architecture.
==========================================================*/

CREATE OR REPLACE DATABASE dt_demo;

CREATE OR REPLACE SCHEMA dt_demo.raw;
CREATE OR REPLACE SCHEMA dt_demo.analytics;


/*==========================================================
 STEP 2: RAW TABLE (INGESTION LAYER)
------------------------------------------------------------
 This table simulates raw data coming from an
 application or API.
 - No validation
 - No filtering
 - Data may be messy or incomplete
==========================================================*/

CREATE OR REPLACE TABLE dt_demo.raw.orders_raw (
    order_id     NUMBER,
    order_ts     TIMESTAMP_NTZ,
    customer_id  NUMBER,
    amount       NUMBER(10,2),
    status       STRING
);


/*==========================================================
 STEP 3: SAMPLE DATA LOAD
------------------------------------------------------------
 Includes both COMPLETED and CANCELLED orders.
 This helps demonstrate filtering in the clean layer.
==========================================================*/

INSERT INTO dt_demo.raw.orders_raw VALUES
(1, '2024-01-01 10:00:00', 101, 500.00, 'COMPLETED'),
(2, '2024-01-01 10:05:00', 102, 1200.00, 'COMPLETED'),
(3, '2024-01-01 10:10:00', 101, 700.00, 'CANCELLED');


/*==========================================================
 STEP 4: CLEAN LAYER – DYNAMIC TABLE
------------------------------------------------------------
 Why Dynamic Table here?
 - Data arrives continuously
 - We want only COMPLETED orders
 - We want incremental refresh without Streams/Tasks

 TARGET_LAG = '1 minute'
 - Indicates acceptable freshness delay
 - Snowflake decides when and how to refresh
==========================================================*/

CREATE OR REPLACE DYNAMIC TABLE dt_demo.analytics.orders_clean
TARGET_LAG = '1 minute'
WAREHOUSE = compute_wh
AS
SELECT
    order_id,
    order_ts,
    customer_id,
    amount
FROM dt_demo.raw.orders_raw
WHERE status = 'COMPLETED';


/*==========================================================
 STEP 5: REFRESH VALIDATION
------------------------------------------------------------
 Insert new raw data and observe automatic refresh.
 No manual job execution required.
==========================================================*/

INSERT INTO dt_demo.raw.orders_raw VALUES
(4, CURRENT_TIMESTAMP, 103, 900.00, 'COMPLETED');

/*
 After ~1 minute:
 SELECT * FROM dt_demo.analytics.orders_clean;
 The new record should appear automatically.
*/


/*==========================================================
 STEP 6: ANALYTICS LAYER – AGGREGATED DYNAMIC TABLE
------------------------------------------------------------
 This Dynamic Table depends on another Dynamic Table.

 Snowflake automatically:
 - Tracks dependencies
 - Refreshes only impacted data
 - Preserves incremental behavior
==========================================================*/

CREATE OR REPLACE DYNAMIC TABLE dt_demo.analytics.daily_sales
TARGET_LAG = '5 minutes'
WAREHOUSE = compute_wh
AS
SELECT
    DATE(order_ts)      AS order_date,
    COUNT(order_id)     AS total_orders,
    SUM(amount)         AS total_sales
FROM dt_demo.analytics.orders_clean
GROUP BY DATE(order_ts);


/*==========================================================
 STEP 7: ANALYTICS QUERY
------------------------------------------------------------
 This table is BI / dashboard ready.
==========================================================*/

SELECT *
FROM dt_demo.analytics.daily_sales
ORDER BY order_date;


/*==========================================================
 STEP 8: OBSERVABILITY & METADATA
------------------------------------------------------------
 Useful for monitoring refresh behavior and debugging.
==========================================================*/

SHOW DYNAMIC TABLES;

DESCRIBE DYNAMIC TABLE dt_demo.analytics.orders_clean;


/*==========================================================
 KEY TAKEAWAY
------------------------------------------------------------
 Dynamic Tables enable declarative data pipelines:
 - No Streams
 - No Tasks
 - No manual scheduling
 - Incremental by default
==========================================================*/
