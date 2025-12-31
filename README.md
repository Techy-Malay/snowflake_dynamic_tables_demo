# Snowflake Dynamic Tables — End-to-End Data Engineering Project

## 📌 Overview
This project demonstrates how to build a **declarative, incremental data pipeline** in **Snowflake** using **Dynamic Tables**, without using Streams or Tasks.

The goal is to show how Snowflake Dynamic Tables simplify:
- Incremental data processing
- Dependency management
- SLA-based data freshness
- Medallion-style architecture (Raw → Clean → Analytics)

This is a **beginner-to-intermediate** project designed to reflect **real-world data engineering patterns**.

---

## 🎯 Business Use Case
An e-commerce platform receives order data continuously.

Business requirements:
- Filter only **completed orders**
- Generate **daily sales metrics**
- Ensure data freshness without manual scheduling
- Minimize operational complexity

---

## 🧠 Why Dynamic Tables?
Traditional Snowflake pipelines often require:
- Streams
- Tasks
- Scheduling logic
- Manual dependency handling

**Dynamic Tables replace this with a declarative approach**:
- You define *what* data you want
- You define *how fresh* it should be
- Snowflake manages *how* and *when* to refresh it

---

## 🏗️ Architecture Overview
RAW ORDERS
(orders_raw)
     |
     |  TARGET_LAG = 1 minute
     v
CLEAN ORDERS (Dynamic Table)
(orders_clean)
     |
     |  TARGET_LAG = 5 minutes
     v
DAILY SALES (Dynamic Table)
(daily_sales)
     |
     v
BI / ANALYTICS


---

## 🧱 Data Layers (Medallion Architecture)

| Layer     |          Object             |         Purpose              |
|-----------|-----------------------------|------------------------------|
| Raw       | orders_raw                  | Ingested, unvalidated data   |
| Clean     | orders_clean (Dynamic Table)| Filtered & standardized data |
| Analytics | daily_sales (Dynamic Table) | Business-ready metrics       |

---

## 🛠️ SQL Implementation

### 1️⃣ Raw Table (Ingestion Layer)

```sql
CREATE TABLE orders_raw (
    order_id NUMBER,
    order_ts TIMESTAMP_NTZ,
    customer_id NUMBER,
    amount NUMBER(10,2),
    status STRING
);
```
### 2️⃣ Raw Table (Ingestion Layer)
```
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
```
3️⃣ Analytics Layer — Dynamic Table on Dynamic Table
```
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
```
🔍 Observability & Monitoring
```
SHOW DYNAMIC TABLES;
DESCRIBE DYNAMIC TABLE orders_clean;
```

📈 Key Takeaways
- Dynamic Tables simplify pipeline orchestration
- Incremental processing is handled automatically
- Cleaner architecture with fewer moving parts
- Strong fit for modern analytics engineering

## 👤 Author
**Malaya Kumar Padhi**  
Snowflake | Data Engineering | Analytics Architecture

