# Day 09 - Complete Incremental Pipeline

## Project

RetailMart Data Engineering Platform

## Objective

Combine Days 6, 7 and 8 into one realistic incremental ETL pipeline.

```text
Source
  ↓
Bronze
  ↓
Data Quality
  ↓
Deduplication
  ↓
Latest Record
  ↓
MERGE / Upsert
  ↓
Silver
```

## 1. Scenario - RetailMart Orders

RetailMart receives order updates. The same `OrderId` can appear multiple times because an order changes status:

```text
1001 → Created
1001 → Shipped
1001 → Delivered
```

We need to process the latest version of each order and update the Silver table.

## 2. Today's Source Data

```python
orders = [
    (1001, 101, 5000, "Created", "2026-08-18 09:00:00"),
    (1002, 102, 3000, "Created", "2026-08-18 09:05:00"),
    (1001, 101, 5000, "Shipped", "2026-08-18 10:00:00"),
    (1003, 103, 7000, "Created", "2026-08-18 10:10:00"),
    (1001, 101, 5000, "Delivered", "2026-08-18 11:00:00"),
    (1002, 102, 3000, "Shipped", "2026-08-18 11:30:00")
]

columns = ["OrderId", "CustomerId", "Amount", "Status", "UpdatedAt"]

df = spark.createDataFrame(orders, columns)

display(df)
```

## 3. Bronze

```python
df.write.format("delta").mode("overwrite").saveAsTable("bronze.day9_orders")
```

Check:

```sql
SELECT *
FROM bronze.day9_orders
ORDER BY OrderId, UpdatedAt;
```

Bronze keeps the source data as received.

## 4. Data Quality

Rules:

```text
OrderId cannot be NULL
CustomerId cannot be NULL
Amount must be greater than 0
Status cannot be NULL
UpdatedAt cannot be NULL
```

Invalid records:

```python
from pyspark.sql.functions import col

invalid_df = df.filter(
    col("OrderId").isNull()
    | col("CustomerId").isNull()
    | (col("Amount") <= 0)
    | col("Status").isNull()
    | col("UpdatedAt").isNull()
)

display(invalid_df)
```

Valid records:

```python
valid_df = df.filter(
    col("OrderId").isNotNull()
    & col("CustomerId").isNotNull()
    & (col("Amount") > 0)
    & col("Status").isNotNull()
    & col("UpdatedAt").isNotNull()
)

display(valid_df)
```

## 5. Deduplication

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = (
    Window
    .partitionBy("OrderId")
    .orderBy(col("UpdatedAt").desc())
)
```

Add row numbers:

```python
ranked_df = valid_df.withColumn(
    "row_num",
    row_number().over(window_spec)
)

display(ranked_df)
```

Conceptually:

```text
OrderId | Status    | UpdatedAt | row_num
------------------------------------------
1001    | Delivered | 11:00     | 1
1001    | Shipped   | 10:00     | 2
1001    | Created   | 09:00     | 3
1002    | Shipped   | 11:30     | 1
1002    | Created   | 09:05     | 2
1003    | Created   | 10:10     | 1
```

## 6. Keep Latest Version

```python
latest_orders = (
    ranked_df
    .filter(col("row_num") == 1)
    .drop("row_num")
)

display(latest_orders)
```

Expected:

```text
1001 | 101 | 5000 | Delivered
1002 | 102 | 3000 | Shipped
1003 | 103 | 7000 | Created
```

## 7. Create the Silver Table

```python
latest_orders.write.format("delta").mode("overwrite").saveAsTable("silver.day9_orders")
```

Check:

```sql
SELECT *
FROM silver.day9_orders
ORDER BY OrderId;
```

## 8. Why MERGE Is Needed

Tomorrow we may receive:

```text
1001 | Delivered
1002 | Delivered
1004 | Created
```

We do not want to overwrite the entire Silver table.

We want:

```text
Existing OrderId → Update
New OrderId      → Insert
```

This is an upsert.

## 9. Tomorrow's Incremental Data

```python
new_orders = [
    (1001, 101, 5000, "Delivered", "2026-08-19 09:00:00"),
    (1002, 102, 3000, "Delivered", "2026-08-19 09:05:00"),
    (1004, 104, 8000, "Created", "2026-08-19 09:10:00")
]

new_df = spark.createDataFrame(new_orders, columns)

display(new_df)
```

## 10. Create a Temporary View

```python
new_df.createOrReplaceTempView("new_orders")
```

## 11. MERGE Into Silver

```sql
MERGE INTO silver.day9_orders AS target
USING new_orders AS source
ON target.OrderId = source.OrderId
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Check:

```sql
SELECT *
FROM silver.day9_orders
ORDER BY OrderId;
```

Expected:

```text
1001 | 101 | 5000 | Delivered
1002 | 102 | 3000 | Delivered
1003 | 103 | 7000 | Created
1004 | 104 | 8000 | Created
```

Result:

```text
1001 → Updated
1002 → Updated
1003 → Existing
1004 → Inserted
```

## 12. Complete Pipeline

```text
             SOURCE
                │
                ▼
             BRONZE
                │
                ▼
        DATA QUALITY
                │
          ┌─────┴─────┐
          │           │
        VALID       INVALID
          │           │
          ▼           ▼
     DEDUPLICATE   QUARANTINE
          │
          ▼
    WINDOW FUNCTION
          │
          ▼
     LATEST RECORD
          │
          ▼
         MERGE
          │
          ▼
        SILVER
```

## 13. Why We Do Not MERGE Raw Data Directly

Consider Bronze:

```text
1001 Created
1001 Shipped
1001 Delivered
```

Instead of processing multiple versions directly:

```text
Bronze
  ↓
Validate
  ↓
Deduplicate
  ↓
Latest record
  ↓
MERGE
  ↓
Silver
```

This is a safer and clearer production pattern.

## 14. Real-World Architecture

```text
                   SOURCE
                     │
                     ▼
               ┌───────────┐
               │  BRONZE   │
               │ Raw Delta │
               └─────┬─────┘
                     │
                     ▼
             Data Quality
                     │
              ┌──────┴──────┐
              │             │
            Valid         Invalid
              │             │
              ▼             ▼
        Deduplication   Quarantine
              │
              ▼
       Window Function
              │
              ▼
       Latest Record
              │
              ▼
            MERGE
              │
              ▼
        ┌───────────┐
        │  SILVER   │
        │ Clean Data│
        └─────┬─────┘
              │
              ▼
             GOLD
```

## 15. Day 9 Hands-on Project - Products

Input:

```text
ProductId | ProductName | Price | Stock | UpdatedAt
----------------------------------------------------
101       | Laptop      | 60000 | 10    | 09:00
102       | Mobile      | 30000 | 20    | 09:05
101       | Laptop      | 60000 | 8     | 10:00
103       | Keyboard    | 2000  | 50    | 10:05
102       | Mobile      | 30000 | 15    | 11:00
```

Final Silver result:

```text
101 | Laptop   | 60000 | 8
102 | Mobile   | 30000 | 15
103 | Keyboard | 2000  | 50
```

Build:

```text
bronze.day9_products
      ↓
Data Quality
      ↓
Window.partitionBy("ProductId")
      ↓
row_number()
      ↓
Latest Record
      ↓
silver.day9_products
```

Then create another batch and use `MERGE` to update existing products and insert new products.

## 16. Day 9 Pipeline Checklist

- [ ] Create the Orders DataFrame
- [ ] Write raw data to Bronze
- [ ] Check data-quality rules
- [ ] Separate valid and invalid records
- [ ] Identify duplicate OrderIds
- [ ] Create a Window specification
- [ ] Use `row_number()`
- [ ] Select the latest order version
- [ ] Create the Silver table
- [ ] Create a second incremental batch
- [ ] Use a temporary view
- [ ] Perform Delta `MERGE`
- [ ] Verify updated records
- [ ] Verify inserted records
- [ ] Repeat the pipeline with Products

# Key Learnings

Do not think of ETL as individual commands. Think of it as a pipeline.

You have now connected:

```text
Day 5 → Delta Lake
Day 6 → MERGE / Incremental Processing
Day 7 → Data Quality
Day 8 → Window Deduplication
Day 9 → Complete Incremental Pipeline
```

Production-style pattern:

```text
Raw Data
   ↓
Bronze
   ↓
Validate
   ↓
Quarantine Invalid
   ↓
Deduplicate
   ↓
Select Latest Version
   ↓
MERGE
   ↓
Silver
   ↓
Gold
```

# Medallion Architecture Progress

```text
Day 1
Databricks Fundamentals
        ↓
Day 2
File → Bronze
        ↓
Day 3
Bronze → Silver
        ↓
Day 4
Silver → Join → Gold
        ↓
Day 5
Delta Lake
ACID
Time Travel
History
Restore
        ↓
Day 6
Incremental Processing
MERGE
Upsert
        ↓
Day 7
Data Quality
NULL
Duplicates
Validation
Quarantine
        ↓
Day 8
Window Functions
ROW_NUMBER
Deduplication
Latest Record
        ↓
Day 9
Complete Incremental Pipeline
Data Quality
Deduplication
MERGE
Silver
```

# Outcome

By completing Day 9, you should be able to:

- Build an end-to-end incremental ETL pipeline
- Preserve raw data in Bronze
- Validate incoming data
- Quarantine invalid records
- Deduplicate business keys
- Select the latest record using Window functions
- Use Delta Lake `MERGE`
- Perform update and insert operations
- Understand upsert processing
- Combine multiple Data Engineering concepts into one pipeline
- Build a realistic Bronze → Silver workflow

---

**Next:** [Day 10](../Day10/README.md) - Incremental File Processing with Auto Loader and handling new files arriving continuously in cloud storage.
