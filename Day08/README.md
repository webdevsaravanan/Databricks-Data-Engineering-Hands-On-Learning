# Day 08 - Advanced Deduplication with PySpark Window Functions

## Project

RetailMart Data Engineering Platform

## Objective

Build a realistic deduplication process for source data containing multiple versions of the same business key.

Today we will practice:

- Business-key duplicates
- Window functions
- `partitionBy()`
- `orderBy()`
- `row_number()`
- Latest-record selection
- `ROW_NUMBER` vs `RANK` vs `DENSE_RANK`
- Deterministic tie-breaking
- Deduplication before Silver processing

## Scenario

RetailMart receives customer updates:

```text
CustomerId | CustomerName | City       | Age | UpdatedAt
---------------------------------------------------------
101        | Arun         | Chennai    | 30  | 09:00
102        | Kumar        | Bangalore  | 35  | 09:05
101        | Arun         | Bangalore  | 31  | 10:00
103        | Priya        | Chennai    | 27  | 10:10
101        | Arun         | Hyderabad  | 32  | 11:00
```

Customer `101` appears three times. These are different versions of the same customer.

We need to keep the latest version:

```text
101 | Arun | Hyderabad | 32 | 11:00
```

# 1. Create the Source Data

```python
customers = [
    (101, "Arun", "Chennai", 30, "2026-08-17 09:00:00"),
    (102, "Kumar", "Bangalore", 35, "2026-08-17 09:05:00"),
    (101, "Arun", "Bangalore", 31, "2026-08-17 10:00:00"),
    (103, "Priya", "Chennai", 27, "2026-08-17 10:10:00"),
    (101, "Arun", "Hyderabad", 32, "2026-08-17 11:00:00")
]

columns = ["CustomerId", "CustomerName", "City", "Age", "UpdatedAt"]

df = spark.createDataFrame(customers, columns)

display(df)
```

# 2. Store Data in Bronze

```python
df.write.format("delta").mode("overwrite").saveAsTable("bronze.day8_customer_updates")
```

Verify:

```sql
SELECT *
FROM bronze.day8_customer_updates
ORDER BY CustomerId, UpdatedAt;
```

# 3. Identify Duplicate Business Keys

```sql
SELECT
    CustomerId,
    COUNT(*) AS RecordCount
FROM bronze.day8_customer_updates
GROUP BY CustomerId
HAVING COUNT(*) > 1;
```

Expected:

```text
CustomerId | RecordCount
------------------------
101        | 3
```

# 4. Why `dropDuplicates()` Is Not Enough

```python
df.dropDuplicates(["CustomerId"])
```

does not express:

> Keep the latest record.

For requirements such as keeping the latest update, highest amount, earliest transaction, or most recent version, we need a controlled ordering rule.

# 5. Introduce Window Functions

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window_spec = (
    Window
    .partitionBy("CustomerId")
    .orderBy(col("UpdatedAt").desc())
)
```

Concept:

```text
partitionBy(CustomerId)
        ↓
Group records by CustomerId

orderBy(UpdatedAt DESC)
        ↓
Latest record comes first
```

# 6. Add Row Numbers

```python
ranked_df = df.withColumn(
    "row_num",
    row_number().over(window_spec)
)

display(ranked_df)
```

Expected conceptually:

```text
CustomerId | City       | UpdatedAt | row_num
----------------------------------------------
101        | Hyderabad  | 11:00     | 1
101        | Bangalore  | 10:00     | 2
101        | Chennai    | 09:00     | 3
102        | Bangalore  | 09:05     | 1
103        | Chennai    | 10:10     | 1
```

# 7. Keep Only the Latest Record

```python
latest_df = (
    ranked_df
    .filter(col("row_num") == 1)
    .drop("row_num")
)

display(latest_df)
```

Expected:

```text
101 | Arun  | Hyderabad | 32 | 11:00
102 | Kumar | Bangalore | 35 | 09:05
103 | Priya | Chennai   | 27 | 10:10
```

# 8. Write the Result to Silver

```python
latest_df.write.format("delta").mode("overwrite").saveAsTable("silver.day8_customers")
```

Verify:

```sql
SELECT *
FROM silver.day8_customers
ORDER BY CustomerId;
```

# 9. Understand `partitionBy()`

```python
Window.partitionBy("CustomerId")
```

means each CustomerId is treated independently.

# 10. Understand `orderBy()`

```python
.orderBy(col("UpdatedAt").desc())
```

means the latest record receives the lowest row number.

Descending:

```text
11:00 → 1
10:00 → 2
09:00 → 3
```

Ascending:

```python
.orderBy(col("UpdatedAt").asc())
```

would produce:

```text
09:00 → 1
10:00 → 2
11:00 → 3
```

# 11. `ROW_NUMBER` vs `RANK` vs `DENSE_RANK`

## ROW_NUMBER

```python
from pyspark.sql.functions import row_number

row_number().over(window_spec)
```

Every row receives a unique number:

```text
1
2
3
4
```

## RANK

```python
from pyspark.sql.functions import rank

rank().over(window_spec)
```

With a tie:

```text
100 → 1
100 → 1
90  → 3
```

## DENSE_RANK

```python
from pyspark.sql.functions import dense_rank

dense_rank().over(window_spec)
```

With a tie:

```text
100 → 1
100 → 1
90  → 2
```

# 12. Which One Should You Use?

For:

> Keep exactly one latest record per CustomerId.

Use:

```text
ROW_NUMBER()
```

because we need exactly one record.

# 13. Production Tie-Breaker

If two records have the same `UpdatedAt`, provide another ordering column.

Example:

```python
window_spec = (
    Window
    .partitionBy("CustomerId")
    .orderBy(
        col("UpdatedAt").desc(),
        col("BatchId").desc()
    )
)
```

This makes the selection deterministic when timestamps tie.

# 14. More Realistic Dataset

```python
customers = [
    (101, "Arun", "Chennai", 30, "2026-08-17 09:00:00", 1),
    (102, "Kumar", "Bangalore", 35, "2026-08-17 09:05:00", 1),
    (101, "Arun", "Bangalore", 31, "2026-08-17 10:00:00", 2),
    (103, "Priya", "Chennai", 27, "2026-08-17 10:10:00", 2),
    (101, "Arun", "Hyderabad", 32, "2026-08-17 11:00:00", 3)
]

columns = [
    "CustomerId",
    "CustomerName",
    "City",
    "Age",
    "UpdatedAt",
    "BatchId"
]

df = spark.createDataFrame(customers, columns)

display(df)
```

Define the Window:

```python
window_spec = (
    Window
    .partitionBy("CustomerId")
    .orderBy(
        col("UpdatedAt").desc(),
        col("BatchId").desc()
    )
)
```

# 15. Real Data Engineering Pattern

```text
                 SOURCE
                    │
                    ▼
                 BRONZE
                    │
                    ▼
             Business Key
             Deduplication
                    │
                    ▼
             Window Function
                    │
                    ▼
              ROW_NUMBER()
                    │
                    ▼
              row_num = 1
                    │
                    ▼
                 SILVER
```

# 16. Day 8 Hands-on Project - Orders

Input:

```text
OrderId | CustomerId | Amount | Status    | UpdatedAt
-------------------------------------------------------
1001    | 101        | 5000   | Created   | 09:00
1002    | 102        | 3000   | Created   | 09:05
1001    | 101        | 5000   | Shipped   | 10:00
1003    | 103        | 7000   | Created   | 10:10
1001    | 101        | 5000   | Delivered | 11:00
1002    | 102        | 3000   | Shipped   | 11:30
```

Goal:

For every `OrderId`, keep only the latest version.

Expected:

```text
1001 | 101 | 5000 | Delivered
1002 | 102 | 3000 | Shipped
1003 | 103 | 7000 | Created
```

## Hands-on Steps

### Step 1

Create:

```text
bronze.day8_order_updates
```

### Step 2

Identify duplicate OrderIds.

### Step 3

Create:

```python
Window.partitionBy("OrderId")
```

### Step 4

Order by:

```text
UpdatedAt DESC
```

### Step 5

Generate:

```text
row_number()
```

### Step 6

Keep:

```text
row_num = 1
```

### Step 7

Write the result to:

```text
silver.day8_orders
```

### Step 8

Verify:

```sql
SELECT *
FROM silver.day8_orders
ORDER BY OrderId;
```

# 17. Connect Day 8 With Day 6

Day 6:

```text
Incremental Data
      ↓
MERGE
      ↓
Silver
```

Day 8:

```text
Incremental Data
      ↓
Deduplicate
      ↓
Latest Record
      ↓
MERGE
      ↓
Silver
```

Combined pipeline:

```text
             BRONZE
                │
                ▼
       Incoming Incremental
             Records
                │
                ▼
        Data Quality Checks
                │
                ▼
          Deduplication
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
             SILVER
                │
                ▼
              GOLD
```

# Key Learnings

```text
Window
   ↓
partitionBy()
   ↓
orderBy()
   ↓
row_number()
   ↓
Latest record
```

Also understand the difference between:

```text
dropDuplicates()
```

and:

```text
Window + ROW_NUMBER()
```

`dropDuplicates()` removes duplicate records but does not express which version should survive.

Window functions let you define the business rule for selecting the correct record.

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
```

# Outcome

By completing Day 8, you should be able to:

- Identify duplicate business keys
- Understand why simple `dropDuplicates()` is sometimes insufficient
- Use PySpark Window functions
- Use `partitionBy()`
- Use `orderBy()`
- Generate row numbers
- Select the latest version of a record
- Understand `ROW_NUMBER`, `RANK`, and `DENSE_RANK`
- Create deterministic tie-breaking rules
- Prepare deduplicated data for Silver processing
- Combine deduplication with incremental processing

---

**Next:** Day 09 - Build a complete incremental pipeline: Bronze → Data Quality → Deduplication → MERGE → Silver.
