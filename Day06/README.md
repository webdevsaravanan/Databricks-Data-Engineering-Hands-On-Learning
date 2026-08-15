# Day 06 - Delta Lake MERGE / Upsert & Incremental Processing

## Project

RetailMart Data Engineering Platform

## Objective

Build an incremental data pipeline using Delta Lake `MERGE`.

Today we will practice:

- Incremental data
- Full load vs incremental load
- Upsert
- Delta `MERGE`
- `WHEN MATCHED`
- `WHEN NOT MATCHED`
- Updating existing records
- Inserting new records
- Delta transaction history

## Scenario

RetailMart receives a daily customer update file containing existing customers whose details changed and new customers.

```text
CustomerId | CustomerName | City       | Age
------------------------------------------------
2           | John         | Bangalore  | 29
4           | David        | Chennai    | 40
5           | Priya        | Chennai    | 27
```

The target Silver table already contains Customers 2 and 4.

Therefore:

```text
Customer already exists
        ↓
      UPDATE

Customer doesn't exist
        ↓
       INSERT
```

This operation is called an **upsert**.

# 1. Check the Existing Customer Table

```sql
SELECT *
FROM silver.day5_customers
ORDER BY CustomerId;
```

# 2. Create Incoming Customer Data

```python
incoming_customers = [
    (2, "John", "Bangalore", 29),
    (4, "David", "Chennai", 40),
    (5, "Priya", "Chennai", 27)
]

columns = [
    "CustomerId",
    "CustomerName",
    "City",
    "Age"
]

incoming_df = spark.createDataFrame(
    incoming_customers,
    columns
)

display(incoming_df)
```

# 3. Store Incoming Data as Bronze

```python
incoming_df.write.format("delta").mode("overwrite").saveAsTable("bronze.day6_customer_updates")
```

Verify:

```sql
SELECT *
FROM bronze.day6_customer_updates;
```

Architecture:

```text
Daily Source
     │
     ▼
  Bronze
     │
     ▼
  MERGE
     │
     ▼
  Silver
```

# 4. Understand the MERGE Operation

Target:

```text
silver.day5_customers
```

Source:

```text
bronze.day6_customer_updates
```

Matching key:

```text
CustomerId
```

Conceptually:

```text
TARGET + SOURCE
       ↓
     MERGE
       ↓
Updated TARGET
```

# 5. Perform the MERGE

```sql
MERGE INTO silver.day5_customers AS target
USING bronze.day6_customer_updates AS source
ON target.CustomerId = source.CustomerId

WHEN MATCHED THEN
  UPDATE SET
    target.CustomerName = source.CustomerName,
    target.City = source.City,
    target.Age = source.Age

WHEN NOT MATCHED THEN
  INSERT (
    CustomerId,
    CustomerName,
    City,
    Age
  )
  VALUES (
    source.CustomerId,
    source.CustomerName,
    source.City,
    source.Age
  );
```

# 6. Verify the Result

```sql
SELECT *
FROM silver.day5_customers
ORDER BY CustomerId;
```

Expected result:

| CustomerId | CustomerName | City | Age |
|---:|---|---|---:|
| 1 | Saravana | Chennai | 30 |
| 2 | John | Bangalore | 29 |
| 3 | Alice | Hyderabad | 25 |
| 4 | David | Chennai | 40 |
| 5 | Priya | Chennai | 27 |

# 7. Check Delta History

```sql
DESCRIBE HISTORY silver.day5_customers;
```

The MERGE should appear as a new Delta table operation.

# 8. Understand WHEN MATCHED

```sql
WHEN MATCHED THEN
  UPDATE SET
    target.CustomerName = source.CustomerName,
    target.City = source.City,
    target.Age = source.Age
```

If the source record already exists in the target based on the `ON` condition, update the target record.

Example:

```text
TARGET
2 | John | Chennai | 28

SOURCE
2 | John | Bangalore | 29

        ↓ MERGE

TARGET
2 | John | Bangalore | 29
```

# 9. Understand WHEN NOT MATCHED

```sql
WHEN NOT MATCHED THEN
  INSERT (...)
```

If the source record does not exist in the target, insert it.

Example:

```text
TARGET
1, 2, 3, 4

SOURCE
2, 4, 5

Customer 5
    ↓
INSERT
```

# 10. Full Load vs Incremental Load

## Full Load

The complete dataset is processed every time.

```text
Day 1 → 1,000,000 records
Day 2 → 1,000,000 records
Day 3 → 1,000,000 records
```

## Incremental Load

Only new or changed records are processed.

```text
Day 1 → 1,000,000 records
Day 2 → 5,000 changed/new records
Day 3 → 3,000 changed/new records
```

Incremental processing can reduce the amount of data that needs to be processed.

# 11. Add a Batch Date

In production pipelines, it is often useful to record when a batch arrived.

```python
incoming_customers_2 = [
    (6, "Kumar", "Chennai", 35, "2026-08-15"),
    (2, "John", "Chennai", 30, "2026-08-15")
]

columns_2 = [
    "CustomerId",
    "CustomerName",
    "City",
    "Age",
    "BatchDate"
]

incoming_df_2 = spark.createDataFrame(
    incoming_customers_2,
    columns_2
)

display(incoming_df_2)
```

This introduces the concept of:

```text
Batch / Load Date
```

# 12. General MERGE Pattern

```sql
MERGE INTO target
USING source
ON matching_condition

WHEN MATCHED THEN
    UPDATE

WHEN NOT MATCHED THEN
    INSERT
```

The important parts are:

```text
Target
   ↓
Table being changed

Source
   ↓
Incoming data

ON
   ↓
How source and target records are matched

WHEN MATCHED
   ↓
Existing record → UPDATE

WHEN NOT MATCHED
   ↓
New record → INSERT
```

# 13. Product MERGE Exercise

Create another incremental pipeline:

```text
bronze.day6_product_updates
              ↓
            MERGE
              ↓
silver.products
```

Use this initial target data:

```text
ProductId | ProductName | Price
--------------------------------
101       | Laptop      | 60000
102       | Mobile      | 30000
103       | Keyboard    | 2000
```

Incoming data:

```text
101 | Laptop   | 58000
102 | Mobile   | 28000
106 | Mouse    | 1000
107 | Webcam   | 5000
```

The first two records should update existing products.

The last two records should be inserted.

Expected final Silver table:

```text
101 | Laptop      | 58000
102 | Mobile      | 28000
103 | Keyboard    | 2000
106 | Mouse       | 1000
107 | Webcam      | 5000
```

Use `MERGE` instead of separate UPDATE and INSERT operations.

# 14. Final Day 6 Architecture

```text
                    SOURCE
                      │
                      ▼
                Customer Files
                      │
                      ▼
                  🥉 BRONZE
                      │
                      ▼
                Incremental Data
                      │
                      ▼
                    MERGE
                  /       \
                 /         \
            MATCHED     NOT MATCHED
               │             │
             UPDATE        INSERT
               │             │
                \           /
                 \         /
                    ▼
                 🥈 SILVER
                    │
                    ▼
                  🥇 GOLD
```

# Day 6 Project Flow

```text
Daily Source File
       │
       ▼
Bronze Delta Table
       │
       ▼
Read Incremental Data
       │
       ▼
MERGE
  │
  ├── Existing → UPDATE
  │
  └── New      → INSERT
       │
       ▼
Silver Delta Table
       │
       ▼
Gold
```

# Key Learnings

## Upsert

```text
UPDATE existing records
+
INSERT new records
```

## MERGE

Delta `MERGE` combines source data into a target table based on a matching condition.

## WHEN MATCHED

```text
Existing record → UPDATE
```

## WHEN NOT MATCHED

```text
New record → INSERT
```

## Incremental Processing

Process only new or changed data instead of repeatedly processing the entire dataset.

## Delta Transaction

A successful `MERGE` creates a new Delta table version.

Check it with:

```sql
DESCRIBE HISTORY silver.day5_customers;
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
```

# Outcome

By completing Day 6, you have practiced a fundamental production ETL pattern:

1. Receive incremental source data.
2. Store the incoming data in Bronze.
3. Match source records against the Silver target.
4. Update existing records.
5. Insert new records.
6. Commit the operation through Delta Lake.
7. Verify the new table state and transaction history.

---

**Next:** Day 07 - Data Quality, Constraints, NULL Handling, Duplicate Detection and Cleaning.
