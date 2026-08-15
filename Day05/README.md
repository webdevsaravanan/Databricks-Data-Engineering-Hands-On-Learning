# Day 05 - Delta Lake Fundamentals & ACID Transactions

## Project
RetailMart Data Engineering Platform

## Objective
Understand and practice Delta Lake fundamentals using a realistic customer-data scenario.

Today we will practice:
- Creating a Delta table
- Append operations
- UPDATE
- DELETE
- Delta transaction history
- Time Travel
- RESTORE
- Schema enforcement
- Understanding the Delta transaction log

## Scenario

RetailMart's customer data changes over time. The business needs to add new customers, update existing customers, delete customers when required, review previous versions, recover from accidental changes, and protect tables from incompatible schemas.

## Delta Lake Mental Model

```text
             Delta Table
                  │
       ┌──────────┴──────────┐
       │                     │
   Data Files          Transaction Log
   (Parquet)             (_delta_log)
       │                     │
       └──────────┬──────────┘
                  │
             Delta Lake
                  │
       ┌──────────┼──────────┐
       │          │          │
      ACID    Time Travel  History
```

## Concepts Learned

- Delta Lake
- Delta tables
- Parquet data files
- Transaction log
- ACID transactions
- INSERT / APPEND
- UPDATE
- DELETE
- `DESCRIBE HISTORY`
- `DESCRIBE DETAIL`
- Time Travel
- `VERSION AS OF`
- `RESTORE TABLE`
- Schema enforcement

# 1. Create Practice Customer Data

```python
customers = [
    (1, "Saravana", "Chennai", 30),
    (2, "John", "Bangalore", 28),
    (3, "Alice", "Hyderabad", 25)
]

columns = [
    "CustomerId",
    "CustomerName",
    "City",
    "Age"
]

df = spark.createDataFrame(customers, columns)

display(df)
```

# 2. Write as a Delta Table

```python
df.write.format("delta").mode("overwrite").saveAsTable("silver.day5_customers")
```

Verify:

```sql
SELECT *
FROM silver.day5_customers;
```

Check the table format:

```sql
DESCRIBE DETAIL silver.day5_customers;
```

Look for:

```text
format = delta
```

# 3. Check Delta History

```sql
DESCRIBE HISTORY silver.day5_customers;
```

Conceptually:

```text
Version 0
    ↓
Initial customer data
```

Use the actual version numbers shown in your environment for later exercises.

# 4. Add a New Customer

```python
new_customer = [
    (4, "David", "Coimbatore", 40)
]

new_df = spark.createDataFrame(
    new_customer,
    columns
)

new_df.write.format("delta").mode("append").saveAsTable("silver.day5_customers")
```

Verify:

```sql
SELECT *
FROM silver.day5_customers
ORDER BY CustomerId;
```

# 5. Check History After Append

```sql
DESCRIBE HISTORY silver.day5_customers;
```

Conceptually:

```text
Version 0 → Initial data
Version 1 → David added
```

# 6. Update an Existing Customer

Business requirement: John moved from Bangalore to Chennai.

```sql
UPDATE silver.day5_customers
SET City = 'Chennai'
WHERE CustomerId = 2;
```

Verify:

```sql
SELECT *
FROM silver.day5_customers
WHERE CustomerId = 2;
```

# 7. Check History After Update

```sql
DESCRIBE HISTORY silver.day5_customers;
```

Conceptually:

```text
Version 0 → Initial data
Version 1 → David inserted
Version 2 → John updated
```

Use the actual versions from your environment.

# 8. Delete a Customer

Business requirement: Alice's customer account should be deleted.

```sql
DELETE FROM silver.day5_customers
WHERE CustomerId = 3;
```

Verify:

```sql
SELECT *
FROM silver.day5_customers
ORDER BY CustomerId;
```

# 9. Check History After Delete

```sql
DESCRIBE HISTORY silver.day5_customers;
```

Identify the version immediately before the DELETE.

# 10. Time Travel

If the version before the DELETE is `2`:

```sql
SELECT *
FROM silver.day5_customers VERSION AS OF 2;
```

Compare with the current table:

```sql
SELECT *
FROM silver.day5_customers;
```

Alice should be present in the historical version but absent from the current version.

## Time Travel Concept

```text
Version 0
Initial data
     │
Version 1
David added
     │
Version 2
John updated
     │
Version 3
Alice deleted
```

Time Travel lets you read an earlier table state without changing the current table.

# 11. Time Travel Using Timestamp

Delta can also query historical data by timestamp.

```sql
SELECT *
FROM silver.day5_customers
TIMESTAMP AS OF '2026-08-15T10:00:00';
```

Use a timestamp corresponding to a time when the required table version existed.

# 12. Restore a Previous Version

First identify the desired version:

```sql
DESCRIBE HISTORY silver.day5_customers;
```

For example:

```sql
RESTORE TABLE silver.day5_customers
TO VERSION AS OF 2;
```

Verify:

```sql
SELECT *
FROM silver.day5_customers
ORDER BY CustomerId;
```

The restored table should contain the data from that version.

The restore itself becomes another table operation; it does not simply erase history.

# 13. Test Schema Enforcement

Create data with a different type for `CustomerId`:

```python
bad_data = [
    ("ABC", "Test", "Chennai", 30)
]

bad_df = spark.createDataFrame(
    bad_data,
    columns
)

bad_df.printSchema()
```

Try writing it:

```python
bad_df.write.format("delta").mode("append").saveAsTable("silver.day5_customers")
```

An incompatible schema should be rejected rather than silently corrupting the existing table.

Conceptually:

```text
Incoming Data
      │
      ▼
Schema Validation
      │
   ┌──┴──┐
   │     │
Valid  Invalid
   │     │
   ▼     ▼
 Write  Reject
```

# 14. Inspect Delta Table Metadata

```sql
DESCRIBE DETAIL silver.day5_customers;
```

Use:

```sql
DESCRIBE HISTORY silver.day5_customers;
```

to inspect table operations and versions.

# 15. ACID Transactions

ACID means:

```text
A → Atomicity
C → Consistency
I → Isolation
D → Durability
```

### Atomicity
A transaction succeeds completely or does not become partially completed.

### Consistency
Data remains within the table's defined rules and schema.

### Isolation
Concurrent operations are handled so readers and writers do not corrupt each other's results.

### Durability
Once a transaction is successfully committed, its result persists.

# 16. Why the Transaction Log Matters

```text
             Delta Table
                  │
       ┌──────────┴──────────┐
       │                     │
 Parquet Files           _delta_log
       │                     │
 Actual Data          Transaction History
                             │
                  ┌──────────┼──────────┐
                  │          │          │
                 ACID     Versions   Operations
```

The transaction log is a key part of how Delta Lake manages reliable table changes and historical versions.

# 17. Final Hands-on Exercise

Create another Delta table:

```text
silver.day5_products
```

with:

```text
ProductId
ProductName
Price
```

Initial data:

```text
101 | Laptop   | 60000
102 | Mobile   | 30000
103 | Keyboard | 2000
```

Then perform:

1. Insert a new product.
2. Update the Laptop price.
3. Delete Keyboard.
4. Run `DESCRIBE HISTORY`.
5. Identify the version before the delete.
6. Query that version using `VERSION AS OF`.
7. Restore the table to that version.
8. Run `DESCRIBE HISTORY` again.
9. Verify the restored data.

# Day 5 Project Flow

```text
Create Delta Table
       │
       ▼
Initial Version
       │
       ▼
Append New Customer
       │
       ▼
Update Customer
       │
       ▼
Delete Customer
       │
       ▼
DESCRIBE HISTORY
       │
       ▼
TIME TRAVEL
       │
       ▼
RESTORE
       │
       ▼
Schema Enforcement
```

# Key Learnings

## Delta Lake

Delta Lake provides a transactional storage layer for data engineering workloads.

A useful mental model is:

```text
Delta Lake
=
Parquet Data Files
+
Transaction Log
```

## Delta Table

```python
df.write.format("delta").saveAsTable("silver.day5_customers")
```

## Transaction History

```sql
DESCRIBE HISTORY silver.day5_customers;
```

## Table Metadata

```sql
DESCRIBE DETAIL silver.day5_customers;
```

## Time Travel

```sql
SELECT *
FROM silver.day5_customers
VERSION AS OF 2;
```

## Update

```sql
UPDATE silver.day5_customers
SET City = 'Chennai'
WHERE CustomerId = 2;
```

## Delete

```sql
DELETE FROM silver.day5_customers
WHERE CustomerId = 3;
```

## Restore

```sql
RESTORE TABLE silver.day5_customers
TO VERSION AS OF 2;
```

## Schema Enforcement

Delta can reject incompatible writes based on the existing table schema.

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
```

Current architecture:

```text
             RETAILMART
                  │
                  ▼
              RAW FILES
                  │
                  ▼
               BRONZE
                  │
                  ▼
               SILVER
             /     |      \
        Customers Products Orders
             \     |      /
              \    |     /
                  JOIN
                   │
                   ▼
                 GOLD
                   │
           ┌───────┴────────┐
           │                │
      Order Details    Customer Revenue

        All layers use Delta tables
```

# Outcome

By completing Day 5, you have practiced the fundamental Delta Lake operations required for reliable Databricks data engineering:

- Create Delta tables
- Append data
- Update records
- Delete records
- Inspect transaction history
- Query historical versions
- Restore previous versions
- Understand schema enforcement
- Understand the role of the Delta transaction log
- Understand ACID transactions

---

**Next:** Day 06 - Delta Lake `MERGE` / Upsert and Incremental Data Processing.
