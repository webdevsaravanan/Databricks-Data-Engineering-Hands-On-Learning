
# Day 07 - Data Quality & Data Cleaning

## Project

RetailMart Data Engineering Platform

## Objective

Build a realistic data-quality layer between Bronze and Silver.

Today we will practice:

- Data profiling
- NULL detection
- Empty-string detection
- Duplicate detection
- Invalid-value detection
- Data-quality rules
- PySpark data cleaning
- Quarantine tables
- Basic data-quality metrics

## Scenario

RetailMart receives customer data containing bad records.

```text
                 BRONZE
                    │
                    ▼
             DATA QUALITY
                CHECKS
                    │
          ┌─────────┴─────────┐
          │                   │
        VALID               INVALID
          │                   │
          ▼                   ▼
       SILVER             QUARANTINE
```

# 1. Create Today's Source Data

```python
customers = [
    (1, "Saravana", "Chennai", 30),
    (2, "John", "Bangalore", 28),
    (3, "Alice", "Hyderabad", 25),
    (3, "Alice", "Hyderabad", 25),
    (4, "David", None, 40),
    (5, None, "Chennai", 27),
    (6, "Kumar", "Chennai", -5),
    (7, "", "Bangalore", 32),
    (None, "Priya", "Chennai", 29)
]

columns = ["CustomerId", "CustomerName", "City", "Age"]

df = spark.createDataFrame(customers, columns)

display(df)
```

# 2. Store Raw Data in Bronze

```python
df.write.format("delta").mode("overwrite").saveAsTable("bronze.day7_customers")
```

Verify:

```sql
SELECT *
FROM bronze.day7_customers;
```

Bronze should generally preserve incoming source data. Cleaning happens downstream.

# 3. Identify NULL Values

```sql
SELECT *
FROM bronze.day7_customers
WHERE CustomerId IS NULL;
```

```sql
SELECT *
FROM bronze.day7_customers
WHERE CustomerName IS NULL;
```

```sql
SELECT *
FROM bronze.day7_customers
WHERE City IS NULL;
```

# 4. Count NULL Values

```sql
SELECT
    COUNT(*) AS TotalRecords,
    SUM(CASE WHEN CustomerId IS NULL THEN 1 ELSE 0 END) AS NullCustomerId,
    SUM(CASE WHEN CustomerName IS NULL THEN 1 ELSE 0 END) AS NullCustomerName,
    SUM(CASE WHEN City IS NULL THEN 1 ELSE 0 END) AS NullCity,
    SUM(CASE WHEN Age IS NULL THEN 1 ELSE 0 END) AS NullAge
FROM bronze.day7_customers;
```

# 5. Detect Duplicate Records

```sql
SELECT
    CustomerId,
    COUNT(*) AS RecordCount
FROM bronze.day7_customers
GROUP BY CustomerId
HAVING COUNT(*) > 1;
```

You should find CustomerId `3`.

# 6. Duplicate Record vs Duplicate Business Key

Exact duplicate:

```text
3 | Alice | Hyderabad | 25
3 | Alice | Hyderabad | 25
```

Business-key duplicate:

```text
3 | Alice | Hyderabad | 25
3 | Alice | Chennai   | 26
```

The second case requires a business rule to decide which record should survive.

# 7. Detect Invalid Age

Business rule:

```text
Age must be between 0 and 120
```

```sql
SELECT *
FROM bronze.day7_customers
WHERE Age < 0
   OR Age > 120;
```

# 8. Detect Empty Strings

NULL and empty string are different.

```sql
SELECT *
FROM bronze.day7_customers
WHERE TRIM(CustomerName) = '';
```

Handle both NULL and empty strings:

```sql
SELECT *
FROM bronze.day7_customers
WHERE CustomerName IS NULL
   OR TRIM(CustomerName) = '';
```

# 9. Define Data Quality Rules

```text
CustomerId cannot be NULL
CustomerId must be unique
CustomerName cannot be NULL or empty
City cannot be NULL
Age must be between 0 and 120
```

# 10. Create a Clean Silver Dataset

```python
from pyspark.sql.functions import col, trim

clean_df = (
    df
    .filter(col("CustomerId").isNotNull())
    .filter(col("CustomerName").isNotNull())
    .filter(trim(col("CustomerName")) != "")
    .filter(col("City").isNotNull())
    .filter((col("Age") >= 0) & (col("Age") <= 120))
    .dropDuplicates(["CustomerId"])
)

display(clean_df)
```

# 11. Write Clean Data to Silver

```python
clean_df.write.format("delta").mode("overwrite").saveAsTable("silver.day7_customers")
```

Verify:

```sql
SELECT *
FROM silver.day7_customers
ORDER BY CustomerId;
```

# 12. Compare Bronze vs Silver

Bronze:

```sql
SELECT COUNT(*) AS BronzeCount
FROM bronze.day7_customers;
```

Silver:

```sql
SELECT COUNT(*) AS SilverCount
FROM silver.day7_customers;
```

The difference represents records removed by the current cleaning rules.

# 13. Quarantine Invalid Records

A production-friendly pattern is:

```text
                    SOURCE
                       │
                       ▼
                    BRONZE
                       │
                       ▼
               DATA QUALITY
                 VALIDATION
                 /                         /                        VALID        INVALID
               │              │
               ▼              ▼
            SILVER       QUARANTINE
```

Create invalid records:

```python
invalid_df = (
    df
    .filter(
        col("CustomerId").isNull()
        | col("CustomerName").isNull()
        | (trim(col("CustomerName")) == "")
        | col("City").isNull()
        | (col("Age") < 0)
        | (col("Age") > 120)
    )
)

display(invalid_df)
```

Store them:

```python
invalid_df.write.format("delta").mode("overwrite").saveAsTable("silver.day7_customer_quarantine")
```

# 14. Important Deduplication Issue

Suppose the source contains:

```text
3 | Alice | Hyderabad | 25
3 | Alice | Chennai   | 26
```

Which record should Silver keep?

You need a business rule, such as:

```text
Keep the latest record
```

This normally requires a field such as:

```text
UpdatedTimestamp
```

or:

```text
BatchTimestamp
```

This leads to the next important topic:

> Deduplication using Window Functions.

# 15. Data Quality Metrics

Useful production metrics include:

```text
Total Records
Valid Records
Invalid Records
Duplicate Records
Data Quality Percentage
```

Formula:

```text
Data Quality % =
Valid Records / Total Records × 100
```

# 16. Day 7 Hands-on Project - Products

Input:

```text
ProductId | ProductName | Category    | Price
------------------------------------------------
101       | Laptop      | Electronics | 60000
102       | Mobile      | Electronics | 30000
103       | Keyboard    | Accessories | 2000
103       | Keyboard    | Accessories | 2000
104       | Mouse       | Accessories | -500
105       | Webcam      | Accessories | NULL
NULL      | Monitor     | Electronics | 15000
106       | ""          | Accessories | 1000
```

## Task 1 - Bronze

Create:

```text
bronze.day7_products
```

## Task 2 - Identify Problems

Find:

- NULL ProductId
- NULL Price
- Empty ProductName
- Negative Price
- Duplicate ProductId

## Task 3 - Define Rules

```text
ProductId cannot be NULL
ProductId must be unique
ProductName cannot be NULL or empty
Price cannot be NULL
Price must be greater than 0
```

## Task 4 - Silver

Create:

```text
silver.day7_products
```

containing valid records only.

## Task 5 - Quarantine

Create:

```text
silver.day7_product_quarantine
```

containing invalid records.

## Task 6 - Metrics

Calculate:

```text
Total Records
Valid Records
Invalid Records
Duplicate Records
Data Quality %
```

# 17. Production Architecture

```text
                         SOURCE
                           │
                           ▼
                     Raw Customer File
                           │
                           ▼
                       🥉 BRONZE
                           │
                           ▼
                  Data Quality Checks
                           │
                  ┌────────┴────────┐
                  │                 │
                VALID             INVALID
                  │                 │
                  ▼                 ▼
              🥈 SILVER       QUARANTINE
                  │
                  ▼
                MERGE
                  │
                  ▼
              🥈 SILVER
                  │
                  ▼
              🥇 GOLD
```

# Day 7 Project Flow

```text
Source
  │
  ▼
Bronze Delta Table
  │
  ▼
Profile Data
  │
  ├── NULL checks
  ├── Duplicate checks
  ├── Empty-string checks
  └── Business-rule checks
  │
  ├───────────────┐
  ▼               ▼
Valid           Invalid
  │               │
  ▼               ▼
Silver        Quarantine
  │
  ▼
Gold
```

# Key Learnings

## Data Quality

Ensuring data satisfies defined technical and business rules before downstream consumption.

## Data Profiling

Understanding the characteristics and quality of incoming data.

## Validation

Checking whether records satisfy the required rules.

## Deduplication

Removing or resolving duplicate records according to a defined business rule.

## Quarantine

Keeping invalid records separately so they can be investigated rather than silently discarded.

## NULL Handling

NULL means a value is missing or unknown. It should be handled explicitly.

## Bronze vs Silver

```text
Bronze
→ Preserve source data

Silver
→ Clean, validated, usable data
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
Validation
NULL Handling
Duplicates
Quarantine
```

# Outcome

By completing Day 7, you should be able to:

- Profile incoming data
- Detect NULL values
- Detect empty strings
- Detect duplicates
- Detect invalid values
- Define data-quality rules
- Clean data using PySpark
- Create Silver-quality data
- Quarantine invalid records
- Calculate basic quality metrics
- Understand why data quality is part of a production ETL pipeline

---

**Next:** Day 08 - Advanced Deduplication with PySpark Window Functions and handling multiple versions of the same business key.
