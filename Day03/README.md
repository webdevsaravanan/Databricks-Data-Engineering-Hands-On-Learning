# Day 03 - Silver Layer Data Cleaning

## Project

RetailMart Data Engineering Platform

## Objective

Build a Bronze-to-Silver ETL pipeline using PySpark.

In this exercise, `customer.csv` is stored in a Unity Catalog Volume, ingested into a Bronze Delta table, then cleaned and validated into a Silver Delta table.

## Architecture

```text
CRM
 |
 | customer.csv
 v
Unity Catalog Volume
 |
 | spark.read.csv()
 v
DataFrame
 |
 | saveAsTable()
 v
bronze.customers
 |
 | PySpark transformations
 v
silver.customers
```

## Databricks Objects

```text
Catalog
│
├── bronze (Schema)
│   ├── raw_files (Volume)
│   │   └── customer.csv
│   └── customers (Bronze Delta Table)
│
└── silver (Schema)
    └── customers (Silver Delta Table)
```

### Important distinction

- **Volume** → stores files
- **DataFrame** → processes data
- **Bronze table** → stores source-aligned data
- **Silver table** → stores cleaned and validated data
- **Schema** → organizes and governs these objects

## Concepts Learned

- Unity Catalog Volume
- CSV ingestion
- PySpark DataFrames
- Bronze and Silver layers
- Data cleaning and validation
- `withColumn()`
- `trim()`
- `initcap()`
- `cast()`
- `filter()`
- `dropDuplicates()`
- NULL handling
- Business keys
- Bronze-to-Silver ETL

## 1. Create the Bronze Volume

```sql
CREATE VOLUME IF NOT EXISTS bronze.raw_files;
```

Example path:

```text
/Volumes/workspace/bronze/raw_files/
```

## 2. Store customer.csv

Example:

```csv
CustomerId,CustomerName,Email,City,Age
1,Saravana,saravana@gmail.com,Chennai,30
2,John,john@gmail.com,Bangalore,28
3,Alice,alice@gmail.com,Hyderabad,25
4," Bob ","bob@gmail.com"," Chennai ",35
5,David,david@gmail.com,Coimbatore,40
6,Emma,emma@gmail.com,Madurai,27
7,Raj,raj@gmail.com,Salem,31
7,Raj,raj@gmail.com,Salem,31
8," Kumar ",kumar@gmail.com,chennai,abc
9,Anita,,Chennai,29
10,Ravi,ravi@gmail.com,,32
```

Store it at:

```text
/Volumes/workspace/bronze/raw_files/customer.csv
```

## 3. Read CSV Using PySpark

```python
df = spark.read.csv(
    "/Volumes/workspace/bronze/raw_files/customer.csv",
    header=True,
    inferSchema=True
)

display(df)
```

Check:

```python
df.printSchema()
df.count()
```

## 4. Create Bronze Table

```sql
CREATE SCHEMA IF NOT EXISTS bronze;
```

```python
df.write     .mode("overwrite")     .saveAsTable("bronze.customers")
```

Verify:

```sql
SELECT *
FROM bronze.customers;
```

## 5. Read Bronze for Silver Processing

```python
df = spark.table("bronze.customers")

display(df)
```

The Silver pipeline reads from Bronze rather than directly from the CSV.

## 6. Data Quality Problems

| Problem | Example |
|---|---|
| Extra spaces | ` Bob ` |
| Duplicate | CustomerId 7 |
| Invalid age | `abc` |
| Missing email | Anita |
| Missing city | Ravi |
| Inconsistent case | `chennai` |

## 7. Clean Customer Name

```python
from pyspark.sql.functions import col, trim, initcap

df_clean = df.withColumn(
    "CustomerName",
    initcap(trim(col("CustomerName")))
)
```

## 8. Clean City

```python
df_clean = df_clean.withColumn(
    "City",
    initcap(trim(col("City")))
)
```

## 9. Convert Age to Integer

```python
df_clean = df_clean.withColumn(
    "Age",
    col("Age").cast("int")
)
```

Invalid values such as `abc` become `NULL`.

```python
display(
    df_clean.filter(col("Age").isNull())
)
```

## 10. Remove Duplicate Customers

CustomerId is treated as the business key.

```python
df_clean = df_clean.dropDuplicates(["CustomerId"])
```

Verify:

```python
df_clean.groupBy("CustomerId")     .count()     .filter(col("count") > 1)     .show()
```

## 11. Validate Required Fields

CustomerId, CustomerName, Email and City must not be NULL.

```python
df_clean = df_clean.filter(
    col("CustomerId").isNotNull() &
    col("CustomerName").isNotNull() &
    col("Email").isNotNull() &
    col("City").isNotNull()
)
```

## 12. Validate Age

Customer age must be between 18 and 100.

```python
df_clean = df_clean.filter(
    (col("Age") >= 18) &
    (col("Age") <= 100)
)
```

## 13. Create Silver Table

```sql
CREATE SCHEMA IF NOT EXISTS silver;
```

```python
df_clean.write     .mode("overwrite")     .saveAsTable("silver.customers")
```

Query:

```sql
SELECT *
FROM silver.customers;
```

## 14. Compare Bronze and Silver

```sql
SELECT COUNT(*) AS bronze_count
FROM bronze.customers;
```

```sql
SELECT COUNT(*) AS silver_count
FROM silver.customers;
```

Silver contains fewer records because duplicate and invalid records were removed.

## Data Quality Rules

| Column | Rule |
|---|---|
| CustomerId | Must not be NULL |
| CustomerId | Must be unique |
| CustomerName | Must not be NULL |
| Email | Must not be NULL |
| City | Must not be NULL |
| Age | Between 18 and 100 |

## Bronze vs Silver

### Bronze

`bronze.customers`

- Source-aligned data
- Minimal transformation
- Preserved for reprocessing

### Silver

`silver.customers`

- Cleaned data
- Standardized values
- Correct data types
- Deduplicated
- Validated and trusted

## Mini Challenges

1. Find customers whose age is greater than 30.
2. Find the number of customers in each city.
3. Find duplicate CustomerIds in Bronze.
4. Find records with missing values before cleaning.
5. Create a DataFrame containing only `CustomerId`, `CustomerName`, and `City`.
6. Create `valid_customers` and `invalid_customers` DataFrames and write invalid records to `silver.invalid_customers`.

## Key Learnings

### Volume

A Unity Catalog Volume stores files such as CSV, JSON, XML, Parquet, PDF and images.

Example:

```text
/Volumes/workspace/bronze/raw_files/customer.csv
```

### DataFrame

A Spark DataFrame is the processing representation of the data.

```text
CSV
 ↓
DataFrame
```

### Bronze Table

```text
bronze.customers
```

Contains source-aligned data.

### Silver Table

```text
silver.customers
```

Contains cleaned and validated data.

## Outcome

Successfully built a Bronze-to-Silver ETL pipeline:

```text
Volume
  |
  | customer.csv
  v
Bronze
  |
  | bronze.customers
  v
PySpark ETL
  |
  | Cleaning
  | Validation
  | Deduplication
  | Type Conversion
  v
Silver
  |
  | silver.customers
  v
Trusted Customer Data
```


**Next:** Day 04 - Multi-table ETL using Customers, Products, and Orders.
