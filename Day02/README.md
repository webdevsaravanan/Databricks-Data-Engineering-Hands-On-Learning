# Day 02 - Customer Data Ingestion

## Project

RetailMart Data Engineering Platform

## Objective

Ingest raw customer data into the Databricks Bronze layer and store it as a Delta table.

## Business Scenario

RetailMart receives customer data from its CRM system as a CSV file.

The requirement is:

> Load the customer data into the Bronze layer without modifying the source data.

## Concepts Learned

- Data ingestion
- CSV files
- PySpark DataFrames
- Schema inference
- CSV header
- Bronze layer
- Delta tables
- Managed tables
- Data inspection

## Source

`customers.csv`

Sample structure:

```text
CustomerId
CustomerName
Email
City
Age
```

Sample data:

```csv
CustomerId,CustomerName,Email,City,Age
1,Saravana,saravana@gmail.com,Chennai,30
2,John,john@gmail.com,Bangalore,28
3,Alice,alice@gmail.com,Hyderabad,25
4,Bob,bob@gmail.com,Chennai,35
5,David,david@gmail.com,Coimbatore,40
```

## Architecture

```text
CRM
 |
 | CSV
 v
customers.csv
 |
 v
Databricks
 |
 v
Bronze Layer
 |
 v
bronze.customers
```

## Tasks Completed

### 1. Created Bronze Schema

```sql
CREATE SCHEMA IF NOT EXISTS bronze;
```

### 2. Read CSV Using PySpark

```python
df = spark.read.csv(
    "/FileStore/customers.csv",
    header=True,
    inferSchema=True
)

display(df)
```

### 3. Inspected the Data

```python
df.printSchema()
df.show()
df.count()
df.describe().show()
```

### 4. Performed Basic Exploration

Select columns:

```python
df.select("CustomerName", "City").show()
```

Filter Chennai customers:

```python
df.filter(df.City == "Chennai").show()
```

Filter customers older than 30:

```python
df.filter(df.Age > 30).show()
```

Find distinct customer names:

```python
df.select("CustomerName").distinct().show()
```

### 5. Created Bronze Delta Table

```python
df.write.mode("overwrite").saveAsTable("bronze.customers")
```

### 6. Verified the Table

```sql
SHOW TABLES IN bronze;
```

```sql
SELECT *
FROM bronze.customers;
```

## Why Bronze?

The Bronze layer stores data as close to the source as practical.

At this stage, we do not perform business transformations.

```text
Source Data
     |
     v
Bronze
     |
     v
Raw / Source-aligned Data
```

Cleaning and business transformations will happen in downstream layers.

## Key Learnings

### Schema Inference

Spark can automatically infer column data types from the source data.

```python
inferSchema=True
```

### DataFrame

The CSV data was loaded into a Spark DataFrame for distributed processing.

### Bronze Layer

The Bronze layer contains raw or source-aligned data and acts as the foundation for downstream processing.

### Delta Table

The ingested data was stored as a Delta table:

`bronze.customers`

## Data Flow

```text
customers.csv
     |
     v
PySpark DataFrame
     |
     v
Bronze Delta Table
     |
     v
bronze.customers
```

## Outcome

Successfully ingested customer data from CSV into Databricks and created the first Bronze Delta table.

---

**Next:** [Day 03](../Day03/README.md) - Clean and transform Bronze customer data into the Silver layer.
