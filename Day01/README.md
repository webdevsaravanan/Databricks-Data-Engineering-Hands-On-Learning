# Day 01 - Databricks Foundations

## Project

RetailMart Data Engineering Platform

## Objective

Set up the Databricks environment and understand the basic components required to build a Data Engineering solution.

## Concepts Learned

- Databricks Workspace
- Apache Spark
- Lakehouse
- Databricks Notebook
- Compute
- Catalog
- Schema
- Managed Tables
- Delta Tables
- PySpark DataFrames
- Spark SQL

## Tasks Completed

### 1. Created Databricks Notebook

Notebook:

`Day01_Setup`

### 2. Executed Python

```python
print("Welcome to RetailMart Data Engineering")
```

### 3. Executed SQL

```sql
SELECT current_timestamp();
```

### 4. Created a Spark DataFrame

```python
employees = [
    (1, "Saravana", "IT", 60000),
    (2, "John", "Sales", 45000),
    (3, "David", "HR", 50000)
]

columns = ["EmpId", "Name", "Department", "Salary"]

df = spark.createDataFrame(employees, columns)

display(df)
```

### 5. Explored the DataFrame

```python
df.show()
df.printSchema()
df.describe().show()
df.count()
```

### 6. Created a Delta Table

```python
df.write.mode("overwrite").saveAsTable("employees")
```

The DataFrame was saved as a managed Delta table and registered in the current catalog and schema.

### 7. Queried the Table

```sql
SELECT *
FROM employees;
```

## Architecture

```text
Python Data
     |
     v
Spark DataFrame
     |
     v
Managed Delta Table
     |
     v
Databricks Catalog
```

## Key Learnings

### Databricks

Databricks is a cloud data and AI platform that provides managed Apache Spark and other capabilities for data engineering, analytics, and machine learning.

### Apache Spark

Apache Spark is a distributed processing engine used to process large datasets across multiple machines.

### DataFrame

A Spark DataFrame is a distributed collection of data organized into named columns.

### Lakehouse

A Lakehouse combines capabilities of data lakes and data warehouses, providing scalable storage with reliable data management and analytics capabilities.

### Delta Table

A Delta table uses Delta Lake to provide capabilities such as ACID transactions, schema enforcement, and time travel.

### Managed Table

A managed table is registered in the catalog and its underlying storage is managed by Databricks.

## Outcome

Successfully created the first Databricks notebook, Spark DataFrame, and managed Delta table.

---

**Next:** [Day 02](../Day02/README.md) - Ingest customer data into the Bronze layer.
