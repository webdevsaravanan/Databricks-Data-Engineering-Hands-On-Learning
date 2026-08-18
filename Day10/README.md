# Day 10 - Auto Loader & Incremental File Processing

## Objective

Move from manually creating DataFrames to a realistic file-ingestion pipeline where new files arrive in storage.

```text
CSV Files
   ↓
Cloud/Object Storage
   ↓
Auto Loader
   ↓
Bronze Delta Table
   ↓
Silver
```

Today you will learn:

- Auto Loader and `cloudFiles`
- Incremental file discovery
- Explicit schemas
- Schema locations
- Checkpoints
- `_rescued_data`
- Schema evolution basics
- `trigger(availableNow=True)`
- Why Auto Loader does not deduplicate records
- Bronze append-only ingestion
- How Auto Loader connects with Bronze → Silver architecture

---

# 1. Scenario - RetailMart Customer Ingestion

RetailMart receives customer files every day:

```text
customers_01.csv
customers_02.csv
customers_03.csv
...
```

Files arrive in:

```text
/Volumes/workspace/bronze/customer_files/
```

Requirement:

> Whenever a new customer file arrives, ingest it into the Bronze Delta table.

Architecture:

```text
Customer CSV Files
        ↓
   Auto Loader
        ↓
  Bronze Delta
        ↓
      Silver
```

# 2. Why Auto Loader?

A normal batch read looks like:

```python
df = spark.read.csv("/path/customers/")
```

With many files, you need to manage which files have already been processed.

Auto Loader provides incremental file discovery.

Important:

> Auto Loader processes newly discovered files incrementally. It does not automatically perform business-key deduplication or MERGE.

# 3. Create Sample CSV Files

### customers_01.csv

```csv
CustomerId,CustomerName,City,Age
101,Arun,Chennai,30
102,Kumar,Bangalore,35
103,Priya,Chennai,27
```

### customers_02.csv

```csv
CustomerId,CustomerName,City,Age
104,Ravi,Coimbatore,32
105,Meena,Madurai,29
```

Place the files into:

```text
/Volumes/workspace/bronze/customer_files/
```

# 4. Auto Loader Read

Auto Loader uses Structured Streaming:

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/bronze/schema/customers/")
    .load("/Volumes/workspace/bronze/customer_files/")
)
```

Do not simply run `display(df)` here. A streaming display query needs an explicit checkpoint in this environment.

# 5. Schema Location

```python
.option("cloudFiles.schemaLocation", "/Volumes/workspace/bronze/schema/customers/")
```

This stores Auto Loader schema information.

Think:

```text
Schema Location
      ↓
"What does the incoming data schema look like?"
```

It is different from a streaming checkpoint.

# 6. Checkpoint Location

The checkpoint belongs to the streaming write:

```python
.option("checkpointLocation", "/Volumes/workspace/bronze/checkpoints/customers/")
```

It stores processing state.

Think:

```text
Checkpoint
    ↓
"What has already been processed?"
```

Remember:

```text
cloudFiles.schemaLocation
        ↓
Schema tracking

checkpointLocation
        ↓
Streaming processing state
```

# 7. Write Auto Loader Data to Bronze

```python
query = (
    df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/workspace/bronze/checkpoints/customers/")
    .trigger(availableNow=True)
    .toTable("bronze.day10_customers")
)
```

Then check:

```sql
SELECT *
FROM bronze.day10_customers
ORDER BY CustomerId;
```

# 8. Why `availableNow=True`?

```python
.trigger(availableNow=True)
```

Conceptually:

```text
Start
  ↓
Find currently available files
  ↓
Process them
  ↓
Finish
```

This works well with scheduled Databricks Jobs.

# 9. Add Another File

Create `customers_03.csv`:

```csv
CustomerId,CustomerName,City,Age
106,Suresh,Salem,41
107,Divya,Tiruchirappalli,26
```

Place it into the same input directory.

Run the Auto Loader pipeline again.

The checkpoint allows the pipeline to remember previously processed files.

```text
customers_01.csv → Already processed
customers_02.csv → Already processed
customers_03.csv → New
                         ↓
                     Process
```

Check:

```sql
SELECT *
FROM bronze.day10_customers
ORDER BY CustomerId;
```

# 10. Important: Auto Loader Does NOT Deduplicate

Suppose the first file contains:

```text
101 | Arun  | Chennai
102 | Kumar | Bangalore
```

A new file contains:

```text
101 | Arun  | Chennai
103 | Priya | Chennai
```

Auto Loader sees a new file and appends its records:

```text
101 | Arun  | Chennai
102 | Kumar | Bangalore
101 | Arun  | Chennai
103 | Priya | Chennai
```

The duplicate `CustomerId = 101` is expected.

Auto Loader works at the file-ingestion level:

```text
New File
   ↓
Detect File
   ↓
Read File
   ↓
Append Records
```

It does not know that `CustomerId` is your business key.

# 11. Auto Loader vs MERGE

Remember:

```text
Auto Loader
    ↓
File-level incremental ingestion
```

Whereas:

```text
Deduplication + MERGE
    ↓
Record-level incremental processing
```

Production-style pipeline:

```text
New Files
   ↓
Auto Loader
   ↓
Bronze
   ↓
Data Quality
   ↓
Deduplication
   ↓
Latest Record
   ↓
MERGE
   ↓
Silver
```

This connects Day 10 with Days 6, 7, 8 and 9.

# 12. Explicit Schema

Instead of schema inference, define the expected schema yourself.

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

customer_schema = StructType([
    StructField("CustomerId", IntegerType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Age", IntegerType(), True)
])
```

Then use:

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(customer_schema)
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/bronze/schema/customers/")
    .load("/Volumes/workspace/bronze/customer_files/")
)
```

When using an explicit schema, you do not need:

```python
.option("inferSchema", "true")
```

# 13. Explicit Schema and Bad Data

Suppose:

```text
Age → Integer
```

but the source contains:

```csv
108,Vinoth,Salem,abc
```

`abc` cannot be converted to an integer.

Without rescued-data handling, the value can become:

```text
Age = NULL
```

# 14. `_rescued_data`

Explicitly enable the rescued data column:

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(customer_schema)
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/bronze/schema/customers/")
    .load("/Volumes/workspace/bronze/customer_files/")
)
```

Unexpected or incompatible data can then be captured in:

```text
_rescued_data
```

For example:

```text
CustomerId | CustomerName | City  | Age  | _rescued_data
----------------------------------------------------------
108        | Vinoth       | Salem | null | {"Age":"abc",...}
```

# 15. New Column Example

Current schema:

```text
CustomerId
CustomerName
City
Age
```

New file:

```csv
CustomerId,CustomerName,City,Age,Email
109,Ravi,Chennai,30,ravi@example.com
```

`Email` is not part of the current schema.

Depending on Auto Loader schema-evolution configuration, the new field can either be incorporated through schema evolution or be rescued.

Important:

> `_rescued_data` is a safety mechanism for data that does not fit the current schema. It is not simply a permanent storage location for every new column.

# 16. Schema Evolution vs Rescued Data

```text
                 New / Unexpected Data
                         │
              ┌──────────┴──────────┐
              │                     │
       Schema Evolution       Rescued Data
              │                     │
              ▼                     ▼
      Update schema          Keep unexpected
                             data in
                             _rescued_data
```

# 17. Why `display(df)` Failed

A streaming DataFrame created with:

```python
spark.readStream
```

is not a normal static DataFrame.

Running:

```python
display(df)
```

can start a streaming query. Databricks therefore requires an explicit checkpoint in this environment.

For the Day 10 project, the recommended approach is to write the stream using:

```python
.option("checkpointLocation", "/Volumes/workspace/bronze/checkpoints/customers/")
```

and query the Bronze table afterward:

```sql
SELECT *
FROM bronze.day10_customers;
```

# 18. Three Important Locations

### Source location

```text
/Volumes/workspace/bronze/customer_files/
```

Contains the actual CSV files.

### Schema location

```text
/Volumes/workspace/bronze/schema/customers/
```

Stores Auto Loader schema information.

### Checkpoint location

```text
/Volumes/workspace/bronze/checkpoints/customers/
```

Stores streaming processing state.

Mental model:

```text
CSV Files
   │
   ▼
SOURCE LOCATION
   │
   ▼
Auto Loader
   │
   ├──────────────► SCHEMA LOCATION
   │                 "What is the schema?"
   │
   ▼
DataFrame
   │
   ▼
writeStream
   │
   └──────────────► CHECKPOINT LOCATION
                     "What have I processed?"
   │
   ▼
Bronze Delta Table
```

# 19. Complete Day 10 Pipeline

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

customer_schema = StructType([
    StructField("CustomerId", IntegerType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Age", IntegerType(), True)
])

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(customer_schema)
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/bronze/schema/customers/")
    .load("/Volumes/workspace/bronze/customer_files/")
)
```

Write to Bronze:

```python
query = (
    df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/workspace/bronze/checkpoints/customers/")
    .trigger(availableNow=True)
    .toTable("bronze.day10_customers")
)
```

Check:

```sql
SELECT *
FROM bronze.day10_customers
ORDER BY CustomerId;
```

# 20. Day 10 Hands-on Project

## RetailMart Customer File Ingestion

### Step 1

Create:

```text
/Volumes/workspace/bronze/customer_files/
```

### Step 2

Add `customers_01.csv`:

```csv
CustomerId,CustomerName,City,Age
101,Arun,Chennai,30
102,Kumar,Bangalore,35
103,Priya,Chennai,27
```

### Step 3

Define an explicit schema using `StructType`.

### Step 4

Configure Auto Loader with:

```text
cloudFiles
cloudFiles.format
cloudFiles.schemaLocation
rescuedDataColumn
```

### Step 5

Write to Bronze using:

```text
writeStream
checkpointLocation
availableNow
```

### Step 6

Add `customers_02.csv`:

```csv
CustomerId,CustomerName,City,Age
104,Ravi,Coimbatore,32
105,Meena,Madurai,29
```

Run the pipeline again.

### Step 7

Add `customers_03.csv`:

```csv
CustomerId,CustomerName,City,Age
106,Suresh,Salem,41
107,Divya,Tiruchirappalli,26
```

Run again.

### Step 8 - Test Duplicate Business Records

Create:

```csv
CustomerId,CustomerName,City,Age
101,Arun,Chennai,30
108,Vinoth,Salem,30
```

Run Auto Loader.

Verify that `CustomerId = 101` appears twice.

This demonstrates:

> Auto Loader prevents reprocessing of the same file, but it does not deduplicate records across different files.

### Step 9 - Test Type Mismatch

Create:

```csv
CustomerId,CustomerName,City,Age
109,Ravi,Chennai,abc
```

With:

```python
.option("rescuedDataColumn", "_rescued_data")
```

verify how the incompatible value is handled.

# Key Learnings

## Auto Loader

```text
Auto Loader
    ↓
Incremental file ingestion
```

## Checkpoint

```text
Checkpoint
    ↓
Streaming processing state
```

## Schema Location

```text
Schema Location
    ↓
Auto Loader schema tracking
```

## Explicit Schema

```text
StructType
    ↓
Expected structure and data types
```

## Rescued Data

```text
Unexpected/incompatible data
            ↓
      _rescued_data
```

## MERGE

```text
Business-key update/insert
            ↓
          MERGE
```

These are different responsibilities.

# Day 10 Architecture

```text
                   SOURCE SYSTEM
                        │
                        ▼
                 CSV / JSON FILES
                        │
                        ▼
                  AUTO LOADER
                        │
               ┌────────┴────────┐
               │                 │
        Schema Handling      File Discovery
               │                 │
               └────────┬────────┘
                        ▼
                     BRONZE
                Raw / Append Only
                        │
                        ▼
                  DATA QUALITY
                        │
                        ▼
                  DEDUPLICATION
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

# Progress So Far

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
ACID / Time Travel / History
        ↓
Day 6
Incremental Processing
MERGE / Upsert
        ↓
Day 7
Data Quality
Validation / Quarantine
        ↓
Day 8
Window Functions
Deduplication / Latest Record
        ↓
Day 9
Complete Incremental Pipeline
        ↓
Day 10
Auto Loader
Incremental File Ingestion
Schema Handling
Rescued Data
```

# Outcome

By completing Day 10, you should be able to:

- Explain what Auto Loader is
- Build an incremental file ingestion pipeline
- Use `cloudFiles`
- Understand Structured Streaming
- Configure schema locations
- Configure checkpoints
- Use explicit PySpark schemas
- Understand `_rescued_data`
- Understand basic schema evolution concepts
- Use `availableNow`
- Explain why Auto Loader does not deduplicate business records
- Understand Bronze as an append-oriented raw layer
- Explain why deduplication and MERGE belong in downstream processing

---

**Next: Day 11 - Auto Loader + Bronze → Silver incremental pipeline with data quality, deduplication, latest-record selection and MERGE.**
