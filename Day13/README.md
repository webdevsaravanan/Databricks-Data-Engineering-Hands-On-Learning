# Day 13 - Complete Incremental Auto Loader → Bronze → Silver Pipeline

## Objective

Build a complete incremental Bronze → Silver pipeline by combining Auto Loader, Structured Streaming, `foreachBatch`, micro-batch deduplication, and conditional Delta `MERGE`.

The main new concept is protecting Silver from an **older record overwriting a newer record**.

```text
CSV Files
    ↓
Auto Loader
    ↓
Structured Streaming
    ↓
Bronze Delta Table
    ↓
readStream
    ↓
Micro-batch
    ↓
foreachBatch
    ↓
Deduplication
    ↓
Conditional MERGE
    ↓
Silver Delta Table
```

---

## 1. Day 13 Scenario

We continue the RetailMart customer pipeline.

Customer data arrives as CSV files.

### customers_01.csv

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
101,Arun,Chennai,30,2026-08-20 09:00:00
102,Kumar,Bangalore,35,2026-08-20 09:05:00
```

Later:

### customers_02.csv

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
101,Arun,Bangalore,31,2026-08-21 10:00:00
103,Priya,Chennai,27,2026-08-21 10:05:00
```

Bronze should retain both versions:

```text
101 Arun Chennai    30  2026-08-20
101 Arun Bangalore  31  2026-08-21
102 Kumar Bangalore 35  2026-08-20
103 Priya Chennai   27  2026-08-21
```

Silver should contain the latest state:

```text
101 Arun  Bangalore 31
102 Kumar Bangalore 35
103 Priya Chennai   27
```

---

## 2. Why Conditional MERGE?

Previously we used:

```python
.whenMatchedUpdateAll()
```

That means an existing customer is always updated.

Consider:

```text
Silver:
101 | Arun | Bangalore | 31 | 2026-08-21

Incoming:
101 | Arun | Chennai | 30 | 2026-08-19
```

The incoming record is older.

We do not want it to overwrite the newer Silver record.

Therefore:

```text
UPDATE only when:

source.UpdatedAt > target.UpdatedAt
```

---

## 3. Create Source Directory

Use:

```text
/Volumes/workspace/bronze/day13_customer_files/
```

Create:

```text
customers_01.csv
```

with:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
101,Arun,Chennai,30,2026-08-20 09:00:00
102,Kumar,Bangalore,35,2026-08-20 09:05:00
```

---

## 4. Define Explicit Schema

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

customer_schema = StructType([
    StructField("CustomerId", IntegerType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("UpdatedAt", StringType(), True)
])
```

---

## 5. Auto Loader → Bronze

```python
bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(customer_schema)
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/bronze/schema/day13_customers/")
    .load("/Volumes/workspace/bronze/day13_customer_files/")
)
```

`bronze_stream` is a **streaming DataFrame**.

---

## 6. Write Bronze

Because Databricks Free Edition uses serverless compute, use:

```python
.trigger(availableNow=True)
```

Do not use:

```python
.trigger(processingTime="1 minute")
```

Bronze query:

```python
bronze_query = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/workspace/bronze/checkpoints/day13_bronze/")
    .trigger(availableNow=True)
    .toTable("bronze.day13_customers")
)
```

Wait:

```python
bronze_query.awaitTermination()
```

Check Bronze:

```sql
SELECT *
FROM bronze.day13_customers
ORDER BY CustomerId, UpdatedAt;
```

---

## 7. Create Silver Table

```sql
CREATE TABLE IF NOT EXISTS silver.day13_customers (
    CustomerId INT,
    CustomerName STRING,
    City STRING,
    Age INT,
    UpdatedAt TIMESTAMP
)
USING DELTA;
```

`UpdatedAt` is stored as a proper timestamp in Silver.

---

## 8. Read Bronze as Streaming

Create the Silver input stream:

```python
silver_stream = (
    spark.readStream
    .table("bronze.day13_customers")
)
```

Important:

```text
spark.table()
    ↓
Batch DataFrame

spark.readStream.table()
    ↓
Streaming DataFrame
```

We use `readStream` because we want incremental processing.

---

## 9. Create foreachBatch Function

```python
def process_silver(batch_df, batch_id):

    print(f"Processing Silver batch: {batch_id}")

    from pyspark.sql.functions import col, to_timestamp, row_number
    from pyspark.sql.window import Window
    from delta.tables import DeltaTable

    batch_df = batch_df.withColumn(
        "UpdatedAt",
        to_timestamp(col("UpdatedAt"))
    )

    window_spec = (
        Window
        .partitionBy("CustomerId")
        .orderBy(col("UpdatedAt").desc())
    )

    latest_batch = (
        batch_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    silver_table = DeltaTable.forName(
        batch_df.sparkSession,
        "silver.day13_customers"
    )

    (
        silver_table.alias("target")
        .merge(
            latest_batch.alias("source"),
            "target.CustomerId = source.CustomerId"
        )
        .whenMatchedUpdate(
            condition="source.UpdatedAt > target.UpdatedAt",
            set={
                "CustomerName": "source.CustomerName",
                "City": "source.City",
                "Age": "source.Age",
                "UpdatedAt": "source.UpdatedAt"
            }
        )
        .whenNotMatchedInsert(
            values={
                "CustomerId": "source.CustomerId",
                "CustomerName": "source.CustomerName",
                "City": "source.City",
                "Age": "source.Age",
                "UpdatedAt": "source.UpdatedAt"
            }
        )
        .execute()
    )
```

---

## 10. Why batch_df.sparkSession?

In Day 12 you encountered a Spark Connect serialization error when using the global:

```python
spark
```

inside `foreachBatch`.

Inside the callback, use:

```python
batch_df.sparkSession
```

when you need the Spark session.

Therefore:

```python
silver_table = DeltaTable.forName(
    batch_df.sparkSession,
    "silver.day13_customers"
)
```

is intentional.

---

## 11. Start Silver Streaming

```python
silver_query = (
    silver_stream.writeStream
    .foreachBatch(process_silver)
    .option("checkpointLocation", "/Volumes/workspace/bronze/checkpoints/day13_silver/")
    .trigger(availableNow=True)
    .start()
)
```

Wait:

```python
silver_query.awaitTermination()
```

Check:

```sql
SELECT *
FROM silver.day13_customers
ORDER BY CustomerId;
```

Expected:

```text
101 | Arun  | Chennai   | 30
102 | Kumar | Bangalore | 35
```

---

## 12. Add the Second File

Create:

`customers_02.csv`

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
101,Arun,Bangalore,31,2026-08-21 10:00:00
103,Priya,Chennai,27,2026-08-21 10:05:00
```

Place it in:

```text
/Volumes/workspace/bronze/day13_customer_files/
```

Run the Bronze pipeline again.

Then run the Silver pipeline again.

Check:

```sql
SELECT *
FROM silver.day13_customers
ORDER BY CustomerId;
```

Expected:

```text
101 | Arun  | Bangalore | 31
102 | Kumar | Bangalore | 35
103 | Priya | Chennai   | 27
```

Customer `101` was updated.

Customer `103` was inserted.

---

## 13. Test an Older Record

This is the most important test of Day 13.

Create:

`customers_03.csv`

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
101,Arun,Chennai,30,2026-08-19 08:00:00
```

Run Bronze and then Silver.

Silver currently has:

```text
101 | Arun | Bangalore | 31 | 2026-08-21
```

Incoming data:

```text
101 | Arun | Chennai | 30 | 2026-08-19
```

The MERGE condition is:

```text
source.UpdatedAt > target.UpdatedAt
```

Comparison:

```text
2026-08-19 > 2026-08-21
        ↓
       FALSE
```

Therefore the record is **not updated**.

Silver remains:

```text
101 | Arun | Bangalore | 31
```

---

## 14. Understand the Deduplication

Suppose one micro-batch contains:

```text
101 | Arun | Chennai    | 30 | 2026-08-20
101 | Arun | Bangalore  | 31 | 2026-08-21
101 | Arun | Coimbatore | 32 | 2026-08-22
```

The window:

```python
window_spec = (
    Window
    .partitionBy("CustomerId")
    .orderBy(col("UpdatedAt").desc())
)
```

produces conceptually:

```text
CustomerId | UpdatedAt | row_num
-----------|-----------|--------
101        | 08-22     | 1
101        | 08-21     | 2
101        | 08-20     | 3
```

Then:

```python
.filter(col("row_num") == 1)
```

keeps only:

```text
101 | Arun | Coimbatore | 32
```

---

## 15. Two Layers of Protection

Our pipeline now has two different protections.

### Protection 1 - Micro-batch Deduplication

```text
Multiple records for same CustomerId
                ↓
        Keep latest record
```

### Protection 2 - Conditional MERGE

```text
Incoming record
       ↓
Is source newer than Silver?
       │
    ┌──┴──┐
   YES    NO
    ↓      ↓
 UPDATE  IGNORE
```

Together:

```text
Micro-batch
    ↓
Deduplication
    ↓
Latest source record
    ↓
Compare with Silver
    ↓
Update only if newer
```

---

## 16. Why Bronze and Silver Are Separate

### Bronze

Bronze captures source data:

```text
Source
  ↓
Auto Loader
  ↓
Bronze
```

Bronze can contain:

```text
Duplicates
Older records
Raw values
Bad records
Multiple versions
```

### Silver

Silver contains cleaned/current data:

```text
Bronze
  ↓
Data Quality
  ↓
Transformation
  ↓
Deduplication
  ↓
Business Rules
  ↓
Silver
```

Therefore:

> **Bronze = what arrived**

> **Silver = clean/current state**

---

## 17. Complete Architecture

```text
                         CSV FILES
                             │
                             ▼
                       AUTO LOADER
                             │
                             ▼
                         readStream
                             │
                             ▼
                       writeStream
                             │
                             ▼
                    🥉 BRONZE DELTA
                             │
                             ▼
                         readStream
                             │
                             ▼
                         Micro-batch
                             │
                             ▼
                        foreachBatch
                             │
                             ▼
                         batch_df
                             │
                             ▼
                       Deduplication
                             │
                             ▼
                      Latest Customer
                             │
                             ▼
                         Delta MERGE
                             │
                   ┌─────────┴─────────┐
                   │                   │
              Existing ID          New ID
                   │                   │
                   ▼                   ▼
           Is source newer?          INSERT
              │      │
             YES    NO
              │      │
              ▼      ▼
            UPDATE  IGNORE
              │
              └──────────┬──────────┘
                         ▼
                     🥈 SILVER
```

---

## 18. Day 10–13 Progression

### Day 10

```text
Auto Loader
```

### Day 11

```text
Auto Loader
   ↓
Bronze
   ↓
Deduplication
   ↓
MERGE
   ↓
Silver
```

### Day 12

```text
Structured Streaming
   ↓
AvailableNow
   ↓
foreachBatch
```

### Day 13

```text
Auto Loader
   ↓
Bronze
   ↓
readStream
   ↓
foreachBatch
   ↓
Deduplication
   ↓
Conditional MERGE
   ↓
Silver
```

---

## 19. Day 13 Checklist

- [ ] Create customer CSV files
- [ ] Configure Auto Loader
- [ ] Use explicit schema
- [ ] Write Bronze using `availableNow`
- [ ] Create Silver Delta table
- [ ] Read Bronze using `readStream`
- [ ] Create `foreachBatch`
- [ ] Understand `batch_df`
- [ ] Use `batch_df.sparkSession`
- [ ] Convert `UpdatedAt` to timestamp
- [ ] Deduplicate each micro-batch
- [ ] MERGE into Silver
- [ ] Test INSERT
- [ ] Test UPDATE
- [ ] Add an older record
- [ ] Verify older data does not overwrite newer Silver data
- [ ] Understand conditional MERGE
- [ ] Understand Bronze vs Silver responsibilities

---

# Key Learnings

### Auto Loader

```text
Incremental file discovery
```

### Structured Streaming

```text
Incremental processing
```

### foreachBatch

```text
Micro-batch
    ↓
Normal DataFrame
    ↓
Batch-style operations
```

### ROW_NUMBER

```text
Find latest record per CustomerId
```

### Conditional MERGE

```text
Update only when source is newer
```

### Bronze

```text
Raw / historical incoming data
```

### Silver

```text
Clean / current business state
```

---

# Day 13 Final Mental Model

```text
                 NEW CSV FILE
                       │
                       ▼
                  AUTO LOADER
                       │
                       ▼
                    BRONZE
                       │
                       ▼
                  readStream
                       │
                       ▼
                  MICRO-BATCH
                       │
                       ▼
                  foreachBatch
                       │
                       ▼
                    batch_df
                       │
                       ▼
                 DEDUPLICATION
                       │
                       ▼
                 LATEST RECORD
                       │
                       ▼
              CONDITIONAL MERGE
                       │
                 ┌─────┴─────┐
                 ▼           ▼
              UPDATE       INSERT
                 │           │
                 └─────┬─────┘
                       ▼
                    SILVER
```

## Day 13 Outcome

You should now be able to explain:

> **Auto Loader incrementally loads files into Bronze. A streaming read of Bronze produces micro-batches. `foreachBatch` provides each micro-batch as a normal DataFrame, allowing deduplication and Delta MERGE. The conditional MERGE ensures that an older source record cannot overwrite a newer Silver record.**

**Next: Day 14 - Data Quality: handling nulls, invalid values, bad records, and quarantine tables.**
