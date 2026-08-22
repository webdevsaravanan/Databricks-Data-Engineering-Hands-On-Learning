# Day 12 - Structured Streaming with AvailableNow + foreachBatch

## Objective

Learn Structured Streaming in Databricks Free Edition and understand how `foreachBatch` allows each streaming micro-batch to be processed with normal DataFrame operations such as deduplication and Delta `MERGE`.

```text
CSV Files
    ↓
Auto Loader
    ↓
Structured Streaming
    ↓
AvailableNow
    ↓
Micro-batch
    ↓
foreachBatch
    ↓
Normal DataFrame
    ↓
Batch-style Processing
```

---

## 1. Free Edition Limitation

Databricks Free Edition uses serverless compute.

This trigger is not supported:

```python
.trigger(processingTime="1 minute")
```

Use:

```python
.trigger(availableNow=True)
```

`AvailableNow` processes currently available data and then terminates.

---

## 2. Batch vs Structured Streaming

### Batch

```text
Files
 ↓
Read
 ↓
Process
 ↓
Stop
```

### Structured Streaming

```text
Incoming Data
 ↓
Streaming DataFrame
 ↓
Micro-batch
 ↓
Process
 ↓
Checkpoint
```

With Free Edition:

```text
Run
 ↓
AvailableNow
 ↓
Process new data
 ↓
Checkpoint
 ↓
Stop
```

Running it again later allows Auto Loader to continue incrementally from its checkpoint.

---

## 3. Day 12 Scenario

We continue the RetailMart customer pipeline.

Source:

```text
/Volumes/workspace/bronze/customer_files/
```

Architecture:

```text
CSV
 ↓
Auto Loader
 ↓
readStream
 ↓
AvailableNow
 ↓
Micro-batch
 ↓
foreachBatch
 ↓
Bronze / Silver processing
```

---

## 4. Define Schema

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

## 5. Create Auto Loader Streaming DataFrame

```python
bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(customer_schema)
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/bronze/schema/day12_customers/")
    .load("/Volumes/workspace/bronze/customer_files/")
)
```

`bronze_stream` is a **streaming DataFrame**. It represents how Spark should process incoming data.

---

## 6. What Is a Micro-batch?

Structured Streaming processes data incrementally.

Conceptually:

```text
Source
 ↓
New data
 ↓
Micro-batch
 ↓
Process
```

Do not assume one file always equals one micro-batch. Spark determines the micro-batch boundaries.

---

## 7. What Is foreachBatch?

`foreachBatch` gives your function the current micro-batch as a normal DataFrame.

```python
def process_batch(batch_df, batch_id):
    print(f"Processing batch: {batch_id}")
    batch_df.show(truncate=False)
```

Important:

> `foreachBatch` does not create the DataFrame.

Spark creates the micro-batch DataFrame and passes it to the function.

```text
Streaming DataFrame
        ↓
Spark creates micro-batch
        ↓
batch_df
        ↓
process_batch()
```

`batch_df` is already a normal DataFrame for that micro-batch.

---

## 8. Basic foreachBatch Experiment

```python
def process_batch(batch_df, batch_id):
    print(f"Processing batch: {batch_id}")
    print(f"Rows in batch: {batch_df.count()}")
    batch_df.show(truncate=False)
```

Start the query:

```python
query = (
    bronze_stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "/Volumes/workspace/bronze/checkpoints/day12_foreachbatch/")
    .trigger(availableNow=True)
    .start()
)
```

Wait for completion:

```python
query.awaitTermination()
```

---

## 9. Using batch_df

Because `batch_df` is a normal DataFrame, normal DataFrame operations can be performed inside the callback.

```python
def process_batch(batch_df, batch_id):
    filtered = batch_df.filter(batch_df.Age >= 18)
    filtered.show()
```

For example:

```python
def process_batch(batch_df, batch_id):
    deduplicated = batch_df.dropDuplicates(["CustomerId"])
    deduplicated.show()
```

---

## 10. Spark Connect Serialization Issue

In Databricks Free Edition, avoid capturing the global `spark` session inside the `foreachBatch` callback.

This can fail:

```python
def process_batch(batch_df, batch_id):
    batch_df.createOrReplaceTempView("customer_batch")
    display(spark.sql("SELECT * FROM customer_batch"))
```

A Spark Connect serialization error can occur because the callback is trying to capture the global Spark session.

---

## 11. Correct Way to Use SQL Inside foreachBatch

If SQL is required inside the callback, use the Spark session associated with the batch DataFrame:

```python
def process_batch(batch_df, batch_id):
    batch_df.createOrReplaceTempView("customer_batch")

    batch_spark = batch_df.sparkSession

    result = batch_spark.sql(
        "SELECT * FROM customer_batch"
    )

    result.show(truncate=False)
```

Important:

```python
batch_df.sparkSession
```

instead of:

```python
spark
```

---

## 12. Write Micro-batches to Bronze

```python
def write_bronze(batch_df, batch_id):
    print(f"Processing Bronze batch: {batch_id}")
    batch_df.write.mode("append").saveAsTable("bronze.day12_customers")
```

Start:

```python
query = (
    bronze_stream.writeStream
    .foreachBatch(write_bronze)
    .option("checkpointLocation", "/Volumes/workspace/bronze/checkpoints/day12_bronze/")
    .trigger(availableNow=True)
    .start()
)
```

Wait:

```python
query.awaitTermination()
```

Check:

```sql
SELECT *
FROM bronze.day12_customers
ORDER BY CustomerId;
```

---

## 13. Add a New CSV

Create `customers_04.csv`:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
106,Suresh,Salem,40,2026-08-24 10:00:00
107,Divya,Chennai,26,2026-08-24 10:05:00
```

Place it in:

```text
/Volumes/workspace/bronze/customer_files/
```

Run the same pipeline again.

Because the checkpoint already exists:

```text
Previously processed files
        ↓
Skipped

New file
        ↓
Processed
```

---

## 14. Checkpoints

Example:

```text
/Volumes/workspace/bronze/checkpoints/day12_bronze/
```

Conceptually:

```text
Files
 ↓
Auto Loader
 ↓
Process
 ↓
Checkpoint
```

Next run:

```text
Files
 ↓
Checkpoint
 ↓
Identify unprocessed data
 ↓
Process new data
```

Do not delete the checkpoint when testing incremental behavior.

---

## 15. AvailableNow Mental Model

For Free Edition, think of `AvailableNow` as scheduled incremental processing:

```text
9:00 AM
   ↓
Databricks Job / Notebook
   ↓
Auto Loader
   ↓
AvailableNow
   ↓
Process new files
   ↓
Checkpoint
   ↓
STOP
```

Later:

```text
10:00 AM
   ↓
Databricks Job / Notebook
   ↓
Auto Loader
   ↓
AvailableNow
   ↓
Process newly available files
   ↓
Checkpoint
   ↓
STOP
```

---

## 16. AvailableNow vs ProcessingTime

| Feature | AvailableNow | ProcessingTime |
|---|---|---|
| Free Edition | Supported | Not supported |
| Processes incrementally | Yes | Yes |
| Query remains running | No | Yes |
| Good for scheduled jobs | Yes | Yes |

For your current Free Edition environment:

```text
Use AvailableNow
```

---

## 17. Why foreachBatch Is Useful

Without `foreachBatch`:

```text
Bronze Delta
     ↓
readStream
     ↓
Streaming DataFrame
     ↓
Streaming transformations
     ↓
MERGE
```

The difficulty is that Delta `MERGE` is a batch-style operation.

With `foreachBatch`:

```text
Bronze Delta
     ↓
readStream
     ↓
Micro-batch
     ↓
foreachBatch
     ↓
batch_df
     ↓
Deduplication
     ↓
Delta MERGE
     ↓
Silver
```

This gives us a normal DataFrame boundary for each micro-batch.

---

## 18. Important Distinction

Do not think:

```text
foreachBatch
    ↓
creates a DataFrame
```

Think:

```text
Structured Streaming
       ↓
Spark creates micro-batch
       ↓
batch_df is passed to foreachBatch
```

Therefore:

```python
def process_batch(batch_df, batch_id):
```

means `batch_df` is already a normal DataFrame.

---

## 19. foreachBatch Does Not Make Processing Incremental

This distinction is important.

### readStream

Determines incremental streaming input:

```text
readStream
    ↓
Incremental data
```

### Checkpoint

Tracks processing progress:

```text
Checkpoint
    ↓
What has already been processed
```

### foreachBatch

Provides a micro-batch as a normal DataFrame:

```text
Micro-batch
    ↓
batch_df
    ↓
Normal DataFrame operations
```

Therefore:

> **Structured Streaming provides incremental processing. `foreachBatch` allows each micro-batch to be processed using normal batch-style DataFrame operations.**

---

## 20. Bronze → Silver Pattern

A practical architecture is:

### Pipeline 1

```text
Source
  ↓
Auto Loader
  ↓
readStream
  ↓
writeStream
  ↓
Bronze
```

### Pipeline 2

```text
Bronze
  ↓
readStream
  ↓
Micro-batch
  ↓
foreachBatch
  ↓
Deduplication
  ↓
MERGE
  ↓
Silver
```

Bronze responsibility:

> Capture source data and preserve ingestion history.

Silver responsibility:

> Apply data quality, transformation, deduplication and business rules.

---

## 21. Example: Bronze → Silver with foreachBatch

```python
def process_silver(batch_df, batch_id):

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
        "silver.day12_customers"
    )

    (
        silver_table.alias("target")
        .merge(
            latest_batch.alias("source"),
            "target.CustomerId = source.CustomerId"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
```

Start:

```python
query = (
    bronze_stream.writeStream
    .foreachBatch(process_silver)
    .option("checkpointLocation", "/Volumes/workspace/bronze/checkpoints/day12_silver/")
    .trigger(availableNow=True)
    .start()
)
```

Wait:

```python
query.awaitTermination()
```

---

## 22. Complete Day 12 Architecture

```text
                     SOURCE
                       │
                       ▼
                  CSV FILES
                       │
                       ▼
                 AUTO LOADER
                       │
                       ▼
                 READ STREAM
                       │
                       ▼
                AVAILABLE NOW
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
              Normal PySpark Logic
                       │
                       ▼
                    MERGE
                       │
                       ▼
                    SILVER
```

---

## 23. Free Edition vs Continuous Streaming

Your Free Edition environment:

```text
Serverless
    ↓
AvailableNow
    ↓
Incremental processing
    ↓
Query finishes
```

This is different from an always-running stream:

```text
Start
 ↓
Wait
 ↓
New data
 ↓
Process
 ↓
Wait
 ↓
New data
 ↓
Process
 ↓
...
```

Do not use:

```python
.trigger(processingTime="1 minute")
```

in your Free Edition environment.

Continuous serverless pipeline patterns will be covered later with Lakeflow.

---

## 24. Day 12 Checklist

- [ ] Understand Structured Streaming
- [ ] Understand streaming DataFrames
- [ ] Understand micro-batches
- [ ] Understand `availableNow`
- [ ] Understand why `processingTime` failed in Free Edition
- [ ] Create an Auto Loader streaming DataFrame
- [ ] Run a streaming query with `availableNow`
- [ ] Understand checkpoints
- [ ] Add a new CSV
- [ ] Run the pipeline again
- [ ] Verify incremental processing
- [ ] Understand `foreachBatch`
- [ ] Understand that `batch_df` is already a normal DataFrame
- [ ] Use `batch_df.show()`
- [ ] Understand the Spark Connect serialization issue
- [ ] Use `batch_df.sparkSession` when needed inside the callback
- [ ] Write a micro-batch to a Bronze table
- [ ] Understand how `foreachBatch` can be used with Delta MERGE
- [ ] Understand Bronze → Silver streaming architecture

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

### AvailableNow

```text
Process currently available data
        ↓
Update checkpoint
        ↓
Stop
```

### Checkpoint

```text
Tracks streaming progress
```

### foreachBatch

```text
Micro-batch
    ↓
Normal DataFrame
    ↓
Batch-style operations
```

### Delta MERGE

```text
Existing record
    ↓
UPDATE

New record
    ↓
INSERT
```

---

# Day 12 Final Mental Model

```text
                 NEW CSV FILE
                       │
                       ▼
                  AUTO LOADER
                       │
                       ▼
              STRUCTURED STREAMING
                       │
                       ▼
                 AVAILABLE NOW
                       │
                       ▼
                  MICRO-BATCH
                       │
                       ▼
                    batch_df
                       │
                       ▼
                  foreachBatch
                       │
              ┌────────┴────────┐
              ▼                 ▼
       Deduplication      Business Logic
              │                 │
              └────────┬────────┘
                       ▼
                     MERGE
                       │
                       ▼
                    SILVER
```

## Day 12 Outcome

You should now be able to explain:

> **Auto Loader incrementally discovers files. Structured Streaming processes them as micro-batches. `AvailableNow` processes currently available data and then stops. `foreachBatch` receives each micro-batch as a normal DataFrame, allowing batch-style operations such as deduplication and Delta MERGE. Checkpoints maintain incremental processing state.**

**Next: [Day 13](../Day13/README.md) - Complete Auto Loader → Bronze → `readStream` → `foreachBatch` → Deduplication → Conditional MERGE → Silver pipeline.**
