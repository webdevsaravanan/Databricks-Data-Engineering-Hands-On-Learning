# Day 19 — Auto Loader + Structured Streaming + Data Quality

## 🎯 Objective

Build a production-style Databricks streaming pipeline combining Auto Loader, Structured Streaming, Bronze Delta, `foreachBatch`, data-quality validation, quarantine, Delta MERGE, checkpoints, and monitoring.

```text
CSV Files
   ↓
Auto Loader
   ↓
Structured Streaming
   ↓
Bronze Delta
   ↓
readStream
   ↓
foreachBatch()
   ↓
Data Quality
   ├── Valid → MERGE → Silver
   └── Invalid → Quarantine
   ↓
Data Quality Metrics
```

---

# 1. Topics Covered

- Auto Loader and `cloudFiles`
- Explicit schema
- Structured Streaming
- `readStream` and `writeStream`
- Bronze Delta table
- Checkpoints
- `trigger(availableNow=True)`
- Streaming micro-batches
- `foreachBatch`
- `batch_df` and `batch_id`
- Reusable data-quality validation
- Duplicate detection
- `ErrorReason`
- Valid vs invalid records
- Quarantine table
- Delta `MERGE`
- Data-quality metrics
- Quality thresholds
- Incremental file processing
- Checkpoint restart
- File-level vs business-level deduplication
- Reprocessing corrected records

---

# 2. Architecture

```text
                         SOURCE FILES
                              |
                              v
                         AUTO LOADER
                              |
                              v
                    STRUCTURED STREAMING
                              |
                              v
                         BRONZE DELTA
                              |
                              v
                          READSTREAM
                              |
                              v
                         FOREACHBATCH
                              |
                              v
                     DATA QUALITY CHECK
                       /             \
                      /               \
                     v                 v
                  VALID             INVALID
                    |                 |
                    v                 v
                 MERGE           QUARANTINE
                    |                 |
                    v                 v
                 SILVER          ErrorReason
                                      |
                                      v
                                  INVESTIGATE
                                      |
                                      v
                                  REPROCESS

                         +
                         |
                         v
                    DQ METRICS
                         |
                         v
                    MONITORING
```

---

# 3. Prerequisites

Before starting Day 19, you should understand:

- PySpark DataFrames
- Explicit schemas
- Delta tables
- Delta `MERGE`
- `readStream` / `writeStream`
- `foreachBatch`
- Data-quality validation
- Quarantine concepts

---

# 4. Create Folder Structure

Example Unity Catalog Volume structure:

```text
/Volumes/workspace/bronze/day19/
│
├── input/
│
└── checkpoint/
    ├── bronze/
    └── silver/
```

Use separate checkpoint locations for separate streaming queries.

---

# 5. Create Test CSV File

Create `day19_customers_01.csv`:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
201,Ravi,Chennai,28,2026-08-31 09:00:00
202,Arun,Bangalore,35,2026-08-31 09:01:00
203,,Chennai,30,2026-08-31 09:02:00
204,Priya,,25,2026-08-31 09:03:00
205,Kumar,Chennai,-5,2026-08-31 09:04:00
206,John,Chennai,150,2026-08-31 09:05:00
207,Meena,Madurai,29,2026-08-31 09:06:00
208,Anitha,Chennai,32,2026-08-31 09:07:00
208,Anitha,Chennai,32,2026-08-31 09:08:00
```

Copy it to:

```text
/Volumes/workspace/bronze/day19/input/
```

Intentional problems:

| CustomerId | Problem |
|---:|---|
| 203 | CustomerName is NULL |
| 204 | City is NULL |
| 205 | Age is below valid range |
| 206 | Age is above valid range |
| 208 | Duplicate CustomerId |

---

# 6. Define Explicit Schema

```python
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType
)

customer_schema = StructType([
    StructField("CustomerId", IntegerType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("UpdatedAt", TimestampType(), True)
])
```

---

# 7. Read Files Using Auto Loader

```python
bronze_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(customer_schema)
    .load("/Volumes/workspace/bronze/day19/input/")
    .withColumn("SourceFile", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
)
```

Auto Loader is represented by:

```python
.format("cloudFiles")
```

while:

```python
spark.readStream
```

makes this a streaming DataFrame.

---

# 8. Create Bronze Delta Table

```sql
CREATE TABLE IF NOT EXISTS bronze.day19_customers
USING DELTA;
```

Bronze stores the incoming data before Silver-level validation and business processing.

---

# 9. Write Auto Loader Stream to Bronze

```python
bronze_checkpoint = "/Volumes/workspace/bronze/day19/checkpoint/bronze"

bronze_query = (
    bronze_stream_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", bronze_checkpoint)
    .trigger(availableNow=True)
    .toTable("bronze.day19_customers")
)
```

Verify:

```sql
SELECT *
FROM bronze.day19_customers
ORDER BY CustomerId;
```

---

# 10. Understand `availableNow=True`

For this exercise:

```python
.trigger(availableNow=True)
```

processes the currently available data and then stops.

Conceptually:

```text
Available files
      ↓
Process
      ↓
Process backlog
      ↓
Stop
```

This is convenient for hands-on testing and incremental scheduled processing.

---

# 11. Add a Second File

Create `day19_customers_02.csv`:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
209,Suresh,Chennai,40,2026-08-31 10:00:00
210,Divya,Coimbatore,27,2026-08-31 10:01:00
211,,Chennai,31,2026-08-31 10:02:00
212,Ramesh,Chennai,120,2026-08-31 10:03:00
```

Copy it to the same input directory and run the Bronze stream again using the same checkpoint.

```python
bronze_query = (
    bronze_stream_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", bronze_checkpoint)
    .trigger(availableNow=True)
    .toTable("bronze.day19_customers")
)
```

Verify:

```sql
SELECT *
FROM bronze.day19_customers
ORDER BY CustomerId;
```

---

# 12. Understand Checkpointing

The checkpoint stores streaming progress/state information.

```text
Input Files
    ↓
Auto Loader
    ↓
Checkpoint
    ↓
Bronze Delta
```

Use different checkpoint locations for different queries:

```text
checkpoint/bronze
checkpoint/silver
```

Do not use the same checkpoint directory for unrelated streaming queries.

---

# 13. Read Bronze as a Stream

```python
silver_stream_df = (
    spark.readStream
    .table("bronze.day19_customers")
)
```

Compare:

```python
spark.read.table(...)
```

with:

```python
spark.readStream.table(...)
```

The first creates a batch DataFrame; the second creates a streaming DataFrame.

---

# 14. Create Reusable Data Quality Validation

```python
def validate_customers(df):

    duplicate_ids = (
        df.groupBy("CustomerId")
          .count()
          .filter(F.col("count") > 1)
          .select("CustomerId")
          .withColumn("IsDuplicate", F.lit(True))
    )

    validated_df = (
        df.join(
            duplicate_ids,
            on="CustomerId",
            how="left"
        )
        .fillna({"IsDuplicate": False})
        .withColumn(
            "ErrorReason",
            F.concat_ws(
                "; ",
                F.when(
                    F.col("CustomerId").isNull(),
                    "CustomerId is NULL"
                ),
                F.when(
                    F.col("CustomerName").isNull() |
                    (F.trim(F.col("CustomerName")) == ""),
                    "CustomerName is NULL"
                ),
                F.when(
                    F.col("City").isNull() |
                    (F.trim(F.col("City")) == ""),
                    "City is NULL"
                ),
                F.when(
                    F.col("Age").isNull() |
                    (F.col("Age") < 0) |
                    (F.col("Age") > 100),
                    "Age is outside valid range"
                ),
                F.when(
                    F.col("UpdatedAt").isNull(),
                    "UpdatedAt is NULL"
                ),
                F.when(
                    F.col("IsDuplicate") == True,
                    "Duplicate CustomerId"
                )
            )
        )
    )

    valid_df = (
        validated_df
        .filter(
            F.col("ErrorReason").isNull() |
            (F.col("ErrorReason") == "")
        )
        .drop("IsDuplicate", "ErrorReason")
    )

    invalid_df = (
        validated_df
        .filter(
            F.col("ErrorReason").isNotNull() &
            (F.col("ErrorReason") != "")
        )
        .drop("IsDuplicate")
        .withColumn(
            "RejectedAt",
            F.current_timestamp()
        )
    )

    return valid_df, invalid_df
```

---

# 15. Create Silver Table

```sql
CREATE TABLE IF NOT EXISTS silver.day19_customers (
    CustomerId INT,
    CustomerName STRING,
    City STRING,
    Age INT,
    UpdatedAt TIMESTAMP
)
USING DELTA;
```

---

# 16. Create Quarantine Table

```sql
CREATE TABLE IF NOT EXISTS silver.day19_customer_quarantine (
    BatchId STRING,
    CustomerId INT,
    CustomerName STRING,
    City STRING,
    Age INT,
    UpdatedAt TIMESTAMP,
    ErrorReason STRING,
    RejectedAt TIMESTAMP,
    SourceFile STRING
)
USING DELTA;
```

---

# 17. Create Data Quality Metrics Table

```sql
CREATE TABLE IF NOT EXISTS silver.day19_data_quality_metrics (
    BatchId STRING,
    TotalRecords INT,
    ValidRecords INT,
    InvalidRecords INT,
    QualityPercentage DOUBLE,
    ProcessedAt TIMESTAMP
)
USING DELTA;
```

---

# 18. Create `foreachBatch` Function

```python
from delta.tables import DeltaTable

def process_silver(batch_df, batch_id):

    print(f"Processing batch: {batch_id}")

    if batch_df.isEmpty():
        return

    batch_df=batch_df.drop( "_ingested_at")

    valid_df, invalid_df = validate_customers(batch_df)

    invalid_df=invalid_df.withColumn("BatchId", F.lit(batch_id ))

    invalid_df.write.mode("append").saveAsTable("silver.day19_customer_quarantine")

    silver_table = DeltaTable.forName(
        spark,
        "silver.day19_customers"
    )

    (
        silver_table.alias("target")
        .merge(
            valid_df.alias("source"),
            "target.CustomerId = source.CustomerId"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    total_count = batch_df.count()
    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    quality_percentage = (
        valid_count / total_count * 100
        if total_count > 0
        else 0
    )

    print(f"Batch ID        : {batch_id}")
    print(f"Total Records   : {total_count}")
    print(f"Valid Records   : {valid_count}")
    print(f"Invalid Records : {invalid_count}")
    print(f"Quality         : {quality_percentage:.2f}%")

    metrics_df = spark.createDataFrame(
        [
            (
                str(batch_id),
                total_count,
                valid_count,
                invalid_count,
                quality_percentage
            )
        ],
        [
            "BatchId",
            "TotalRecords",
            "ValidRecords",
            "InvalidRecords",
            "QualityPercentage"
        ]
    ).withColumn(
        "ProcessedAt",
        F.current_timestamp()
    )

    metrics_df.write.mode("append").saveAsTable("silver.day19_data_quality_metrics")
```

---

# 19. Start Silver Structured Streaming

```python
silver_checkpoint = "/Volumes/workspace/bronze/day19/checkpoint/silver"

silver_query = (
    silver_stream_df
    .writeStream
    .foreachBatch(process_silver)
    .option("checkpointLocation", silver_checkpoint)
    .trigger(availableNow=True)
    .start()
)

silver_query.awaitTermination()
```

---

# 20. Understand the `foreachBatch` Flow

```text
Streaming DataFrame
       ↓
Micro-batch
       ↓
batch_df
       ↓
process_silver()
       ↓
Normal DataFrame operations
       ↓
Validation
       ↓
MERGE + Quarantine + Metrics
```

`batch_df` is the current micro-batch.

`batch_id` identifies the micro-batch.

---

# 21. Verify Silver

```sql
SELECT *
FROM silver.day19_customers
ORDER BY CustomerId;
```

For the first file, expected valid IDs are:

```text
201
202
207
```

Customer 208 is rejected because its CustomerId appears more than once in the batch.

---

# 22. Verify Quarantine

```sql
SELECT
    BatchId,
    CustomerId,
    CustomerName,
    City,
    Age,
    ErrorReason,
    RejectedAt,
    SourceFile
FROM silver.day19_customer_quarantine
ORDER BY CustomerId;
```

Invalid records should appear with their rejection reasons.

---

# 23. Verify Data Quality Metrics

```sql
SELECT *
FROM silver.day19_data_quality_metrics
ORDER BY ProcessedAt DESC;
```

The metrics table contains:

```text
BatchId
TotalRecords
ValidRecords
InvalidRecords
QualityPercentage
ProcessedAt
```

---

# 24. Analyze Rejection Reasons

```sql
SELECT
    ErrorReason,
    COUNT(*) AS RejectedRecords
FROM silver.day19_customer_quarantine
GROUP BY ErrorReason
ORDER BY RejectedRecords DESC;
```

This identifies the most common data-quality problems.

---

# 25. Add a Data Quality Threshold

```python
QUALITY_THRESHOLD = 80.0
```

Then:

```python
if quality_percentage < QUALITY_THRESHOLD:
    print(
        f"WARNING: Data quality {quality_percentage:.2f}% "
        f"is below threshold {QUALITY_THRESHOLD:.2f}%"
    )
else:
    print(
        f"Data quality {quality_percentage:.2f}% "
        f"is acceptable"
    )
```

A quality threshold acts as a data-quality gate.

---

# 26. Warning vs Hard Failure

### Warning

```python
if quality_percentage < QUALITY_THRESHOLD:
    print("WARNING")
```

The pipeline continues.

### Hard Failure

```python
if quality_percentage < QUALITY_THRESHOLD:
    raise ValueError(
        f"Data quality {quality_percentage:.2f}% "
        f"is below threshold"
    )
```

The pipeline fails.

The correct choice depends on the business impact of bad data.

---

# 27. Test an Existing Customer

Create `day19_customers_03.csv`:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
201,Ravi Kumar,Chennai,30,2026-08-31 13:00:00
213,Surya,Chennai,27,2026-08-31 13:01:00
214,,Madurai,28,2026-08-31 13:02:00
```

Copy it to the Auto Loader input directory.

Run Bronze again:

```python
bronze_query = (
    bronze_stream_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", bronze_checkpoint)
    .trigger(availableNow=True)
    .toTable("bronze.day19_customers")
)
```

Then run Silver again:

```python
silver_query = (
    silver_stream_df
    .writeStream
    .foreachBatch(process_silver)
    .option("checkpointLocation", silver_checkpoint)
    .trigger(availableNow=True)
    .start()
)

silver_query.awaitTermination()
```

---

# 28. Verify the Existing Customer

```sql
SELECT *
FROM silver.day19_customers
WHERE CustomerId = 201;
```

Customer 201 should contain the updated values.

The reason is:

```python
.merge(
    valid_df.alias("source"),
    "target.CustomerId = source.CustomerId"
)
.whenMatchedUpdateAll()
.whenNotMatchedInsertAll()
```

---

# 29. Verify the New Customer

```sql
SELECT *
FROM silver.day19_customers
WHERE CustomerId = 213;
```

Customer 213 should be inserted because it did not previously exist.

---

# 30. Verify the Invalid Customer

```sql
SELECT *
FROM silver.day19_customer_quarantine
WHERE CustomerId = 214;
```

Customer 214 should be quarantined because `CustomerName` is NULL.

---

# 31. Understand File-Level vs Business-Level Deduplication

Auto Loader tracks files, while your business logic needs to identify duplicate business records.

```text
File-level tracking
        ≠
Business-level deduplication
```

For example, a previously processed file copied under a different filename can be seen as a new file.

Therefore, the Silver layer still needs business-key logic such as:

```text
CustomerId
```

and Delta `MERGE`.

---

# 32. Checkpoint Restart Experiment

1. Run the Bronze stream.
2. Stop it.
3. Add another CSV file.
4. Restart using the same checkpoint.
5. Observe the processing behavior.

Use:

```python
.option(
    "checkpointLocation",
    bronze_checkpoint
)
```

The checkpoint allows the query to continue from its previous progress.

---

# 33. Reprocessing Corrected Data

Quarantine should not be treated as a dead end.

Suppose:

```text
CustomerId = 214
CustomerName = NULL
```

was rejected.

After correction:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
214,Arun,Chennai,28,2026-08-31 14:00:00
```

The corrected record can go through the same validation process.

```text
Quarantine
    ↓
Investigate
    ↓
Correct
    ↓
Reprocess
    ↓
Validate
    ↓
MERGE
    ↓
Silver
```

---

# 34. Why MERGE Matters During Reprocessing

If the business key already exists in Silver, blindly appending the corrected record can create duplicates.

Instead:

```text
Corrected Record
       ↓
     MERGE
       ↓
CustomerId exists?
    /         YES       NO
   |         |
UPDATE     INSERT
```

This is why Delta MERGE is useful for incremental and reprocessed data.

---

# 35. Production-Style Monitoring

The pipeline now has three important outputs:

```text
                    PROCESSING
                         |
            +------------+------------+
            |            |            |
            v            v            v
         SILVER      QUARANTINE    METRICS
            |            |            |
            v            v            v
         Trusted      Rejected      Quality
          Data          Data       Statistics
```

### Silver

Trusted records.

### Quarantine

Rejected records and rejection reasons.

### Metrics

Batch-level data-quality statistics.

---

# 36. Find Bad Batches

```sql
SELECT *
FROM silver.day19_data_quality_metrics
WHERE QualityPercentage < 80
ORDER BY ProcessedAt DESC;
```

This identifies batches whose data quality fell below the threshold.

---

# 37. Find Common Rejection Reasons

```sql
SELECT
    ErrorReason,
    SUM(RejectedRecords) AS TotalRejected
FROM (
    SELECT
        ErrorReason,
        COUNT(*) AS RejectedRecords
    FROM silver.day19_customer_quarantine
    GROUP BY ErrorReason
)
GROUP BY ErrorReason
ORDER BY TotalRejected DESC;
```

This helps identify recurring source-system problems.

---

# 38. Important Concept Comparison

| Concept | Main Purpose |
|---|---|
| Auto Loader | Discover new files efficiently |
| Structured Streaming | Process data incrementally |
| Checkpoint | Maintain streaming progress/state |
| `foreachBatch` | Apply batch-style logic to each micro-batch |
| Delta MERGE | Update existing records and insert new records |
| Quarantine | Preserve invalid records |
| Data Quality Metrics | Measure pipeline/data quality |
| Quality Threshold | Decide whether data quality is acceptable |

---

# 39. Day 19 End-to-End Flow

```text
CSV
 ↓
Auto Loader
 ↓
readStream
 ↓
writeStream
 ↓
Bronze Delta
 ↓
readStream
 ↓
foreachBatch(batch_df, batch_id)
 ↓
validate_customers()
 ↓
 ┌───────────────────┬────────────────────┐
 │                   │                    │
valid_df          invalid_df          metrics
 │                   │                    │
 ↓                   ↓                    ↓
Delta MERGE       Quarantine          Metrics
 │
 ↓
Silver
```

---

# 40. Day 19 Checklist

## Auto Loader

- [ ] Create Day 19 input folder
- [ ] Create CSV files
- [ ] Define explicit schema
- [ ] Use `cloudFiles`
- [ ] Read files using `readStream`
- [ ] Configure Auto Loader
- [ ] Write to Bronze Delta
- [ ] Configure checkpoint
- [ ] Use `availableNow=True`

## Structured Streaming

- [ ] Understand streaming DataFrame
- [ ] Understand micro-batches
- [ ] Read Bronze using `readStream`
- [ ] Configure Silver checkpoint
- [ ] Start Silver streaming query
- [ ] Use `awaitTermination()`

## `foreachBatch`

- [ ] Create `process_silver()`
- [ ] Understand `batch_df`
- [ ] Understand `batch_id`
- [ ] Apply normal DataFrame operations
- [ ] Perform validation
- [ ] Perform Delta MERGE
- [ ] Write quarantine records
- [ ] Write metrics

## Data Quality

- [ ] Validate CustomerId
- [ ] Validate CustomerName
- [ ] Validate City
- [ ] Validate Age
- [ ] Validate UpdatedAt
- [ ] Detect duplicate CustomerId
- [ ] Generate `ErrorReason`
- [ ] Separate valid and invalid records

## Monitoring

- [ ] Calculate total records
- [ ] Calculate valid records
- [ ] Calculate invalid records
- [ ] Calculate quality percentage
- [ ] Store metrics
- [ ] Define quality threshold
- [ ] Find bad batches
- [ ] Find common rejection reasons

## Incremental Processing

- [ ] Process first file
- [ ] Add second file
- [ ] Process new file incrementally
- [ ] Test existing CustomerId
- [ ] Test new CustomerId
- [ ] Test invalid record
- [ ] Test duplicate file
- [ ] Test checkpoint restart
- [ ] Test corrected/reprocessed record

---

# 41. Key Learnings

1. **Auto Loader** discovers new files arriving in cloud storage.

2. **Structured Streaming** processes data incrementally.

3. **Checkpoints** maintain streaming progress and state.

4. **`foreachBatch`** provides each micro-batch as a DataFrame for batch-style processing.

5. **Data-quality validation** separates trusted records from invalid records.

6. **Quarantine** preserves rejected data instead of silently dropping it.

7. **`ErrorReason`** explains why a record failed validation.

8. **Delta MERGE** handles both new records and updates to existing business keys.

9. **File-level tracking and business-level deduplication are different problems.**

10. **Data-quality metrics** allow engineers to monitor pipeline health over time.

11. **Quality thresholds** can warn or stop processing when quality falls below an acceptable level.

12. **Reprocessing** provides a controlled path for corrected records to return to Silver.

---

# 42. Final Mental Model

Remember this:

```text
                       NEW FILE
                           |
                           v
                     AUTO LOADER
                           |
                           v
                 STRUCTURED STREAMING
                           |
                           v
                      BRONZE DELTA
                           |
                           v
                       READSTREAM
                           |
                           v
                     FOREACHBATCH
                           |
                           v
                     VALIDATION
                           |
              +------------+------------+
              |                         |
              v                         v
            VALID                    INVALID
              |                         |
              v                         v
           MERGE                  QUARANTINE
              |                         |
              v                         v
           SILVER                 ErrorReason
                                        |
                                        v
                                  INVESTIGATION
                                        |
                                        v
                                   REPROCESSING

                       +
                       |
                       v
                  DQ METRICS
                       |
                       v
                   MONITORING
```

---

# 43. Outcome

After completing Day 19, you should be able to explain and implement:

```text
Files
  ↓
Auto Loader
  ↓
Bronze
  ↓
Structured Streaming
  ↓
foreachBatch
  ↓
Data Quality
  ├── Valid → MERGE → Silver
  └── Invalid → Quarantine
  ↓
Metrics / Monitoring
```

You have now connected the major concepts from the previous days into a realistic Databricks streaming pipeline.

## 🚀 Day 19 Complete

```text
Auto Loader
     +
Structured Streaming
     +
Bronze Delta
     +
foreachBatch
     +
Data Quality
     +
Quarantine
     +
Delta MERGE
     +
Checkpoints
     +
Monitoring
```

This is the foundation for moving from individual PySpark transformations toward production-style Databricks Data Engineering.
