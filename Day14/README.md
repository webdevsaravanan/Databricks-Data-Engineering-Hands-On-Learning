# Day 14 - Data Quality & Quarantine

## Objective

Learn how to handle bad data in a real Data Engineering pipeline instead of silently allowing invalid records into Silver.

```text
CSV Files
    ↓
Auto Loader
    ↓
Structured Streaming
    ↓
🥉 Bronze
    ↓
Data Quality Rules
    ↓
 ┌───────────────┐
 │               │
VALID          INVALID
 │               │
 ↓               ↓
Silver       Quarantine
```

Today we learn data-quality rules, `is_valid`, `dq_reason`, `lit()`, `concat_ws()`, valid/invalid splitting, quarantine tables, deduplication, and conditional Delta `MERGE`.

---

## 1. Day 14 Scenario

We continue the RetailMart customer pipeline.

### customers_04.csv

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
105,Meena,Madurai,29,2026-08-22 09:00:00
106,,Chennai,32,2026-08-22 09:05:00
107,Ravi,Coimbatore,-5,2026-08-22 09:10:00
108,Divya,,27,2026-08-22 09:15:00
109,Karthik,Salem,35,2026-08-22 09:20:00
```

Expected:

```text
105 → VALID
106 → INVALID → CustomerName missing
107 → INVALID → Age invalid
108 → INVALID → City missing
109 → VALID
```

---

## 2. Data Quality Rules

| Column | Rule |
|---|---|
| `CustomerId` | Must not be NULL |
| `CustomerName` | Must not be NULL |
| `City` | Must not be NULL |
| `Age` | Must be greater than 0 |
| `UpdatedAt` | Must be a valid timestamp |

We create:

```text
is_valid
dq_reason
```

---

## 3. Create Source Directory

```text
/Volumes/workspace/bronze/day14_customer_files/
```

Place `customers_04.csv` there.

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
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/bronze/schema/day14_customers/")
    .load("/Volumes/workspace/bronze/day14_customer_files/")
)
```

---

## 6. Write to Bronze

For Databricks Free Edition use:

```python
.trigger(availableNow=True)
```

Bronze query:

```python
bronze_query = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/workspace/bronze/checkpoints/day14_bronze/")
    .trigger(availableNow=True)
    .toTable("bronze.day14_customers")
)
```

Wait:

```python
bronze_query.awaitTermination()
```

Check:

```sql
SELECT *
FROM bronze.day14_customers
ORDER BY CustomerId;
```

Bronze should still contain invalid records. We do not silently discard them during ingestion.

---

## 7. Create Silver and Quarantine Tables

```sql
CREATE TABLE IF NOT EXISTS silver.day14_customers (
    CustomerId INT,
    CustomerName STRING,
    City STRING,
    Age INT,
    UpdatedAt TIMESTAMP
)
USING DELTA;
```

```sql
CREATE TABLE IF NOT EXISTS silver.day14_customer_quarantine (
    CustomerId INT,
    CustomerName STRING,
    City STRING,
    Age INT,
    UpdatedAt STRING,
    dq_reason STRING,
    quarantined_at TIMESTAMP
)
USING DELTA;
```

---

## 8. Read Bronze as Streaming

```python
silver_stream = spark.readStream.table("bronze.day14_customers")
```

This gives us incremental micro-batches from Bronze.

---

## 9. Understanding lit()

`lit()` creates a fixed value as a Spark Column expression.

```python
from pyspark.sql.functions import lit
```

Example:

```python
lit("CustomerName is NULL")
```

In:

```python
when(col("CustomerName").isNull(), lit("CustomerName is NULL"))
```

the meaning is:

```text
IF CustomerName IS NULL
    ↓
return "CustomerName is NULL"
```

Remember:

```text
col() → Existing DataFrame column
lit() → Fixed value
```

---

## 10. Understanding concat_ws()

`concat_ws()` means concatenate strings **with a separator**.

`ws` = **with separator**.

```python
from pyspark.sql.functions import concat_ws
```

Example:

```python
concat_ws("; ", col("Error1"), col("Error2"))
```

produces:

```text
Error 1; Error 2
```

For data quality, it lets us combine multiple failure reasons:

```text
CustomerName is NULL; City is NULL; Age must be greater than 0
```

`concat_ws()` ignores NULL values, which is useful because only failed rules need to contribute a message.

---

## 11. Create Data Quality Function

```python
def process_customer_quality(batch_df, batch_id):

    from pyspark.sql.functions import (
        col, to_timestamp, when, lit,
        current_timestamp, concat_ws, row_number
    )
    from pyspark.sql.window import Window
    from delta.tables import DeltaTable

    print(f"Processing batch: {batch_id}")

    batch_df = batch_df.withColumn(
        "UpdatedAtParsed",
        to_timestamp(col("UpdatedAt"))
    )

    valid_condition = (
        col("CustomerId").isNotNull()
        & col("CustomerName").isNotNull()
        & col("City").isNotNull()
        & col("Age").isNotNull()
        & (col("Age") > 0)
        & col("UpdatedAtParsed").isNotNull()
    )

    batch_df = batch_df.withColumn("is_valid", valid_condition)

    batch_df = batch_df.withColumn(
        "dq_reason",
        concat_ws(
            "; ",
            when(col("CustomerId").isNull(), lit("CustomerId is NULL")),
            when(col("CustomerName").isNull(), lit("CustomerName is NULL")),
            when(col("City").isNull(), lit("City is NULL")),
            when(col("Age").isNull(), lit("Age is NULL")),
            when(col("Age") <= 0, lit("Age must be greater than 0")),
            when(col("UpdatedAtParsed").isNull(), lit("UpdatedAt is invalid"))
        )
    )

    valid_df = (
        batch_df
        .filter(col("is_valid"))
        .withColumn("UpdatedAt", col("UpdatedAtParsed"))
        .drop("UpdatedAtParsed", "is_valid", "dq_reason")
    )

    invalid_df = (
        batch_df
        .filter(~col("is_valid"))
        .withColumn("quarantined_at", current_timestamp())
        .select(
            "CustomerId", "CustomerName", "City", "Age",
            "UpdatedAt", "dq_reason", "quarantined_at"
        )
    )

    invalid_df.write.mode("append").saveAsTable("silver.day14_customer_quarantine")

    window_spec = (
        Window
        .partitionBy("CustomerId")
        .orderBy(col("UpdatedAt").desc())
    )

    latest_valid = (
        valid_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    silver_table = DeltaTable.forName(
        batch_df.sparkSession,
        "silver.day14_customers"
    )

    (
        silver_table.alias("target")
        .merge(
            latest_valid.alias("source"),
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

## 12. Start the Streaming Pipeline

```python
quality_query = (
    silver_stream.writeStream
    .foreachBatch(process_customer_quality)
    .option("checkpointLocation", "/Volumes/workspace/bronze/checkpoints/day14_quality/")
    .trigger(availableNow=True)
    .start()
)
```

Wait:

```python
quality_query.awaitTermination()
```

---

## 13. Check Silver

```sql
SELECT *
FROM silver.day14_customers
ORDER BY CustomerId;
```

Expected valid records:

```text
105 | Meena   | Madurai | 29
109 | Karthik | Salem   | 35
```

Invalid records should not enter Silver.

---

## 14. Check Quarantine

```sql
SELECT *
FROM silver.day14_customer_quarantine
ORDER BY CustomerId;
```

Expected:

```text
106 | NULL  | Chennai    | 32 | CustomerName is NULL
107 | Ravi  | Coimbatore | -5 | Age must be greater than 0
108 | Divya | NULL       | 27 | City is NULL
```

Now we know which record failed and why.

---

## 15. Multiple Data Quality Failures

Input:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
110,,,-5,invalid-date
```

Possible `dq_reason`:

```text
CustomerName is NULL; City is NULL; Age must be greater than 0; UpdatedAt is invalid
```

This is more useful than simply storing:

```text
is_valid = false
```

---

## 16. Why Quarantine Instead of Dropping?

Bad approach:

```python
df.filter(valid_condition)
```

and silently ignore the invalid records.

Better:

```text
Invalid
   ↓
Quarantine
   ↓
Investigate
   ↓
Fix source/process
   ↓
Reprocess if required
```

This preserves visibility of bad data.

---

## 17. Bronze vs Quarantine

### Bronze

```text
Bronze
 ├── valid
 ├── invalid
 ├── duplicates
 └── older records
```

Bronze preserves what arrived from the source.

### Quarantine

```text
Quarantine
 ├── missing CustomerName
 ├── invalid Age
 ├── missing City
 └── invalid UpdatedAt
```

Quarantine contains records that failed our validation rules.

> **Bronze preserves what arrived. Quarantine identifies what failed validation.**

---

## 18. Day 13 vs Day 14

### Day 13

```text
Bronze
  ↓
Deduplication
  ↓
Conditional MERGE
  ↓
Silver
```

### Day 14

```text
Bronze
  ↓
Data Quality
  ↓
 ┌─────────────┐
 │             │
Valid        Invalid
 │             │
 ↓             ↓
Dedup       Quarantine
 │
 ↓
Conditional MERGE
 │
 ↓
Silver
```

The major addition is:

```text
Data Quality
      ↓
Quarantine
```

---

## 19. Complete Day 14 Architecture

```text
                         CSV FILES
                             │
                             ▼
                       AUTO LOADER
                             │
                             ▼
                     STRUCTURED STREAMING
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
                    DATA QUALITY RULES
                             │
                    ┌────────┴────────┐
                    ▼                 ▼
                 VALID             INVALID
                    │                 │
                    ▼                 ▼
             DEDUPLICATION        dq_reason
                    │                 │
                    ▼                 ▼
             CONDITIONAL MERGE   QUARANTINE
                    │
                    ▼
                  SILVER
```

---

## 20. Day 14 Checklist

- [ ] Create `customers_04.csv`
- [ ] Configure Auto Loader
- [ ] Write data to Bronze
- [ ] Create Silver table
- [ ] Create Quarantine table
- [ ] Read Bronze using `readStream`
- [ ] Use `foreachBatch`
- [ ] Understand `lit()`
- [ ] Understand `concat_ws()`
- [ ] Validate NULL values
- [ ] Validate Age
- [ ] Validate UpdatedAt
- [ ] Create `is_valid`
- [ ] Create `dq_reason`
- [ ] Split valid and invalid records
- [ ] Write invalid records to Quarantine
- [ ] Deduplicate valid records
- [ ] Perform conditional MERGE into Silver
- [ ] Verify invalid records don't enter Silver
- [ ] Verify invalid records exist in Quarantine
- [ ] Understand Bronze vs Quarantine

---

# Key Learnings

### Data Quality

```text
Validate incoming records
```

### `lit()`

```text
Fixed value
    ↓
Spark Column expression
```

### `concat_ws()`

```text
Multiple strings
    ↓
Join with separator
    ↓
One error message
```

### `is_valid`

```text
true  → Good record
false → Bad record
```

### `dq_reason`

```text
Why did the record fail?
```

### Quarantine

```text
Bad data
   ↓
Separate table
   ↓
Investigation / reprocessing
```

### Bronze

```text
Preserve source data
```

### Silver

```text
Clean and validated data
```

### Conditional MERGE

```text
Update only when source is newer
```

---

# Day 14 Final Mental Model

```text
                    CSV
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
             DATA QUALITY RULES
                     │
              ┌──────┴──────┐
              ▼             ▼
           VALID          INVALID
              │             │
              ▼             ▼
       DEDUPLICATION    dq_reason
              │             │
              ▼             ▼
       CONDITIONAL       QUARANTINE
          MERGE              │
              │              ▼
              ▼        Investigation
           SILVER
```

## Day 14 Outcome

You should now be able to explain:

> **Bronze preserves the incoming source data. During Silver processing, Data Quality rules identify valid and invalid records. Valid records are deduplicated and conditionally MERGED into Silver, while invalid records are written to a quarantine table with the reason for failure.**

**Next: Day 15 - Schema Evolution & `_rescued_data`: handling new columns, unexpected columns, datatype mismatches, and evolving source schemas.**
