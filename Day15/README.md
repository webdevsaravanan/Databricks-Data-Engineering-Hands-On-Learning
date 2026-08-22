# Day 15 - Schema Evolution, Schema Detection, `_rescued_data` & Backfill

## Objective

Learn how production-style pipelines handle changes in the source schema.

Today we extend the Day 14 pipeline to cover:

- Schema evolution
- `_rescued_data`
- New columns
- Datatype mismatches
- Schema detection
- Schema-change logging
- Review / approval workflow
- Updating the Silver data model
- Historical backfill after a schema change
- Difference between schema change, datatype mismatch, and data quality

```text
Source
  ↓
Auto Loader
  ↓
Bronze
  ↓
Schema Detection
  ↓
Schema Change Detected
  ↓
Log / Review
  ↓
Approve
  ↓
Update Schema + Transformation
  ↓
Backfill Historical Data if Required
  ↓
Silver
```

---

## 1. Day 15 Scenario

Our existing customer schema is:

```text
CustomerId
CustomerName
City
Age
UpdatedAt
```

Now the source system starts sending:

```text
CustomerId
CustomerName
City
Age
UpdatedAt
Email ← NEW
```

Example:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt,Email
110,Arun,Chennai,30,2026-08-23 09:00:00,arun@gmail.com
111,Kumar,Bangalore,35,2026-08-23 09:05:00,kumar@gmail.com
```

The production question is not simply:

> "Can Auto Loader read this?"

The important question is:

> **"Is Email an approved change to our customer data model?"**

---

## 2. Production-Style Schema Change Flow

```text
                   SOURCE
                      │
                      ▼
                 AUTO LOADER
                      │
                      ▼
                    BRONZE
                      │
                      ▼
               SCHEMA DETECTION
                      │
              ┌───────┴────────┐
              │                │
        No schema change   Schema change
              │                │
              │                ▼
              │           Log change
              │                │
              │                ▼
              │          Review / Approve
              │                │
              │                ▼
              │        Update Silver schema
              │                │
              └────────┬───────┘
                       ▼
                    SILVER
                       │
                       ▼
               Backfill if required
```

---

## 3. Schema Evolution vs Data Quality

These are different problems.

### Data Quality

```text
Age = -5
```

The column exists, but the value is invalid.

### Schema Evolution

```text
Email ← NEW COLUMN
```

The source structure has changed.

### Datatype Mismatch

```text
Age = ABC
```

The existing column is present, but the incoming value doesn't match the expected datatype.

Therefore:

```text
Schema Evolution
        ≠
Datatype Mismatch
        ≠
Data Quality
```

---

## 4. Current Explicit Schema

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

There is no `Email`.

---

## 5. Create Day 15 Source Directory

Use:

```text
/Volumes/workspace/bronze/day15_customer_files/
```

Create `customers_05.csv`:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt,Email
110,Arun,Chennai,30,2026-08-23 09:00:00,arun@gmail.com
111,Kumar,Bangalore,35,2026-08-23 09:05:00,kumar@gmail.com
```

---

## 6. Auto Loader with `_rescued_data`

```python
bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(customer_schema)
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/bronze/schema/day15_customers/")
    .load("/Volumes/workspace/bronze/day15_customer_files/")
)
```

`_rescued_data` acts as a safety mechanism for data that cannot be mapped normally to the expected schema.

---

## 7. Inspect the Incoming Schema

```python
bronze_stream.printSchema()
```

Also:

```python
display(bronze_stream)
```

Investigate:

```text
Email
_rescued_data
```

Do not assume every new column automatically becomes a normal column. Observe the actual result from your Auto Loader configuration.

---

## 8. Write to Bronze

Use `availableNow=True` for the Free Edition workflow:

```python
bronze_query = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/workspace/bronze/checkpoints/day15_bronze/")
    .trigger(availableNow=True)
    .toTable("bronze.day15_customers")
)
```

Wait:

```python
bronze_query.awaitTermination()
```

Check:

```sql
SELECT *
FROM bronze.day15_customers
ORDER BY CustomerId;
```

---

# Part A - Schema Detection

## 9. Why Detect Schema Changes?

Suppose expected schema is:

```text
CustomerId
CustomerName
City
Age
UpdatedAt
```

Incoming source contains:

```text
CustomerId
CustomerName
City
Age
UpdatedAt
Email
```

We want to detect:

```text
NEW COLUMN
---------
Email
```

Production thinking:

```text
Unexpected source change
        ↓
Detect
        ↓
Log
        ↓
Review
        ↓
Approve / Reject
```

---

## 10. Basic Schema Comparison

For learning, simulate schema detection with Python.

Expected columns:

```python
expected_columns = {
    "CustomerId",
    "CustomerName",
    "City",
    "Age",
    "UpdatedAt"
}
```

Incoming columns:

```python
incoming_columns = set(bronze_stream.columns)
```

Find new columns:

```python
new_columns = incoming_columns - expected_columns
```

Check:

```python
print(new_columns)
```

Conceptually:

```text
{'Email'}
```

---

## 11. Detect Removed Columns

```python
removed_columns = expected_columns - incoming_columns
```

For example, if `UpdatedAt` disappears:

```text
Removed:
UpdatedAt
```

A removed column can be serious because downstream transformations may fail.

---

## 12. Detect Added and Removed Columns Together

```python
new_columns = incoming_columns - expected_columns
removed_columns = expected_columns - incoming_columns

print("New columns:", new_columns)
print("Removed columns:", removed_columns)
```

```text
Schema Detection
       │
 ┌─────┴─────┐
 ▼           ▼
Added      Removed
columns    columns
```

---

## 13. Create Schema Change Log Table

```sql
CREATE TABLE IF NOT EXISTS silver.day15_schema_change_log (
    detected_at TIMESTAMP,
    source_name STRING,
    change_type STRING,
    column_name STRING,
    status STRING,
    details STRING
)
USING DELTA;
```

Example:

```text
detected_at   = 2026-08-23
source_name   = customer_csv
change_type   = NEW_COLUMN
column_name   = Email
status        = PENDING_REVIEW
details       = New source column detected
```

---

## 14. Record the Schema Change

For the learning exercise:

```python
for column_name in new_columns:
    print(f"Schema change detected: {column_name}")
```

Production concept:

```text
Source
  ↓
Schema comparison
  ↓
Schema change event
  ↓
Schema Change Log
```

A production implementation may use orchestration/monitoring around the ingestion pipeline rather than a simple Python loop.

---

## 15. Review the Schema Change

Suppose we detect:

```text
Schema Change
Source: customer_csv
New Column: Email
```

Ask:

> Is Email an approved business attribute?

### If NO

```text
Bronze
   ↓
Preserve unexpected information
   ↓
Silver ignores Email
```

### If YES

```text
Approve
   ↓
Update schema
   ↓
Update Silver
   ↓
Update transformation
   ↓
Test
   ↓
Deploy
```

---

# Part B - Approve the New Column

## 16. Update the Silver Schema

Suppose the business approves `Email`.

```sql
ALTER TABLE silver.day15_customers
ADD COLUMNS (Email STRING);
```

Silver now contains:

```text
CustomerId
CustomerName
City
Age
UpdatedAt
Email
```

---

## 17. Update the Transformation

Intentionally include Email:

```python
select(
    "CustomerId",
    "CustomerName",
    "City",
    "Age",
    "UpdatedAt",
    "Email"
)
```

The source change is now intentionally incorporated into the Silver business model.

---

## 18. Update the MERGE

Add Email to the matched update:

```python
.whenMatchedUpdate(
    condition="source.UpdatedAt > target.UpdatedAt",
    set={
        "CustomerName": "source.CustomerName",
        "City": "source.City",
        "Age": "source.Age",
        "UpdatedAt": "source.UpdatedAt",
        "Email": "source.Email"
    }
)
```

And to insert:

```python
.whenNotMatchedInsert(
    values={
        "CustomerId": "source.CustomerId",
        "CustomerName": "source.CustomerName",
        "City": "source.City",
        "Age": "source.Age",
        "UpdatedAt": "source.UpdatedAt",
        "Email": "source.Email"
    }
)
```

---

# Part C - Backfill Scenario

## 19. Why Is Backfill Needed?

Suppose Email is introduced today.

Today's records:

```text
110 → Email available
111 → Email available
```

But historical records also need Email.

The business says:

> "Populate Email for historical customers too."

Updating the schema alone is not enough.

We need:

```text
Historical Bronze
       ↓
Reprocess
       ↓
Transform
       ↓
Validate
       ↓
Deduplicate
       ↓
Silver
```

This is a **backfill**.

---

## 20. Normal Processing vs Backfill

### Normal incremental processing

```text
New files
   ↓
Bronze
   ↓
Silver
```

### Backfill

```text
Existing historical data
          ↓
      Reprocess
          ↓
        Silver
```

---

## 21. Backfill Scenario

Suppose a historical source file contains:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt,Email
101,Arun,Chennai,30,2026-08-20 09:00:00,arun@gmail.com
102,Kumar,Bangalore,35,2026-08-20 09:05:00,kumar@gmail.com
```

We want to populate:

```text
silver.day15_customers.Email
```

for existing customers.

---

## 22. Define a Backfill Window

Never blindly reprocess everything.

Example:

```text
Start date = 2026-08-01
End date   = 2026-08-22
```

The exact range should be business-approved.

Then:

```text
Historical source
       ↓
Filter required period
       ↓
Transform
       ↓
Validate
       ↓
Deduplicate
       ↓
MERGE
```

---

## 23. Read Historical Data

For the learning exercise:

```python
historical_df = spark.table("bronze.day15_customers")
```

Filter the approved backfill period:

```python
from pyspark.sql.functions import col

backfill_df = (
    historical_df
    .filter(col("UpdatedAt") < "2026-08-23")
)
```

In a production pipeline, the exact source, date range, and selection criteria should be explicitly controlled.

---

## 24. Apply Data Quality During Backfill

Backfill data should use the same validation rules as normal processing.

```text
Historical data
       ↓
Data Quality
       ↓
 ┌─────┴─────┐
 ▼           ▼
Valid      Invalid
 │           │
 ▼           ▼
Silver    Quarantine
```

Do not assume historical data is automatically clean.

---

## 25. Deduplicate Historical Data

Suppose:

```text
CustomerId | UpdatedAt
-----------|----------
101        | 2026-08-01
101        | 2026-08-10
101        | 2026-08-20
```

Keep the latest record:

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window_spec = (
    Window
    .partitionBy("CustomerId")
    .orderBy(col("UpdatedAt").desc())
)

latest_backfill = (
    backfill_df
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)
```

---

## 26. MERGE Backfill into Silver

Use the same business key:

```text
CustomerId
```

and the same rule:

```text
source.UpdatedAt > target.UpdatedAt
```

Conceptually:

```text
Historical Bronze
       ↓
Latest valid customer
       ↓
MERGE
       ↓
Silver
```

This keeps the backfill consistent with normal incremental processing.

---

## 27. Backfill vs Full Reload

### Full reload

```text
Delete/recreate
      ↓
Process everything
```

### Backfill

```text
Existing data
      ↓
Process a specific historical range
      ↓
MERGE into existing Silver
```

Backfill is controlled historical reprocessing.

---

# Part D - Datatype Mismatch

## 28. Test Invalid Datatype

Create `customers_06.csv`:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
112,Ravi,Chennai,ABC,2026-08-23 10:00:00
```

Schema expects:

```python
StructField("Age", IntegerType(), True)
```

but source contains:

```text
ABC
```

Investigate:

```sql
SELECT
    CustomerId,
    Age,
    _rescued_data
FROM bronze.day15_customers
WHERE CustomerId = 112;
```

Check whether the original problematic value has been preserved in `_rescued_data`.

---

# Part E - Data Quality Difference

## 29. Test Invalid Business Value

Create `customers_07.csv`:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
113,Mani,Chennai,-5,2026-08-23 10:05:00
```

`-5` is a valid integer.

Therefore:

```text
Schema:
    VALID

Datatype:
    VALID

Business rule:
    INVALID
```

Day 14 should send this record to Quarantine.

---

## 30. Three Problems Compared

| Scenario | Problem | Handling |
|---|---|---|
| `Email` appears | Schema evolution | Detect → review → approve/evolve |
| `Age = ABC` | Datatype mismatch | Rescue/ingestion handling → investigate |
| `Age = -5` | Data quality | Quarantine |
| Historical Email required | Backfill | Reprocess approved historical range |

Remember:

```text
Email = new column
       ↓
Schema Change

Age = ABC
       ↓
Datatype Problem

Age = -5
       ↓
Business/Data Quality Problem

Historical Email needed
       ↓
Backfill
```

---

# 31. Production-Style Architecture

```text
                         SOURCE
                           │
                           ▼
                      AUTO LOADER
                           │
                           ▼
                         BRONZE
                           │
                 ┌─────────┴─────────┐
                 │                   │
             Normal data       Schema change
                 │                   │
                 │                   ▼
                 │              DETECTION
                 │                   │
                 │             ┌─────┴─────┐
                 │             ▼           ▼
                 │          Reject       Approve
                 │                         │
                 │                         ▼
                 │                  Schema Evolution
                 │                         │
                 └──────────────┬──────────┘
                                ▼
                         DATA QUALITY
                                │
                         ┌──────┴──────┐
                         ▼             ▼
                       VALID        INVALID
                         │             │
                         ▼             ▼
                   DEDUPLICATE     QUARANTINE
                         │
                         ▼
                  CONDITIONAL MERGE
                         │
                         ▼
                       SILVER
                         ▲
                         │
                       BACKFILL
                         │
                  Historical Bronze
```

---

# 32. Day 15 Checklist

- [ ] Create a CSV with a new column
- [ ] Configure Auto Loader with explicit schema
- [ ] Enable `_rescued_data`
- [ ] Inspect the incoming schema
- [ ] Inspect Bronze
- [ ] Understand schema evolution
- [ ] Detect new columns
- [ ] Detect removed columns
- [ ] Create schema-change log table
- [ ] Record a schema-change event
- [ ] Understand review/approval
- [ ] Approve `Email`
- [ ] Add `Email` to Silver
- [ ] Update the transformation
- [ ] Update the MERGE
- [ ] Understand why backfill is required
- [ ] Define a historical backfill window
- [ ] Read historical Bronze data
- [ ] Apply data-quality rules during backfill
- [ ] Deduplicate historical records
- [ ] MERGE historical data into Silver
- [ ] Test datatype mismatch
- [ ] Inspect `_rescued_data`
- [ ] Test invalid business value
- [ ] Understand Schema Evolution vs Data Quality vs Datatype Mismatch
- [ ] Understand incremental processing vs backfill

---

# Key Learnings

### Schema Detection

```text
Expected Schema
      ↓
Compare with incoming
      ↓
Detect Added / Removed columns
```

### Schema Change

```text
Detected
   ↓
Review
   ↓
Approve
   ↓
Evolve
```

### `_rescued_data`

```text
Unexpected / unmappable data
        ↓
Preserve for investigation
```

### Data Quality

```text
Valid datatype
      +
Invalid business value
      ↓
Quarantine
```

### Backfill

```text
Historical data
      ↓
Reprocess selected range
      ↓
Validate
      ↓
Deduplicate
      ↓
MERGE
      ↓
Silver
```

### Bronze

```text
Source-oriented
Historical
Raw
```

### Silver

```text
Business-oriented
Controlled schema
Validated
```

---

# Day 15 Final Mental Model

```text
                     SOURCE
                       │
                       ▼
                  AUTO LOADER
                       │
                       ▼
                     BRONZE
                       │
             ┌─────────┴─────────┐
             ▼                   ▼
       Normal Data          Schema Change
             │                   │
             │              Detection
             │                   │
             │              Review/Approve
             │                   │
             │             Schema Evolution
             │                   │
             └──────────┬────────┘
                        ▼
                  DATA QUALITY
                        │
                 ┌──────┴──────┐
                 ▼             ▼
               VALID        INVALID
                 │             │
                 ▼             ▼
            DEDUPLICATE    QUARANTINE
                 │
                 ▼
          CONDITIONAL MERGE
                 │
                 ▼
               SILVER
                 ▲
                 │
              BACKFILL
                 │
          Historical Data
```

## Day 15 Outcome

You should now be able to explain:

> **A production pipeline should not blindly expose every new source column in Silver. The ingestion layer detects or tracks schema changes, unexpected data can be preserved through `_rescued_data`, and the engineering team can review and approve legitimate schema changes before updating the Silver data model. When a newly approved attribute is also required for historical records, a controlled backfill can reprocess the required historical range using the same validation, deduplication, and MERGE rules.**

**Next: Day 16 - Advanced Delta Lake: SCD Type 1 and SCD Type 2.**
