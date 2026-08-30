# Day 17 — Streaming SCD Type 2 with Delta Lake

This hands-on continues Day 16. You will build a robust customer-history pipeline using CSV micro-batches, Delta Lake, Structured Streaming, and `foreachBatch`.

```text
CSV files in a Unity Catalog Volume
        ↓
Structured Streaming micro-batches
        ↓
Bronze Delta table (raw, auditable events)
        ↓
foreachBatch
        ↓
Silver Delta SCD Type 2 table (customer history)
```

## Learning goals

By the end of Day 17, you will be able to:

- ingest customer CSV changes as streaming micro-batches;
- retain raw input data in Bronze;
- classify new, changed, unchanged, and late records;
- close the prior SCD2 row and insert the new version atomically;
- remove duplicate events within a batch;
- safely re-run the stream without duplicate history; and
- validate SCD Type 2 history with SQL.

`foreachBatch` lets each micro-batch run Delta `MERGE` logic. It provides at-least-once semantics by default, so this lab also uses Delta transaction identifiers for idempotent Bronze writes and logically idempotent Silver merge logic. See [Databricks foreachBatch documentation](https://docs.databricks.com/aws/en/structured-streaming/foreach).

---

## Step 1 — Create lab storage and Delta tables

In a Databricks SQL cell, run the following. If `workspace.default` is not your available catalog and schema, replace it with one you can use from Catalog Explorer.

```sql
CREATE SCHEMA IF NOT EXISTS workspace.default;
CREATE VOLUME IF NOT EXISTS workspace.default.day17_lab;

CREATE TABLE IF NOT EXISTS workspace.default.day17_customer_bronze (
  CustomerId INT,
  CustomerName STRING,
  City STRING,
  Age INT,
  UpdatedAt TIMESTAMP,
  SourceFile STRING,
  IngestedAt TIMESTAMP,
  EventHash STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.default.day17_customer_scd2 (
  CustomerId INT,
  CustomerName STRING,
  City STRING,
  Age INT,
  ValidFrom TIMESTAMP,
  ValidTo TIMESTAMP,
  IsCurrent BOOLEAN,
  EventHash STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.default.day17_customer_late_events (
  CustomerId INT,
  CustomerName STRING,
  City STRING,
  Age INT,
  UpdatedAt TIMESTAMP,
  EventHash STRING,
  LateReason STRING,
  SourceFile STRING,
  RejectedAt TIMESTAMP
) USING DELTA;
```

Use this Volume path throughout the lab:

```text
/Volumes/workspace/default/day17_lab
```

Create an `incoming` folder beneath that Volume with Catalog Explorer. Upload the CSV files from later steps into this folder. Never reuse a filename after it has been processed.

---

## Step 2 — Upload the initial customer batch

Create a file named `day17_batch_01.csv`:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
101,Arun,Chennai,30,2026-08-01 09:00:00
102,Kumar,Bangalore,35,2026-08-01 09:05:00
103,Priya,Chennai,27,2026-08-01 09:10:00
```

Upload it to:

```text
/Volumes/workspace/default/day17_lab/incoming/
```

All three rows are initial versions, so all will become current records in the Silver SCD2 table.

---

## Step 3 — Define paths, table names, and schema

Run this Python cell in a notebook.

```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.window import Window

catalog_schema = "workspace.default"

incoming_path = "/Volumes/workspace/default/day17_lab/incoming"
checkpoint_path = "/Volumes/workspace/default/day17_lab/checkpoints/customer_scd2"

bronze_table = f"{catalog_schema}.day17_customer_bronze"
silver_table = f"{catalog_schema}.day17_customer_scd2"
late_table = f"{catalog_schema}.day17_customer_late_events"

customer_schema = StructType([
    StructField("CustomerId", IntegerType(), False),
    StructField("CustomerName", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("UpdatedAt", TimestampType(), False)
])
```

An explicit schema is better than `inferSchema` for streaming. It guarantees that `CustomerId`, `Age`, and `UpdatedAt` have the expected data types.

---

## Step 4 — Read CSV files as a stream

```python
raw_stream_df = (
    spark.readStream
    .format("csv")
    .schema(customer_schema)
    .option("header", "true")
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    .load(incoming_path)
)
```

For this lab, we will use `availableNow=True`. Each run processes every newly uploaded file, then stops. This works well for learning in Databricks Free Edition.

---

## Step 5 — Add audit fields and a change hash

```python
hash_expression = F.sha2(
    F.concat_ws(
        "||",
        F.coalesce(F.col("CustomerName"), F.lit("∅")),
        F.coalesce(F.col("City"), F.lit("∅")),
        F.coalesce(F.col("Age").cast("string"), F.lit("∅"))
    ),
    256
)

prepared_stream_df = (
    raw_stream_df
    .withColumn("SourceFile", F.input_file_name())
    .withColumn("IngestedAt", F.current_timestamp())
    .withColumn("EventHash", hash_expression)
)
```

The hash covers the tracked customer attributes: name, city, and age. A matching hash means no tracked attribute changed; a different hash means a new SCD2 version is needed.

---

## Step 6 — Implement the SCD Type 2 micro-batch function

Run this entire Python cell.

```python
def process_customer_scd2(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # Retain only the latest event for a customer within this micro-batch.
    latest_per_customer_window = (
        Window.partitionBy("CustomerId")
        .orderBy(F.col("UpdatedAt").desc(), F.col("EventHash").desc())
    )

    deduped_batch_df = (
        batch_df
        .withColumn("row_number", F.row_number().over(latest_per_customer_window))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )

    # Bronze is the immutable audit/replay layer.
    bronze_to_write = deduped_batch_df.select(
        "CustomerId", "CustomerName", "City", "Age", "UpdatedAt",
        "SourceFile", "IngestedAt", "EventHash"
    )

    bronze_to_write.write.format("delta").mode("append").option("txnAppId", "day17-customer-bronze").option("txnVersion", batch_id).saveAsTable(bronze_table)

    # Load the current SCD2 row only for each customer.
    current_silver_df = (
        spark.table(silver_table)
        .filter(F.col("IsCurrent") == True)
        .select(
            F.col("CustomerId").alias("CurrentCustomerId"),
            F.col("ValidFrom").alias("CurrentValidFrom"),
            F.col("EventHash").alias("CurrentEventHash")
        )
    )

    compared_df = (
        deduped_batch_df.alias("s")
        .join(
            current_silver_df.alias("t"),
            F.col("s.CustomerId") == F.col("t.CurrentCustomerId"),
            "left"
        )
        .select(
            F.col("s.CustomerId"), F.col("s.CustomerName"), F.col("s.City"),
            F.col("s.Age"), F.col("s.UpdatedAt"), F.col("s.SourceFile"),
            F.col("s.EventHash"), F.col("t.CurrentCustomerId"),
            F.col("t.CurrentValidFrom"), F.col("t.CurrentEventHash")
        )
    )

    # Do not allow an older changed event to rewrite already-built history.
    late_changed_df = (
        compared_df
        .filter(
            F.col("CurrentCustomerId").isNotNull()
            & (F.col("UpdatedAt") <= F.col("CurrentValidFrom"))
            & (F.col("EventHash") != F.col("CurrentEventHash"))
        )
        .select(
            "CustomerId", "CustomerName", "City", "Age", "UpdatedAt", "EventHash",
            F.lit("Changed event is older than or equal to the current SCD2 version").alias("LateReason"),
            "SourceFile", F.current_timestamp().alias("RejectedAt")
        )
    )

    if not late_changed_df.isEmpty():
        late_changed_df.write.format("delta").mode("append").option("txnAppId", "day17-customer-late-events").option("txnVersion", batch_id).saveAsTable(late_table)

    # An incoming record is eligible only if it is new or later than the active version.
    eligible_df = compared_df.filter(
        F.col("CurrentCustomerId").isNull()
        | (F.col("UpdatedAt") > F.col("CurrentValidFrom"))
    )

    # Same hash = unchanged. Only NEW and CHANGED records move to staging.
    new_or_changed_df = (
        eligible_df
        .filter(
            F.col("CurrentCustomerId").isNull()
            | (F.col("EventHash") != F.col("CurrentEventHash"))
        )
        .withColumn(
            "ChangeType",
            F.when(F.col("CurrentCustomerId").isNull(), F.lit("NEW"))
            .otherwise(F.lit("CHANGED"))
        )
        .select(
            "CustomerId", "CustomerName", "City", "Age",
            F.col("UpdatedAt").alias("ValidFrom"), "EventHash", "ChangeType"
        )
    )

    # Changed records generate two staging rows.
    # CLOSE matches the active target. INSERT has null MergeKey, so it is inserted.
    close_stage_df = (
        new_or_changed_df
        .filter(F.col("ChangeType") == "CHANGED")
        .withColumn("MergeKey", F.col("CustomerId"))
        .withColumn("Action", F.lit("CLOSE"))
    )

    insert_stage_df = (
        new_or_changed_df
        .withColumn("MergeKey", F.lit(None).cast("int"))
        .withColumn("Action", F.lit("INSERT"))
    )

    staged_changes_df = close_stage_df.unionByName(insert_stage_df)

    if not staged_changes_df.isEmpty():
        silver_delta = DeltaTable.forName(spark, silver_table)

        (
            silver_delta.alias("t")
            .merge(
                staged_changes_df.alias("s"),
                "t.CustomerId = s.MergeKey AND t.IsCurrent = true"
            )
            .whenMatchedUpdate(
                condition="s.Action = 'CLOSE'",
                set={"IsCurrent": "false", "ValidTo": "s.ValidFrom"}
            )
            .whenNotMatchedInsert(
                condition="s.Action = 'INSERT'",
                values={
                    "CustomerId": "s.CustomerId",
                    "CustomerName": "s.CustomerName",
                    "City": "s.City",
                    "Age": "s.Age",
                    "ValidFrom": "s.ValidFrom",
                    "ValidTo": "CAST(NULL AS TIMESTAMP)",
                    "IsCurrent": "true",
                    "EventHash": "s.EventHash"
                }
            )
            .execute()
        )
```

### How the logic classifies records

| Incoming event | Outcome |
|---|---|
| Customer does not exist in Silver | Insert a current version |
| Customer exists and hash differs, with a newer event time | Close old version and insert new current version |
| Customer exists and hash matches | No Silver change |
| Same customer occurs more than once in a micro-batch | Retain newest event only |
| Changed event is older than current history | Save to late-events table |

The staging design allows a single Delta `MERGE` to close the current version and insert the next version. See [Databricks Delta MERGE documentation](https://docs.databricks.com/aws/en/delta/merge).

---

## Step 7 — Run batch 1

Run the following Python cell.

```python
query = (
    prepared_stream_df.writeStream
    .foreachBatch(process_customer_scd2)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
```

The checkpoint path must remain the same for all later runs. It records the files that were processed.

Validate the first load:

```sql
SELECT CustomerId, CustomerName, City, Age, ValidFrom, ValidTo, IsCurrent
FROM workspace.default.day17_customer_scd2
ORDER BY CustomerId, ValidFrom;
```

Expected: three current rows and three null `ValidTo` values.

---

## Step 8 — Test changed, unchanged, new, and duplicate records

Create `day17_batch_02.csv`:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
102,Kumar,Bangalore,36,2026-08-02 10:00:00
103,Priya,Chennai,27,2026-08-02 10:05:00
104,Divya,Hyderabad,29,2026-08-02 10:10:00
104,Divya,Hyderabad,29,2026-08-02 10:11:00
```

Upload it into `incoming`, then run the Step 7 streaming cell again.

Expected results:

- `102`: age changed from 35 to 36; the prior version is closed and a new version is inserted.
- `103`: no tracked attribute changed; no history row is added.
- `104`: new customer; the duplicate is deduplicated and only the 10:11 event is retained.

Check the history for customer 102:

```sql
SELECT CustomerId, CustomerName, City, Age, ValidFrom, ValidTo, IsCurrent
FROM workspace.default.day17_customer_scd2
WHERE CustomerId = 102
ORDER BY ValidFrom;
```

Expected:

| CustomerId | Age | ValidFrom | ValidTo | IsCurrent |
|---|---:|---|---|---|
| 102 | 35 | 2026-08-01 09:05:00 | 2026-08-02 10:00:00 | false |
| 102 | 36 | 2026-08-02 10:00:00 | null | true |

---

## Step 9 — Test a later change, unchanged replay, and late event

Create `day17_batch_03.csv`:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
101,Arun,Chennai,31,2026-08-03 08:00:00
102,Kumar,Bangalore,36,2026-08-03 08:05:00
102,Kumar,Mysore,36,2026-08-01 08:00:00
```

Upload it into `incoming`, then run Step 7 again.

Expected results:

- `101`: age changed, creating a new current SCD2 version.
- `102` at `2026-08-03`: attributes are unchanged, so no new history is created.
- `102` at `2026-08-01`: the city differs but the event is older than the active history, so it goes to the late-events table.

Inspect late events:

```sql
SELECT CustomerId, City, Age, UpdatedAt, LateReason, RejectedAt
FROM workspace.default.day17_customer_late_events
ORDER BY RejectedAt;
```

---

## Step 10 — Validate the complete SCD2 table

### Confirm exactly one current row per customer

```sql
SELECT CustomerId, COUNT(*) AS CurrentRowCount
FROM workspace.default.day17_customer_scd2
WHERE IsCurrent = true
GROUP BY CustomerId
HAVING COUNT(*) <> 1;
```

Expected: no rows.

### Confirm closed rows have an end date

```sql
SELECT *
FROM workspace.default.day17_customer_scd2
WHERE IsCurrent = false
  AND ValidTo IS NULL;
```

Expected: no rows.

### Review all customer history

```sql
SELECT CustomerId, CustomerName, City, Age, ValidFrom, ValidTo, IsCurrent
FROM workspace.default.day17_customer_scd2
ORDER BY CustomerId, ValidFrom;
```

### Inspect Bronze audit data

```sql
SELECT CustomerId, CustomerName, City, Age, UpdatedAt, SourceFile, IngestedAt
FROM workspace.default.day17_customer_bronze
ORDER BY IngestedAt, CustomerId;
```

---

## Step 11 — Test idempotency

Run the Step 7 streaming cell one more time without uploading a file.

Expected: no additional records. The checkpoint prevents previously processed files from being read again.

The Bronze write uses `txnAppId` and `txnVersion=batch_id`, which protects against a repeated micro-batch write. The Silver logic is also logically idempotent: once a new version is active, replaying the same business event finds the same attribute hash and makes no further change.

---

## Production considerations

1. Prefer an immutable source event ID when one exists. It is stronger than deduplication by customer and timestamp.
2. Keep late records for reconciliation; do not silently discard them.
3. Agree on the meaning of `ValidTo`. This lab uses the next event time; some businesses use one millisecond before the next `ValidFrom`.
4. This lab retains the newest same-customer event in one micro-batch. To retain every intermediate update, use ordered CDC data with a reliable source sequence number.
5. Add data-quality checks for null customer IDs, invalid timestamps, invalid ages, and unexpected schema changes.
6. Treat the checkpoint path as permanent state. Reusing or deleting it can cause reprocessing.
7. Let unexpected failures fail the query and let job orchestration retry it; do not silently catch logic errors in `foreachBatch`.
8. Monitor current-row uniqueness, late-event volume, batch size, and processing duration.
9. For large SCD2 tables, choose partitioning and optimization based on query patterns; avoid partitioning by a high-cardinality customer ID.

## Day 17 outcome

You now have a Bronze-to-Silver customer pipeline that preserves raw events, maintains SCD Type 2 history, handles duplicate and unchanged inputs, protects established history from late changes, and can be tested one micro-batch at a time in Databricks Free Edition.

**Next:[Day 18](../Day18/README.md) - Data Quality & Quarantine**
