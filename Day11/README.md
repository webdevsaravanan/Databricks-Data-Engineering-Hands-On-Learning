# Day 11 - Auto Loader → Bronze → Silver Incremental Pipeline

## Objective

Build a realistic incremental Data Engineering pipeline using the concepts learned so far.

```text
CSV Files
    ↓
Auto Loader
    ↓
🥉 Bronze
    ↓
Deduplication
    ↓
Latest Record
    ↓
MERGE
    ↓
🥈 Silver
```

> **Bronze keeps incoming/history data. Silver maintains the current clean state.**

---

## 1. Scenario - RetailMart Customer Pipeline

RetailMart receives customer files continuously:

```text
customers_01.csv
customers_02.csv
customers_03.csv
...
```

Requirement:

> Incrementally ingest new files into Bronze and maintain the latest customer state in Silver.

---

## 2. Create the Source File

Create `customers_01.csv`:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
101,Arun,Chennai,30,2026-08-20 09:00:00
102,Kumar,Bangalore,35,2026-08-20 09:05:00
103,Priya,Chennai,27,2026-08-20 09:10:00
```

Place it in:

```text
/Volumes/workspace/bronze/customer_files/
```

---

## 3. Define Explicit Schema

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

`UpdatedAt` is initially read as a string and converted to a timestamp later.

---

## 4. Read Using Auto Loader

```python
bronze_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(customer_schema)
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/bronze/schema/day11_customers/")
    .load("/Volumes/workspace/bronze/customer_files/")
)
```

Important:

```text
cloudFiles
    ↓
Auto Loader

cloudFiles.schemaLocation
    ↓
Schema tracking

rescuedDataColumn
    ↓
Unexpected/incompatible data
```

---

## 5. Write to Bronze

```python
query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/workspace/bronze/checkpoints/day11_customers/")
    .trigger(availableNow=True)
    .toTable("bronze.day11_customers")
)
```

Check:

```sql
SELECT *
FROM bronze.day11_customers
ORDER BY CustomerId;
```

---

## 6. Add a Second File

Create `customers_02.csv`:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
101,Arun,Bangalore,31,2026-08-21 10:00:00
104,Ravi,Coimbatore,32,2026-08-21 10:05:00
```

Place it in:

```text
/Volumes/workspace/bronze/customer_files/
```

Run Auto Loader again.

Bronze now contains:

```text
101 | Arun  | Chennai    | 30
102 | Kumar | Bangalore  | 35
103 | Priya | Chennai    | 27
101 | Arun  | Bangalore  | 31
104 | Ravi  | Coimbatore | 32
```

Customer 101 appears twice. This is correct Bronze behavior.

---

## 7. Why Bronze Contains Duplicates

Auto Loader performs **file-level incremental ingestion**.

It does not know that:

```text
CustomerId = 101
```

is a business key.

Therefore:

```text
New File
   ↓
Auto Loader
   ↓
Read Records
   ↓
Append to Bronze
```

Deduplication and business-key updates are downstream responsibilities.

---

## 8. Read Bronze

```python
bronze = spark.table("bronze.day11_customers")
```

---

## 9. Convert UpdatedAt to Timestamp

```python
from pyspark.sql.functions import col, to_timestamp

bronze = bronze.withColumn("UpdatedAt", to_timestamp(col("UpdatedAt")))
```

---

## 10. Deduplicate Using ROW_NUMBER

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
```

Create the window:

```python
window_spec = (
    Window
    .partitionBy("CustomerId")
    .orderBy(col("UpdatedAt").desc())
)
```

Select the latest record:

```python
latest_customers = (
    bronze
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)
```

Check:

```python
display(latest_customers)
```

Expected:

```text
101 | Arun  | Bangalore  | 31
102 | Kumar | Bangalore  | 35
103 | Priya | Chennai    | 27
104 | Ravi  | Coimbatore | 32
```

---

## 11. Why ROW_NUMBER?

For Customer 101:

```text
CustomerId | City       | UpdatedAt
---------------------------------------------
101        | Chennai    | 2026-08-20 09:00
101        | Bangalore  | 2026-08-21 10:00
```

With:

```text
PARTITION BY CustomerId
ORDER BY UpdatedAt DESC
```

the latest record gets:

```text
row_num = 1
```

and the older record gets:

```text
row_num = 2
```

Therefore:

```python
.filter(col("row_num") == 1)
```

keeps the latest version.

---

## 12. Create Silver

Create the initial Silver table:

```python
latest_customers.write.mode("overwrite").saveAsTable("silver.day11_customers")
```

Check:

```sql
SELECT *
FROM silver.day11_customers
ORDER BY CustomerId;
```

---

## 13. Add a Third File

Create `customers_03.csv`:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
102,Kumar,Hyderabad,36,2026-08-22 09:00:00
105,Meena,Madurai,29,2026-08-22 09:05:00
```

Run Auto Loader again.

Latest expected state:

```text
101 | Arun  | Bangalore  | 31
102 | Kumar | Hyderabad  | 36
103 | Priya | Chennai    | 27
104 | Ravi  | Coimbatore | 32
105 | Meena | Madurai    | 29
```

---

## 14. Why MERGE?

We want:

```text
If CustomerId exists
        ↓
      UPDATE

If CustomerId does not exist
        ↓
      INSERT
```

This is an **upsert**.

Delta Lake `MERGE` provides this behavior.

---

## 15. MERGE Into Silver

```python
from delta.tables import DeltaTable

silver_table = DeltaTable.forName(spark, "silver.day11_customers")

(
    silver_table.alias("target")
    .merge(
        latest_customers.alias("source"),
        "target.CustomerId = source.CustomerId"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

Check:

```sql
SELECT *
FROM silver.day11_customers
ORDER BY CustomerId;
```

Expected:

```text
101 | Arun  | Bangalore  | 31
102 | Kumar | Hyderabad  | 36
103 | Priya | Chennai    | 27
104 | Ravi  | Coimbatore | 32
105 | Meena | Madurai    | 29
```

---

## 16. Test INSERT

Create:

```csv
106,Suresh,Salem,40,2026-08-23 10:00:00
```

Expected:

```text
106 → INSERT into Silver
```

---

## 17. Test UPDATE

Create:

```csv
103,Priya,Bangalore,28,2026-08-23 11:00:00
```

Expected:

```text
103 → UPDATE in Silver
```

Result:

```text
103 | Priya | Bangalore | 28
```

---

## 18. Test an Older Record

Create:

```csv
103,Priya,Chennai,27,2026-08-19 10:00:00
```

Because this record is older than the current version, the latest-record logic should continue to select:

```text
103 | Priya | Bangalore | 28
```

---

## 19. Complete Pipeline

```text
                  CSV FILES
                      │
                      ▼
                 AUTO LOADER
                      │
                      ▼
                  🥉 BRONZE
              Append / History
                      │
                      ▼
               Read Bronze
                      │
                      ▼
            Convert UpdatedAt
                      │
                      ▼
                ROW_NUMBER()
                      │
                      ▼
              Latest Record
                      │
                      ▼
                   MERGE
                      │
                      ▼
                  🥈 SILVER
               Current State
```

---

## 20. Bronze vs Silver

### Bronze

Bronze represents what arrived from the source.

It can contain multiple versions:

```text
101 | Arun | Chennai    | 30
101 | Arun | Bangalore  | 31
```

### Silver

Silver represents the latest clean state:

```text
101 | Arun | Bangalore | 31
```

Remember:

> **Bronze = incoming/history**

> **Silver = current/conformed state**

---

## 21. Important Architecture Concept

Different components have different responsibilities:

```text
Auto Loader
    ↓
File-level incremental ingestion

Window Function
    ↓
Record-level deduplication

MERGE
    ↓
Insert / Update Silver
```

This distinction is fundamental to production Data Engineering pipelines.

---

## 22. Day 11 Checklist

- [ ] Create customer CSV files
- [ ] Configure Auto Loader
- [ ] Use explicit schema
- [ ] Configure `_rescued_data`
- [ ] Configure schema location
- [ ] Configure checkpoint location
- [ ] Ingest into Bronze
- [ ] Add a second file
- [ ] Observe duplicate business records in Bronze
- [ ] Convert `UpdatedAt` to timestamp
- [ ] Use `ROW_NUMBER()`
- [ ] Select the latest record
- [ ] Create Silver
- [ ] Use Delta `MERGE`
- [ ] Test INSERT
- [ ] Test UPDATE
- [ ] Test an older record
- [ ] Understand Bronze vs Silver responsibilities
- [ ] Understand file-level vs record-level incremental processing

---

# Key Learnings

### Auto Loader

```text
New Files
   ↓
Incremental Ingestion
   ↓
Bronze
```

### Bronze

```text
Raw / Append-oriented
        ↓
Can contain multiple record versions
```

### Window Function

```text
CustomerId
    ↓
Order by UpdatedAt DESC
    ↓
ROW_NUMBER()
    ↓
Latest Record
```

### MERGE

```text
Existing Customer
       ↓
    UPDATE

New Customer
       ↓
    INSERT
```

### Silver

```text
Clean
Current
Conformed
Business-ready
```

---

# Day 11 Architecture

```text
                         SOURCE
                           │
                           ▼
                    CSV / File Arrival
                           │
                           ▼
                      AUTO LOADER
                           │
                           ▼
                        BRONZE
                   Raw / Append Only
                           │
                           ▼
                  Data Transformation
                           │
                           ▼
                     DEDUPLICATION
                           │
                           ▼
                    Latest Record
                           │
                           ▼
                         MERGE
                           │
                           ▼
                        SILVER
                     Current State
                           │
                           ▼
                         GOLD
```

---

# Progress

```text
Day 01
Databricks Fundamentals
        ↓
Day 02
File Ingestion → Bronze
        ↓
Day 03
Bronze → Silver
        ↓
Day 04
Silver → Gold
        ↓
Day 05
Delta Lake
        ↓
Day 06
MERGE / Upsert
        ↓
Day 07
Data Quality
        ↓
Day 08
Window Functions / Deduplication
        ↓
Day 09
Incremental ETL
        ↓
Day 10
Auto Loader
        ↓
Day 11
Auto Loader → Bronze → Silver
```

---

# Outcome

By completing Day 11, you should be able to explain:

> Auto Loader incrementally ingests new files into Bronze. Bronze is append-oriented and can contain multiple versions of the same business record. We then read Bronze, use a window function to select the latest record for each business key, and MERGE those records into Silver so that new records are inserted and existing records are updated.

This is a core production Data Engineering pattern.

---

**Next: [Day 12](../Day12/README.md) - Structured Streaming and how the Bronze → Silver pipeline can run continuously instead of being manually triggered.**
