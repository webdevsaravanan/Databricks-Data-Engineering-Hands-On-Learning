# Day 16 - SCD Type 1 & SCD Type 2 with Delta Lake

## Objective

Learn how to handle changes to customer dimension data using Slowly
Changing Dimensions (SCD).

Today we will implement:

-   SCD Type 1
-   SCD Type 2
-   Delta Lake MERGE for Type 1
-   Historical version tracking for Type 2
-   `StartDate`
-   `EndDate`
-   `IsCurrent`
-   Current-record queries
-   Historical-record queries
-   Changed vs unchanged records
-   Production considerations such as idempotency

The core idea is:

``` text
Customer changes
      ↓
How should Silver store the change?
      ↓
SCD Type 1 or SCD Type 2
```

------------------------------------------------------------------------

## 1. Day 16 Scenario

We continue the RetailMart customer pipeline.

Initial customer data:

``` text
CustomerId | CustomerName | City       | Age
-----------|--------------|------------|----
101        | Arun         | Chennai    | 30
102        | Kumar        | Bangalore  | 35
103        | Priya        | Chennai    | 27
```

Later, the source sends:

``` csv
CustomerId,CustomerName,City,Age,UpdatedAt
101,Arun,Coimbatore,31,2026-08-24 09:00:00
102,Kumar,Bangalore,36,2026-08-24 09:05:00
104,Meena,Madurai,29,2026-08-24 09:10:00
```

Changes:

``` text
Customer 101
City: Chennai → Coimbatore
Age : 30     → 31

Customer 102
Age: 35 → 36

Customer 104
New customer
```

The business question is:

> **Should we overwrite the old values or preserve the history?**

This is where SCD comes in.

------------------------------------------------------------------------

# Part A - SCD Type 1

## 2. What Is SCD Type 1?

SCD Type 1 means:

> **Overwrite the existing record with the latest value.**

Before:

``` text
CustomerId | CustomerName | City
-----------|--------------|---------
101        | Arun         | Chennai
```

Incoming:

``` text
101 | Arun | Coimbatore
```

After:

``` text
CustomerId | CustomerName | City
-----------|--------------|------------
101        | Arun         | Coimbatore
```

The previous `Chennai` value is no longer stored as a separate version.

------------------------------------------------------------------------

## 3. Create Day 16 Source Directory

Use:

``` text
/Volumes/workspace/bronze/day16_customer_files/
```

Create:

``` text
day16_customers_initial.csv
```

with:

``` csv
CustomerId,CustomerName,City,Age,UpdatedAt
101,Arun,Chennai,30,2026-08-01 09:00:00
102,Kumar,Bangalore,35,2026-08-01 09:05:00
103,Priya,Chennai,27,2026-08-01 09:10:00
```

------------------------------------------------------------------------

## 4. Read the Initial CSV

``` python
initial_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Volumes/workspace/bronze/day16_customer_files/day16_customers_initial.csv")
)
```

Check:

``` python
display(initial_df)
```

------------------------------------------------------------------------

## 5. Check the Schema

``` python
initial_df.printSchema()
```

Expected approximately:

``` text
CustomerId   integer
CustomerName string
City         string
Age          integer
UpdatedAt    timestamp
```

------------------------------------------------------------------------

## 6. Load Initial Data into Bronze

``` python
initial_df.write.mode("overwrite").format("delta").saveAsTable("bronze.day16_customers")
```

Check:

``` sql
SELECT *
FROM bronze.day16_customers
ORDER BY CustomerId;
```

------------------------------------------------------------------------

## 7. Create SCD Type 1 Silver Table

``` sql
CREATE TABLE IF NOT EXISTS silver.day16_customers_scd1 (
    CustomerId INT,
    CustomerName STRING,
    City STRING,
    Age INT,
    UpdatedAt TIMESTAMP
)
USING DELTA;
```

------------------------------------------------------------------------

## 8. Initial SCD1 Load

``` python
bronze_df = spark.table("bronze.day16_customers")
```

Load into Silver:

``` python
bronze_df.write.mode("overwrite").saveAsTable("silver.day16_customers_scd1")
```

Check:

``` sql
SELECT *
FROM silver.day16_customers_scd1
ORDER BY CustomerId;
```

Expected:

``` text
101 | Arun  | Chennai   | 30
102 | Kumar | Bangalore | 35
103 | Priya | Chennai    | 27
```

------------------------------------------------------------------------

## 9. Create Changed Customer Data

Create:

``` text
day16_customers_updates.csv
```

``` csv
CustomerId,CustomerName,City,Age,UpdatedAt
101,Arun,Coimbatore,31,2026-08-24 09:00:00
102,Kumar,Bangalore,36,2026-08-24 09:05:00
104,Meena,Madurai,29,2026-08-24 09:10:00
```

There are three cases:

``` text
101 → Existing + changed
102 → Existing + changed
104 → New customer
```

------------------------------------------------------------------------

## 10. Read the Update File

``` python
updates_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Volumes/workspace/bronze/day16_customer_files/day16_customers_updates.csv")
)
```

Check:

``` python
display(updates_df)
```

------------------------------------------------------------------------

## 11. SCD Type 1 Logic

``` text
Existing Customer
       ↓
Overwrite

New Customer
       ↓
Insert
```

Expected final result:

``` text
101 | Arun  | Coimbatore | 31
102 | Kumar | Bangalore  | 36
103 | Priya | Chennai    | 27
104 | Meena | Madurai    | 29
```

------------------------------------------------------------------------

## 12. Perform SCD Type 1 MERGE

``` python
from delta.tables import DeltaTable

silver_scd1 = DeltaTable.forName(
    spark,
    "silver.day16_customers_scd1"
)
```

MERGE:

``` python
(
    silver_scd1.alias("target")
    .merge(
        updates_df.alias("source"),
        "target.CustomerId = source.CustomerId"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

------------------------------------------------------------------------

## 13. Check SCD Type 1 Result

``` sql
SELECT *
FROM silver.day16_customers_scd1
ORDER BY CustomerId;
```

Expected:

``` text
CustomerId | CustomerName | City       | Age
-----------|--------------|------------|----
101        | Arun         | Coimbatore | 31
102        | Kumar        | Bangalore  | 36
103        | Priya        | Chennai    | 27
104        | Meena        | Madurai    | 29
```

Notice:

``` text
101 | Chennai | 30
```

is gone.

This is the main characteristic of SCD Type 1.

------------------------------------------------------------------------

# Part B - SCD Type 2

## 14. What Is SCD Type 2?

SCD Type 2 means:

> **Preserve the old record and create a new version when a tracked
> business attribute changes.**

Before:

``` text
CustomerId | City
-----------|---------
101        | Chennai
```

New data:

``` text
101 | Coimbatore
```

After:

``` text
CustomerId | City       | IsCurrent
-----------|------------|----------
101        | Chennai    | false
101        | Coimbatore | true
```

The old version is retained.

------------------------------------------------------------------------

## 15. SCD2 Metadata Columns

A typical SCD2 table contains:

``` text
CustomerId
CustomerName
City
Age
UpdatedAt
StartDate
EndDate
IsCurrent
```

Example:

``` text
CustomerId | City       | StartDate  | EndDate    | IsCurrent
-----------|------------|------------|------------|----------
101        | Chennai    | 2026-08-01 | 2026-08-24 | false
101        | Coimbatore | 2026-08-24 | NULL       | true
```

------------------------------------------------------------------------

## 16. Create SCD2 Silver Table

``` sql
CREATE TABLE IF NOT EXISTS silver.day16_customers_scd2 (
    CustomerId INT,
    CustomerName STRING,
    City STRING,
    Age INT,
    UpdatedAt TIMESTAMP,
    StartDate TIMESTAMP,
    EndDate TIMESTAMP,
    IsCurrent BOOLEAN
)
USING DELTA;
```

------------------------------------------------------------------------

## 17. Prepare Initial SCD2 Data

``` python
from pyspark.sql.functions import col, lit

initial_scd2_df = (
    initial_df
    .withColumn("StartDate", col("UpdatedAt"))
    .withColumn("EndDate", lit(None).cast("timestamp"))
    .withColumn("IsCurrent", lit(True))
)
```

Check:

``` python
display(initial_scd2_df)
```

Expected:

``` text
CustomerId | City       | Age | StartDate | EndDate | IsCurrent
-----------|------------|-----|-----------|---------|----------
101        | Chennai    | 30  | Aug 01    | NULL    | true
102        | Bangalore  | 35  | Aug 01    | NULL    | true
103        | Chennai    | 27  | Aug 01    | NULL    | true
```

------------------------------------------------------------------------

## 18. Initial SCD2 Load

``` python
initial_scd2_df.write.mode("overwrite").saveAsTable("silver.day16_customers_scd2")
```

Check:

``` sql
SELECT *
FROM silver.day16_customers_scd2
ORDER BY CustomerId;
```

Every initial record should have:

``` text
IsCurrent = true
EndDate   = NULL
```

------------------------------------------------------------------------

## 19. Prepare the Update Data

``` python
updates_scd2_df = (
    updates_df
    .withColumn("StartDate", col("UpdatedAt"))
    .withColumn("EndDate", lit(None).cast("timestamp"))
    .withColumn("IsCurrent", lit(True))
)
```

------------------------------------------------------------------------

## 20. Understand the SCD2 Change

Take Customer 101.

Current Silver:

``` text
101 | Arun | Chennai | 30 | true
```

Incoming:

``` text
101 | Arun | Coimbatore | 31
```

SCD2 must perform two logical operations.

### Operation 1 - Close the old version

``` text
101 | Arun | Chennai | 30 | false
```

Set:

``` text
EndDate = 2026-08-24
```

### Operation 2 - Insert the new version

``` text
101 | Arun | Coimbatore | 31 | true
```

Final state:

``` text
101 | Chennai    | 30 | false
101 | Coimbatore | 31 | true
```

------------------------------------------------------------------------

## 21. Identify Changed Current Records

``` python
current_df = (
    spark.table("silver.day16_customers_scd2")
    .filter(col("IsCurrent") == True)
)
```

Join source and target:

``` python
changed_df = (
    updates_df.alias("source")
    .join(
        current_df.alias("target"),
        "CustomerId"
    )
    .filter(
        (col("source.CustomerName") != col("target.CustomerName")) |
        (col("source.City") != col("target.City")) |
        (col("source.Age") != col("target.Age"))
    )
)
```

Check:

``` python
display(changed_df)
```

Expected changed customers:

``` text
101
102
```

Customer 104 does not appear because it does not yet exist in the
target.

------------------------------------------------------------------------

## 22. Close Existing SCD2 Versions

``` python
silver_scd2 = DeltaTable.forName(
    spark,
    "silver.day16_customers_scd2"
)
```

Close changed current records:

``` python
(
    silver_scd2.alias("target")
    .merge(
        updates_df.alias("source"),
        "target.CustomerId = source.CustomerId AND target.IsCurrent = true"
    )
    .whenMatchedUpdate(
        condition="""
            target.CustomerName <> source.CustomerName
            OR target.City <> source.City
            OR target.Age <> source.Age
        """,
        set={
            "EndDate": "source.UpdatedAt",
            "IsCurrent": "false"
        }
    )
    .execute()
)
```

------------------------------------------------------------------------

## 23. Check the Closed Versions

``` sql
SELECT *
FROM silver.day16_customers_scd2
ORDER BY CustomerId, StartDate;
```

Expected approximately:

``` text
101 | Arun  | Chennai   | 30 | Aug 01 | Aug 24 | false
102 | Kumar | Bangalore | 35 | Aug 01 | Aug 24 | false
103 | Priya | Chennai   | 27 | Aug 01 | NULL   | true
```

The old records are preserved and marked as no longer current.

------------------------------------------------------------------------

## 24. Create New SCD2 Versions

``` python
new_versions_df = (
    updates_df
    .withColumn("StartDate", col("UpdatedAt"))
    .withColumn("EndDate", lit(None).cast("timestamp"))
    .withColumn("IsCurrent", lit(True))
)
```

------------------------------------------------------------------------

## 25. Insert the New Versions

``` python
new_versions_df.write.mode("append").saveAsTable("silver.day16_customers_scd2")
```

This inserts:

``` text
101 | Arun  | Coimbatore | 31 | true
102 | Kumar | Bangalore  | 36 | true
104 | Meena | Madurai    | 29 | true
```

------------------------------------------------------------------------

## 26. Check Final SCD2 Result

``` sql
SELECT
    CustomerId,
    CustomerName,
    City,
    Age,
    StartDate,
    EndDate,
    IsCurrent
FROM silver.day16_customers_scd2
ORDER BY CustomerId, StartDate;
```

Expected:

``` text
CustomerId | CustomerName | City       | Age | StartDate | EndDate | IsCurrent
-----------|--------------|------------|-----|-----------|---------|----------
101        | Arun         | Chennai    | 30  | Aug 01    | Aug 24  | false
101        | Arun         | Coimbatore | 31  | Aug 24    | NULL    | true

102        | Kumar        | Bangalore  | 35  | Aug 01    | Aug 24  | false
102        | Kumar        | Bangalore  | 36  | Aug 24    | NULL    | true

103        | Priya        | Chennai    | 27  | Aug 01    | NULL    | true

104        | Meena        | Madurai    | 29  | Aug 24    | NULL    | true
```

This is SCD Type 2.

------------------------------------------------------------------------

# Part C - Querying SCD2

## 27. Query Current Customers

``` sql
SELECT *
FROM silver.day16_customers_scd2
WHERE IsCurrent = true
ORDER BY CustomerId;
```

Expected:

``` text
101 | Arun  | Coimbatore | 31
102 | Kumar | Bangalore  | 36
103 | Priya | Chennai    | 27
104 | Meena | Madurai    | 29
```

------------------------------------------------------------------------

## 28. Query Customer History

``` sql
SELECT *
FROM silver.day16_customers_scd2
WHERE CustomerId = 101
ORDER BY StartDate;
```

Expected:

``` text
101 | Arun | Chennai    | 30 | Aug 01 | Aug 24 | false
101 | Arun | Coimbatore | 31 | Aug 24 | NULL   | true
```

------------------------------------------------------------------------

## 29. Query Historical State

Suppose the business asks:

> Where was Customer 101 on August 20?

``` sql
SELECT *
FROM silver.day16_customers_scd2
WHERE CustomerId = 101
  AND StartDate <= '2026-08-20'
  AND (EndDate > '2026-08-20' OR EndDate IS NULL);
```

Expected:

``` text
101 | Arun | Chennai
```

This is one of the main advantages of SCD Type 2.

------------------------------------------------------------------------

# Part D - Unchanged Records

## 30. Test an Unchanged Customer

Create:

``` csv
CustomerId,CustomerName,City,Age,UpdatedAt
103,Priya,Chennai,27,2026-08-25 09:00:00
```

Compare:

``` text
CustomerName → same
City         → same
Age          → same
UpdatedAt    → changed
```

If `UpdatedAt` is only a technical timestamp, this should normally **not
create a new SCD2 version**.

Production principle:

``` text
Technical column changed
        ↓
Usually no new SCD2 version

Tracked business attribute changed
        ↓
Create new SCD2 version
```

------------------------------------------------------------------------

# Part E - Type 1 vs Type 2

## 31. Direct Comparison

  Feature                   SCD Type 1   SCD Type 2
  ------------------------- ------------ ------------
  Latest value              Yes          Yes
  Historical values         No           Yes
  Old record overwritten    Yes          No
  Multiple versions         No           Yes
  `IsCurrent`               Usually no   Yes
  `StartDate` / `EndDate`   Usually no   Yes
  Complexity                Lower        Higher
  Storage                   Lower        Higher

------------------------------------------------------------------------

## 32. Visual Comparison

### Type 1

``` text
Before

101 | Chennai | 30

       ↓ CHANGE

After

101 | Coimbatore | 31
```

History:

``` text
LOST
```

### Type 2

``` text
Before

101 | Chennai | 30 | true

       ↓ CHANGE

After

101 | Chennai    | 30 | false
101 | Coimbatore | 31 | true
```

History:

``` text
PRESERVED
```

------------------------------------------------------------------------

# Part F - Production Consideration

## 33. Why `whenMatchedUpdateAll()` Is Type 1

Our existing MERGE pattern:

``` python
.whenMatchedUpdateAll()
.whenNotMatchedInsertAll()
```

does:

``` text
Existing
   ↓
Overwrite
   ↓
Latest record
```

This is naturally suited to Type 1.

Type 2 requires:

``` text
Existing current version
        ↓
Close old version
        ↓
Keep old record
        ↓
Insert new version
```

Therefore Type 2 needs additional logic.

------------------------------------------------------------------------

## 34. SCD2 Idempotency

Our simple hands-on implementation demonstrates the concept, but
production pipelines must handle retries and duplicate processing
safely.

For example, if the same update is processed twice:

``` text
101 | Coimbatore | 31
```

we should not create duplicate current versions.

A production SCD2 pipeline must carefully handle:

``` text
New customer
Changed customer
Unchanged customer
Duplicate source record
Retry
Reprocessing
```

This is called designing the pipeline to be **idempotent**.

------------------------------------------------------------------------

# 35. Day 15 → Day 16

### Day 15

``` text
Unexpected source data
        ↓
_rescued_data
```

### Day 16

``` text
Valid customer data
        ↓
Business attribute changes
        ↓
How should history be managed?
        ↓
SCD Type 1 / SCD Type 2
```

------------------------------------------------------------------------

# 36. Day 16 Architecture

``` text
                    SOURCE
                       │
                       ▼
                    BRONZE
                       │
                       ▼
                 CUSTOMER CHANGE
                       │
                       ▼
                  SCD DECISION
                       │
             ┌─────────┴─────────┐
             ▼                   ▼
          TYPE 1              TYPE 2
             │                   │
             ▼                   ▼
        OVERWRITE          CLOSE OLD VERSION
                                 │
                                 ▼
                           INSERT NEW VERSION
                                 │
                                 ▼
                              SILVER
```

------------------------------------------------------------------------

# 37. Day 16 Checklist

-   [ ] Create initial customer CSV
-   [ ] Read initial customer data
-   [ ] Load initial data into Bronze
-   [ ] Create SCD1 Silver table
-   [ ] Perform initial SCD1 load
-   [ ] Create update CSV
-   [ ] Identify existing and new customers
-   [ ] Perform SCD1 MERGE
-   [ ] Verify old values are overwritten
-   [ ] Create SCD2 Silver table
-   [ ] Add `StartDate`
-   [ ] Add `EndDate`
-   [ ] Add `IsCurrent`
-   [ ] Perform initial SCD2 load
-   [ ] Create changed customer records
-   [ ] Identify changed business attributes
-   [ ] Close old SCD2 versions
-   [ ] Insert new SCD2 versions
-   [ ] Query current records
-   [ ] Query historical records
-   [ ] Query historical state by date
-   [ ] Test an unchanged customer
-   [ ] Understand Type 1 vs Type 2
-   [ ] Understand why `whenMatchedUpdateAll()` fits Type 1
-   [ ] Understand SCD2 idempotency considerations

------------------------------------------------------------------------

# Key Learnings

### SCD Type 1

``` text
OLD
 ↓
Overwrite
 ↓
NEW

History = Lost
```

### SCD Type 2

``` text
OLD
 ↓
Close old version
 ↓
Keep old version
 ↓
Insert new version

History = Preserved
```

### SCD2 Metadata

``` text
StartDate
EndDate
IsCurrent
```

help determine which version was active and which version is currently
valid.

### Current State

``` text
WHERE IsCurrent = true
```

returns the current customer version.

### Historical State

``` text
StartDate <= requested date
AND
EndDate > requested date
OR EndDate IS NULL
```

allows point-in-time analysis.

### Business vs Technical Changes

``` text
Business attribute changed
        ↓
SCD2 version may be required

Only technical timestamp changed
        ↓
Usually no new SCD2 version
```

### Bronze vs Silver

``` text
Bronze
   ↓
Source-oriented data

Silver
   ↓
Business-oriented dimension
   ↓
SCD history
```

------------------------------------------------------------------------

# Day 16 Final Mental Model

``` text
                         SOURCE
                            │
                            ▼
                         BRONZE
                            │
                            ▼
                    CUSTOMER CHANGE
                            │
                            ▼
                      SCD DECISION
                            │
                 ┌──────────┴──────────┐
                 ▼                     ▼
              TYPE 1                TYPE 2
                 │                     │
                 ▼                     ▼
             OVERWRITE          IS BUSINESS ATTRIBUTE
                                      CHANGED?
                                  ┌────┴────┐
                                 NO        YES
                                 │          │
                                 ▼          ▼
                               IGNORE    CLOSE OLD
                                            │
                                            ▼
                                      INSERT NEW
                                            │
                                            ▼
                                          SILVER
```

## Day 16 Outcome

You should now be able to explain:

> **SCD Type 1 overwrites changed dimension attributes and keeps only
> the latest state. SCD Type 2 preserves history by closing the previous
> version and inserting a new current version. Delta Lake MERGE is
> commonly used for these patterns, but Type 2 requires additional logic
> to manage historical versions, effective dates, and current-record
> status.**

**Next:[Day 17](../Day17/README.md) - Streaming SCD Type 2 with Delta Lake**
