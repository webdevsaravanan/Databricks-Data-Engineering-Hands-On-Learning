# Day 15 - Schema Evolution & `_rescued_data`

## Objective

Learn how Auto Loader handles source data that does not match the expected schema.

Today we focus on:

- Explicit schema
- `_rescued_data`
- Unexpected/new columns
- Datatype mismatches
- NULL values vs rescued values
- Bronze handling of unexpected source data
- Difference between rescue mode and schema evolution
- How Silver should use a controlled schema

```text
CSV Files
    ↓
Auto Loader
    ↓
Explicit Schema
    ↓
🥉 Bronze
    ↓
Unexpected / Unmapped Data
    ↓
_rescued_data
```

---

## 1. Day 15 Scenario

We continue the RetailMart customer pipeline.

Our existing customer schema is:

```text
CustomerId
CustomerName
City
Age
UpdatedAt
```

Now the source system sends an additional column:

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

The important question today is:

> **What happens when the incoming CSV contains data that is not part of our explicit schema?**

---

## 2. Current Explicit Schema

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

Notice that `Email` is not included.

---

## 3. Create Day 15 Source Directory

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

## 4. Auto Loader with `_rescued_data`

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

The important option is:

```python
.option("rescuedDataColumn", "_rescued_data")
```

It tells Auto Loader to preserve data that cannot be mapped normally to the expected schema.

---

## 5. Important Observation

Because we explicitly provide:

```python
.schema(customer_schema)
```

the DataFrame schema is based on the schema we supplied.

Therefore:

```python
bronze_stream.columns
```

does **not** mean:

> "Show me every column physically present in the CSV header."

For our example, the DataFrame columns can look conceptually like:

```text
CustomerId
CustomerName
City
Age
UpdatedAt
_rescued_data
```

You should **not** expect `Email` to appear as a normal DataFrame column when using the fixed explicit schema and rescue handling.

---

## 6. Inspect the Streaming DataFrame

```python
bronze_stream.printSchema()
```

Then:

```python
display(bronze_stream)
```

Pay attention to:

```text
_rescued_data
```

---

## 7. Inspect `_rescued_data`

```python
display(
    bronze_stream.select(
        "CustomerId",
        "_rescued_data"
    )
)
```

Conceptually, you may see something like:

```text
CustomerId | _rescued_data
-----------|----------------------------
110        | {"Email":"arun@gmail.com"}
111        | {"Email":"kumar@gmail.com"}
```

The exact representation depends on the Auto Loader configuration and runtime, so always inspect the actual result.

The important concept is:

```text
Expected columns
      ↓
Normal DataFrame columns

Unexpected/unmapped data
      ↓
_rescued_data
```

---

## 8. Write to Bronze

For Databricks Free Edition, use:

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

---

## 9. Check Bronze

```sql
SELECT *
FROM bronze.day15_customers
ORDER BY CustomerId;
```

Also inspect:

```sql
SELECT CustomerId, CustomerName, City, Age, UpdatedAt, _rescued_data
FROM bronze.day15_customers
ORDER BY CustomerId;
```

The objective is to see how the unexpected `Email` information is represented.

---

# 10. Why `_rescued_data` Is Useful

Imagine the source team unexpectedly adds:

```text
Email
```

With rescue handling:

```text
Source
  ↓
CustomerId
CustomerName
City
Age
UpdatedAt
Email
  ↓
Auto Loader
  ↓
Expected schema
  ↓
Known columns → normal columns
Unexpected data → _rescued_data
```

This gives the engineering team visibility into unexpected source data.

---

# 11. New Column vs Schema Evolution

These concepts should not be confused.

### Fixed explicit schema + rescue

```text
.schema(customer_schema)
        ↓
Fixed expected schema
        ↓
Unexpected data
        ↓
_rescued_data
```

### Auto Loader schema evolution

Auto Loader also supports schema evolution configurations such as:

```python
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
```

Conceptually:

```text
Source
  ↓
Auto Loader
  ↓
Schema changes
  ↓
Schema evolves
```

This is a different strategy from deliberately keeping a fixed schema and rescuing unexpected data.

For our Day 15 hands-on, we are focusing on:

> **Explicit schema + `_rescued_data`**

rather than automatically evolving the schema.

---

# 12. Why Not Automatically Add Every New Column?

Suppose today's source adds:

```text
Email
```

Tomorrow:

```text
Phone
```

Next week:

```text
LoyaltyScore
```

If every source change automatically becomes part of Silver, the business data model can become uncontrolled.

A controlled architecture is:

```text
Source
   ↓
Bronze
   ↓
Controlled transformation
   ↓
Silver
```

Bronze can preserve unexpected source information while Silver exposes an intentional business schema.

---

# 13. Datatype Mismatch Scenario

Suppose our schema says:

```python
StructField("Age", IntegerType(), True)
```

but the source sends:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
112,Ravi,Chennai,ABC,2026-08-23 10:00:00
```

The source has:

```text
Age = ABC
```

but our schema expects:

```text
Age = Integer
```

This is a datatype mismatch.

---

# 14. Test Datatype Mismatch

Create `customers_06.csv`:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
112,Ravi,Chennai,ABC,2026-08-23 10:00:00
```

Process it through Auto Loader.

Then inspect:

```sql
SELECT CustomerId, Age, _rescued_data
FROM bronze.day15_customers
WHERE CustomerId = 112;
```

Investigate whether the original problematic value is available through `_rescued_data`.

---

# 15. NULL vs Datatype Problem

Consider:

### Record 1

```csv
113,Ravi,Chennai,,2026-08-23 10:05:00
```

Age is genuinely missing.

### Record 2

```csv
114,Mani,Chennai,ABC,2026-08-23 10:10:00
```

Age contains a value that cannot be interpreted as an integer.

Both situations can result in an unusable/null `Age` value after parsing, but their meanings are different.

```text
Age = NULL
    │
    ├── Source value was missing
    │
    └── Source value could not be mapped correctly
                 ↓
           Check _rescued_data
```

Therefore:

> **Do not automatically assume that every NULL means the source sent NULL.**

---

# 16. Schema Problem vs Data Quality Problem

Consider:

```text
Age = ABC
```

This is a schema/datatype mapping problem.

But:

```text
Age = -5
```

is different.

`-5` is a perfectly valid integer.

Therefore:

```text
Age = -5
       ↓
Datatype = VALID
       ↓
Business rule = INVALID
       ↓
Data Quality problem
```

Day 14 handles this type of problem with Quarantine.

---

# 17. Three Scenarios

| Input | Problem | Main Handling |
|---|---|---|
| `Email` is added | Unexpected/new column | `_rescued_data` with fixed schema |
| `Age = ABC` | Datatype mismatch | `_rescued_data` / ingestion investigation |
| `Age = -5` | Business-rule failure | Data Quality / Quarantine |

Remember:

```text
Email = new source field
        ↓
Schema difference

Age = ABC
        ↓
Datatype mapping problem

Age = -5
        ↓
Data Quality problem
```

---

# 18. Bronze and Silver

### Bronze

Bronze is source-oriented.

```text
Bronze
 ├── Raw/ingested columns
 ├── Unexpected data
 └── _rescued_data
```

Its purpose is to preserve incoming information and support downstream processing.

### Silver

Silver is business-oriented.

```text
Silver
 ├── Approved columns
 ├── Validated data
 ├── Deduplicated records
 └── Business rules
```

Therefore:

```text
Bronze
   ↓
Preserve source information

Silver
   ↓
Controlled business model
```

---

# 19. Day 14 + Day 15

### Day 14

```text
Bronze
   ↓
Data Quality
   ↓
 ┌───────┴───────┐
 ▼               ▼
Valid          Invalid
 ▼               ▼
Silver       Quarantine
```

### Day 15

```text
Bronze
   ↓
Unexpected / Unmapped Data
   ↓
_rescued_data
```

Together:

```text
                     SOURCE
                       │
                       ▼
                  AUTO LOADER
                       │
                       ▼
                     BRONZE
                       │
          ┌────────────┴────────────┐
          ▼                         ▼
     Normal data              Unexpected data
          │                         │
          │                         ▼
          │                   _rescued_data
          │
          ▼
     DATA QUALITY
          │
     ┌────┴────┐
     ▼         ▼
   VALID     INVALID
     │         │
     ▼         ▼
  SILVER   QUARANTINE
```

---

# 20. Day 15 Hands-On Exercises

### Exercise 1 - New Column

Create:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt,Email
110,Arun,Chennai,30,2026-08-23 09:00:00,arun@gmail.com
```

Check:

```text
_rescued_data
```

Understand why `Email` does not appear as a normal column when using the explicit schema.

### Exercise 2 - Datatype Mismatch

Create:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
112,Ravi,Chennai,ABC,2026-08-23 10:00:00
```

Inspect:

```text
Age
_rescued_data
```

### Exercise 3 - Missing Value

Create:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
113,Ravi,Chennai,,2026-08-23 10:05:00
```

Compare this with Exercise 2.

Understand:

```text
Missing value
     vs
Invalid datatype
```

### Exercise 4 - Business Data Quality

Create:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
114,Mani,Chennai,-5,2026-08-23 10:10:00
```

Compare it with Exercise 2.

Understand:

```text
ABC
 ↓
Datatype problem

-5
 ↓
Data quality problem
```

---

# 21. Day 15 Architecture

```text
                         CSV FILE
                             │
                             ▼
                       AUTO LOADER
                             │
                             ▼
                      EXPLICIT SCHEMA
                             │
                    ┌────────┴────────┐
                    ▼                 ▼
              Known columns     Unexpected data
                    │                 │
                    ▼                 ▼
                  BRONZE        _rescued_data
                    │
                    ▼
             DATA QUALITY
                    │
              ┌─────┴─────┐
              ▼           ▼
            VALID       INVALID
              │           │
              ▼           ▼
           SILVER     QUARANTINE
```

---

# 22. Day 15 Checklist

- [ ] Create `customers_05.csv`
- [ ] Configure Auto Loader
- [ ] Define explicit schema
- [ ] Enable `_rescued_data`
- [ ] Inspect `printSchema()`
- [ ] Inspect `_rescued_data`
- [ ] Write data to Bronze
- [ ] Query Bronze
- [ ] Understand why `bronze_stream.columns` does not show the unexpected CSV column with an explicit schema
- [ ] Understand new-column handling
- [ ] Understand explicit schema + rescue
- [ ] Understand Auto Loader schema evolution conceptually
- [ ] Test a new column
- [ ] Test a datatype mismatch
- [ ] Compare NULL vs invalid datatype
- [ ] Compare datatype mismatch vs data quality
- [ ] Understand Bronze vs Silver responsibilities
- [ ] Complete all Day 15 exercises

---

# Key Learnings

### Explicit Schema

```text
Known expected structure
        ↓
Controlled ingestion
```

### `_rescued_data`

```text
Unexpected / unmapped source data
        ↓
Preserve it
        ↓
Investigate later
```

### New Column

```text
Source adds Email
        ↓
Not in explicit schema
        ↓
Unexpected data
        ↓
_rescued_data
```

### Datatype Mismatch

```text
Expected Integer
        ↓
Received "ABC"
        ↓
Cannot map normally
```

### Data Quality

```text
Age = -5
        ↓
Valid Integer
        ↓
Invalid business value
        ↓
Quarantine
```

### Bronze

```text
Source-oriented
        ↓
Preserve incoming information
```

### Silver

```text
Business-oriented
        ↓
Controlled and validated
```

---

# Day 15 Final Mental Model

```text
                    SOURCE CSV
                         │
                         ▼
                    AUTO LOADER
                         │
                         ▼
                  EXPLICIT SCHEMA
                         │
                ┌────────┴────────┐
                ▼                 ▼
          Known columns      Unexpected data
                │                 │
                ▼                 ▼
             BRONZE         _rescued_data
                │
                ▼
         DATA QUALITY RULES
                │
          ┌─────┴─────┐
          ▼           ▼
        VALID       INVALID
          │           │
          ▼           ▼
       SILVER      QUARANTINE
```

## Day 15 Outcome

You should now be able to explain:

> **When Auto Loader uses an explicit schema with `_rescued_data`, the DataFrame follows the expected schema and unexpected or unmappable source data can be preserved in `_rescued_data`. A new source column, a datatype mismatch, and a business data-quality failure are different problems and should be handled differently. Bronze preserves source information, while Silver should expose a controlled and validated business schema.**

**Next: [Day 16](../Day16/README.md) - Advanced Delta Lake: SCD Type 1 and SCD Type 2.**
