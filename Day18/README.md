# Day 18 - Data Quality & Quarantine

## Objective

Learn how to add a data-quality layer to a Databricks customer pipeline so that valid records can continue to Silver while invalid records are captured separately in a quarantine table.

Today we cover:

- Explicit schema
- Duplicate detection
- Data-quality validation
- `ErrorReason`
- Valid vs invalid DataFrames
- Quarantine records
- Rejection timestamp
- Data-quality metrics
- Rejection-reason analysis
- Reusable validation function

---

## 1. Day 18 Scenario

Incoming customer data contains:

```text
CustomerId
CustomerName
City
Age
UpdatedAt
```

The pipeline should distinguish between:

```text
                    Incoming Data
                          |
                          v
                   Data Validation
                     /          \
                    /            \
                   v              v
                VALID          INVALID
                  |                |
                  v                v
               Silver          Quarantine
```

The purpose is to keep trusted data in Silver while retaining rejected records and the reason for rejection.

---

## 2. Data Quality Rules

The notebook validates:

| Rule | Validation |
|---|---|
| CustomerId | Must not be NULL |
| CustomerName | Must not be NULL or blank |
| City | Must not be NULL or blank |
| Age | Must be between 0 and 100 |
| UpdatedAt | Must not be NULL |
| CustomerId | Duplicate IDs in the batch are identified |

---

## 3. Input Test Data

The exercise uses intentionally valid and invalid customer records.

Example:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
105,Ravi,Chennai,28,2026-08-30 09:00:00
106,,Bangalore,35,2026-08-30 09:01:00
107,Arun,Chennai,-5,2026-08-30 09:02:00
108,Priya,Chennai,150,2026-08-30 09:03:00
108,Priya,Chennai,30,2026-08-30 09:04:00
109,Meena,Madurai,29,2026-08-30 09:05:00
```

---

## 4. Define Explicit Schema

```python
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

Check:

```python
df.printSchema()
```

Expected:

```text
CustomerId    integer
CustomerName  string
City          string
Age           integer
UpdatedAt     timestamp
```

---

## 5. Read the CSV

```python
df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(customer_schema)
    .load("/Volumes/workspace/bronze/day18_customer_files/day18_customers_01.csv")
)
```

Display:

```python
display(df)
```

---

## 6. Detect Duplicate Customer IDs

```python
duplicate_ids = (
    df.groupBy("CustomerId")
      .count()
      .filter(F.col("count") > 1)
)
```

Display:

```python
display(duplicate_ids)
```

For the test data, `CustomerId = 108` appears more than once.

---

## 7. Add Duplicate Flag

```python
df_with_duplicate_flag = (
    df.join(
        duplicate_ids.select("CustomerId").withColumn("IsDuplicate", F.lit(True)),
        on="CustomerId",
        how="left"
    )
    .fillna({"IsDuplicate": False})
)
```

Now each record can be checked for duplicate status.

---

## 8. Create Validation Conditions

Import functions:

```python
from pyspark.sql import functions as F
```

CustomerId:

```python
customer_id_error = F.col("CustomerId").isNull()
```

CustomerName:

```python
customer_name_error = (
    F.col("CustomerName").isNull() |
    (F.trim(F.col("CustomerName")) == "")
)
```

City:

```python
city_error = (
    F.col("City").isNull() |
    (F.trim(F.col("City")) == "")
)
```

Age:

```python
age_error = (
    F.col("Age").isNull() |
    (F.col("Age") < 0) |
    (F.col("Age") > 100)
)
```

UpdatedAt:

```python
updated_at_error = F.col("UpdatedAt").isNull()
```

Duplicate:

```python
duplicate_error = F.col("IsDuplicate") == True
```

---

## 9. Create ErrorReason

```python
validated_df = (
    df_with_duplicate_flag
    .withColumn(
        "ErrorReason",
        F.concat_ws(
            "; ",
            F.when(customer_id_error, "CustomerId is NULL"),
            F.when(customer_name_error, "CustomerName is NULL"),
            F.when(city_error, "City is NULL"),
            F.when(age_error, "Age is outside valid range"),
            F.when(updated_at_error, "UpdatedAt is NULL"),
            F.when(duplicate_error, "Duplicate CustomerId")
        )
    )
)
```

Display:

```python
display(validated_df)
```

`ErrorReason` can contain multiple reasons when a record violates more than one rule.

---

## 10. Separate Valid Records

```python
valid_df = (
    validated_df
    .filter(
        F.col("ErrorReason").isNull() |
        (F.col("ErrorReason") == "")
    )
    .drop("IsDuplicate", "ErrorReason")
)
```

Display:

```python
display(valid_df)
```

These records are eligible to continue into Silver.

---

## 11. Separate Invalid Records

```python
invalid_df = (
    validated_df
    .filter(
        F.col("ErrorReason").isNotNull() &
        (F.col("ErrorReason") != "")
    )
    .drop("IsDuplicate")
)
```

Display:

```python
display(invalid_df)
```

These records are retained for quarantine.

---

## 12. Add Rejection Timestamp

```python
invalid_df = invalid_df.withColumn(
    "RejectedAt",
    F.current_timestamp()
)
```

The quarantine data now contains the source columns plus:

```text
ErrorReason
RejectedAt
```

---

## 13. Create Silver Table

```sql
CREATE TABLE IF NOT EXISTS silver.day18_customers (
    CustomerId INT,
    CustomerName STRING,
    City STRING,
    Age INT,
    UpdatedAt TIMESTAMP
)
USING DELTA;
```

---

## 14. Write Valid Data to Silver

```python
valid_df.write.mode("append").saveAsTable("silver.day18_customers")
```

Verify:

```sql
SELECT *
FROM silver.day18_customers
ORDER BY CustomerId;
```

Only valid records should reach this table.

---

## 15. Create Quarantine Table

```sql
CREATE TABLE IF NOT EXISTS silver.day18_customer_quarantine (
    CustomerId INT,
    CustomerName STRING,
    City STRING,
    Age INT,
    UpdatedAt TIMESTAMP,
    ErrorReason STRING,
    RejectedAt TIMESTAMP
)
USING DELTA;
```

---

## 16. Write Invalid Data to Quarantine

```python
invalid_df.write.mode("append").saveAsTable("silver.day18_customer_quarantine")
```

Verify:

```sql
SELECT *
FROM silver.day18_customer_quarantine
ORDER BY CustomerId;
```

The rejected records are now preserved with their rejection reason.

---

## 17. Calculate Data Quality Metrics

```python
valid_count = valid_df.count()
invalid_count = invalid_df.count()
```

Display:

```python
print(f"Valid records   : {valid_count}")
print(f"Invalid records : {invalid_count}")
```

Calculate the quality percentage:

```python
total_count = valid_count + invalid_count

quality_percentage = (
    valid_count / total_count * 100
)

print(f"Data Quality : {quality_percentage:.2f}%")
```

Formula:

```text
Data Quality %
=
Valid Records / Total Records × 100
```

---

## 18. Analyze Rejection Reasons

```python
(
    invalid_df
    .groupBy("ErrorReason")
    .count()
    .orderBy(F.desc("count"))
    .display()
)
```

This helps identify which validation rules are causing the most rejected records.

---

## 19. Create Reusable Validation Function

The notebook consolidates the validation logic into:

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
        .withColumn("RejectedAt", F.current_timestamp())
    )

    return valid_df, invalid_df
```

---

## 20. Test the Validation Function

```python
valid_df, invalid_df = validate_customers(df)
```

Display valid records:

```python
display(valid_df)
```

Display invalid records:

```python
display(invalid_df)
```

The function returns:

```text
validate_customers(df)
        |
        +----> valid_df
        |
        +----> invalid_df
```

---

## 21. Test Another Input File

The notebook also demonstrates applying the validation logic to another customer input file.

Example:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
110,Suresh,Chennai,32,2026-08-30 10:00:00
111,Anitha,Coimbatore,26,2026-08-30 10:01:00
112,John,Bangalore,40,2026-08-30 10:02:00
```

Read it using the same schema and apply:

```python
valid_df, invalid_df = validate_customers(df2)
```

This demonstrates that the validation logic can be reused for subsequent batches.

---

## 22. Test Multiple Validation Errors

A record can violate multiple rules at the same time.

Example:

```csv
CustomerId,CustomerName,City,Age,UpdatedAt
113,,,-10,
```

It violates several rules:

```text
CustomerName is NULL
City is NULL
Age is outside valid range
UpdatedAt is NULL
```

The `ErrorReason` column can capture the applicable reasons together.

---

## 23. Day 17 → Day 18

### Day 17

```text
Bronze
   ↓
foreachBatch
   ↓
batch_df
   ↓
SCD Type 2
   ↓
Silver
```

### Day 18

```text
Bronze / Batch Data
        ↓
Data Quality Validation
        ↓
   +----+----+
   |         |
   v         v
 VALID     INVALID
   |         |
   v         v
 Silver   Quarantine
```

Data quality becomes a gate before trusted Silver processing.

---

## 24. Production Architecture

```text
                         SOURCE
                            |
                            v
                         BRONZE
                            |
                            v
                    DATA VALIDATION
                            |
                 +----------+----------+
                 |                     |
                 v                     v
              VALID                 INVALID
                 |                     |
                 v                     v
          BUSINESS LOGIC          QUARANTINE
                 |                     |
                 v                     v
              SILVER              ErrorReason
                                       |
                                       v
                                  Investigation
```

A production implementation can additionally capture metadata such as:

```text
BatchId
SourceFile
SourceSystem
IngestionTimestamp
RuleName
ErrorReason
RejectedAt
PipelineRunId
```

The notebook establishes the core validation/quarantine pattern; these additional metadata fields are production extensions rather than additional notebook steps.

---

## 25. Day 18 Checklist

- [ ] Create Day 18 customer input data
- [ ] Define explicit customer schema
- [ ] Read CSV with explicit schema
- [ ] Detect duplicate CustomerIds
- [ ] Add duplicate flag
- [ ] Create CustomerId validation
- [ ] Create CustomerName validation
- [ ] Create City validation
- [ ] Create Age validation
- [ ] Create UpdatedAt validation
- [ ] Build `ErrorReason`
- [ ] Separate valid records
- [ ] Separate invalid records
- [ ] Add `RejectedAt`
- [ ] Create Silver table
- [ ] Write valid records to Silver
- [ ] Create quarantine table
- [ ] Write invalid records to quarantine
- [ ] Calculate valid/invalid counts
- [ ] Calculate data-quality percentage
- [ ] Analyze rejection reasons
- [ ] Create reusable `validate_customers()` function
- [ ] Test the validation function
- [ ] Test another input file
- [ ] Test multiple validation errors

---

# Key Learnings

### Data Quality

Validation prevents invalid source records from silently becoming trusted Silver data.

### Quarantine

Invalid records are retained separately instead of being discarded.

### ErrorReason

```text
ErrorReason
```

explains why a record failed validation.

### Multiple Errors

A single record can contain multiple validation failures, and `concat_ws()` allows the reasons to be combined.

### Duplicate Detection

`groupBy()` + `count()` can identify duplicate business keys within the incoming batch.

### Reusable Validation

A function such as:

```python
validate_customers(df)
```

allows the same validation rules to be applied to multiple input batches.

### Data Quality Metrics

Valid/invalid counts and the quality percentage provide basic visibility into source-data quality.

---

# Final Mental Model

```text
                         SOURCE
                            |
                            v
                         BRONZE
                            |
                            v
                    VALIDATION LAYER
                            |
              +-------------+-------------+
              |                           |
              v                           v
         VALID RECORDS              INVALID RECORDS
              |                           |
              v                           v
        BUSINESS LOGIC              QUARANTINE
              |                           |
              v                           v
            SILVER                  ErrorReason
                                        |
                                        v
                                  Investigation
```

The key separation is:

```text
Bronze
  =
Incoming/source data

Silver
  =
Trusted/business-ready data

Quarantine
  =
Rejected data retained for investigation
```

---

# Outcome

After completing Day 18, you should be able to explain and implement:

> **A PySpark data-quality layer that validates incoming customer records, identifies invalid values and duplicates, separates valid and invalid records, writes trusted records to Silver, and stores rejected records in a quarantine Delta table with an explanation of why they were rejected.**
