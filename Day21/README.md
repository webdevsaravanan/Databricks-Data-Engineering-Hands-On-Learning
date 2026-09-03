# Day 21 Advanced Delta Lake

## Objective

Develop a deeper understanding of Delta Lake beyond basic table
operations.

By the end of Day 21, you should be able to:

-   Understand Delta table history and versions
-   Use Delta Lake Time Travel
-   Understand transaction history
-   Understand table maintenance
-   Understand `OPTIMIZE`
-   Understand Z-Ordering conceptually
-   Understand `VACUUM`
-   Understand the relationship between Time Travel and retained files
-   Investigate accidental data changes
-   Apply Delta Lake maintenance concepts in production scenarios

------------------------------------------------------------------------

# 1. Scenario

You are working as a Data Engineer for **RetailMart**.

The pipeline currently follows:

``` text
Source
   ↓
Bronze
   ↓
Silver
   ↓
Gold
```


Today, the focus moves from **building Delta tables** to **understanding
and maintaining them in production**.

------------------------------------------------------------------------

# 2. Day 21 Focus

The roadmap defines Day 21 around:

-   Advanced Delta Lake capabilities
-   Table maintenance
-   Performance-related Delta features
-   Time Travel
-   Version history
-   Optimization concepts
-   Production considerations

The goal is to understand Delta Lake beyond basic table operations.

------------------------------------------------------------------------

# 3. Delta Lake Architecture

A Delta table consists of data files together with transaction
information.

``` text
                         DELTA TABLE
                              │
               ┌──────────────┴──────────────┐
               │                             │
               ▼                             ▼
          Data Files                    _delta_log
           Parquet                    Transaction Log
               │                             │
               │                    ┌────────┼────────┐
               │                    ▼        ▼        ▼
               │                 Version  Version  Version
               │                    0        1        2
               │
               ▼
          Current Table State
```

The transaction log records changes that allow Delta Lake to determine
the state of the table at different versions.

------------------------------------------------------------------------

# 4. Prerequisites

Use a practice table instead of experimenting with destructive
operations on important production tables.

For today's lab, use the Gold table created during the previous
exercise:

``` text
gold.day20_order_details
```

First verify it exists:

``` sql
SHOW TABLES IN gold;
```

Then:

``` sql
SELECT *
FROM gold.day20_order_details;
```

------------------------------------------------------------------------

# 5. Create the Day 21 Practice Table

Create a separate Delta table for today's experiments.

``` sql
CREATE OR REPLACE TABLE gold.day21_delta_lab AS
SELECT *
FROM gold.day20_order_details;
```

Verify:

``` sql
SELECT *
FROM gold.day21_delta_lab;
```

Check the table:

``` sql
DESCRIBE DETAIL gold.day21_delta_lab;
```

Look for:

``` text
format = delta
```

------------------------------------------------------------------------

# 6. Understand Delta Table History

Delta Lake records table operations in its transaction history.

Run:

``` sql
DESCRIBE HISTORY gold.day21_delta_lab;
```

Important columns include:

``` text
version
timestamp
operation
operationParameters
userName
```

The `version` represents a committed state of the Delta table.

Mental model:

``` text
Version 0
   ↓
INSERT / CREATE
   ↓
Version 1
   ↓
UPDATE
   ↓
Version 2
   ↓
DELETE
   ↓
Version 3
```

------------------------------------------------------------------------

# 7. Hands-on --- Identify Table Versions

Run:

``` sql
DESCRIBE HISTORY gold.day21_delta_lab;
```

### Task

Identify:

-   Latest version
-   Earliest available version
-   Operation associated with each version
-   Timestamp of each operation

Do not simply look at the latest version.

Understand how the table changed over time.

------------------------------------------------------------------------

# 8. Time Travel

Time Travel allows you to query a previous version of a Delta table.

Syntax:

``` sql
SELECT *
FROM table_name VERSION AS OF version_number;
```

Example:

``` sql
SELECT *
FROM gold.day21_delta_lab VERSION AS OF 0;
```

Current table:

``` sql
SELECT *
FROM gold.day21_delta_lab;
```

Historical table:

``` sql
SELECT *
FROM gold.day21_delta_lab VERSION AS OF 0;
```

These queries can represent different states of the same Delta table.

------------------------------------------------------------------------

# 9. Time Travel Using Timestamp

Delta Lake also supports querying using a timestamp.

Syntax:

``` sql
SELECT *
FROM table_name
TIMESTAMP AS OF 'timestamp';
```

First inspect the actual timestamps:

``` sql
DESCRIBE HISTORY gold.day21_delta_lab;
```

Then use one of the timestamps returned by the history.

Example:

``` sql
SELECT *
FROM gold.day21_delta_lab
TIMESTAMP AS OF '2026-09-03 20:00:00';
```

Use an actual timestamp from your table history rather than blindly
using the example.

------------------------------------------------------------------------

# 10. Version vs Timestamp

Two common Time Travel approaches are:

``` text
VERSION AS OF
```

and

``` text
TIMESTAMP AS OF
```

### Version

``` sql
SELECT *
FROM gold.day21_delta_lab
VERSION AS OF 1;
```

You explicitly identify the Delta table version.

### Timestamp

``` sql
SELECT *
FROM gold.day21_delta_lab
TIMESTAMP AS OF '2026-09-03 20:00:00';
```

You identify the historical state by time.

Mental model:

``` text
Version
   ↓
Exact transaction state

Timestamp
   ↓
State as of a point in time
```

------------------------------------------------------------------------

# 11. Hands-on --- UPDATE

Choose a test record.

First inspect it:

``` sql
SELECT *
FROM gold.day21_delta_lab
WHERE OrderId = 1001;
```

Update it:

``` sql
UPDATE gold.day21_delta_lab
SET OrderAmount = OrderAmount + 100
WHERE OrderId = 1001;
```

Verify:

``` sql
SELECT *
FROM gold.day21_delta_lab
WHERE OrderId = 1001;
```

Now inspect history:

``` sql
DESCRIBE HISTORY gold.day21_delta_lab;
```

Observe the new version.

Mental model:

``` text
Previous State
      ↓
    UPDATE
      ↓
New Delta Version
```

------------------------------------------------------------------------

# 12. Hands-on --- Query the Previous State

Find the version immediately before the update.

Then query:

``` sql
SELECT *
FROM gold.day21_delta_lab VERSION AS OF <previous_version>
WHERE OrderId = 1001;
```

Compare with the current value:

``` sql
SELECT *
FROM gold.day21_delta_lab
WHERE OrderId = 1001;
```

### Task

Explain:

-   What was the original value?
-   What is the current value?
-   Which Delta version contains the original value?

------------------------------------------------------------------------

# 13. Hands-on --- DELETE

Delete another test record:

``` sql
DELETE FROM gold.day21_delta_lab
WHERE OrderId = 1002;
```

Verify:

``` sql
SELECT *
FROM gold.day21_delta_lab
WHERE OrderId = 1002;
```

The record should no longer exist in the current state.

Now inspect history:

``` sql
DESCRIBE HISTORY gold.day21_delta_lab;
```

Identify the version created by the DELETE.

------------------------------------------------------------------------

# 14. Recover the Deleted Record Using Time Travel

Find the version immediately before the DELETE.

Then:

``` sql
SELECT *
FROM gold.day21_delta_lab VERSION AS OF <previous_version>
WHERE OrderId = 1002;
```

You should be able to see the record in the historical state.

### Important

Time Travel lets you **read a previous state**.

It does not automatically mean that the deleted record has been restored
into the current table.

------------------------------------------------------------------------

# 15. Hands-on --- INSERT

Insert a test record using a schema-compatible row.

Example:

``` sql
INSERT INTO gold.day21_delta_lab
SELECT *
FROM gold.day21_delta_lab
LIMIT 1;
```

For learning purposes, the exact inserted data is less important than
observing the Delta transaction.

Check:

``` sql
DESCRIBE HISTORY gold.day21_delta_lab;
```

Observe how another transaction creates another table version.

------------------------------------------------------------------------

# 16. Build a Version Timeline

After performing the exercises, create a timeline like:

``` text
Version 0
   │
   ├── Initial table
   │
Version 1
   │
   ├── UPDATE
   │
Version 2
   │
   ├── DELETE
   │
Version 3
   │
   └── INSERT
```

Your actual version numbers may differ.

The important point is to understand that Delta operations create
committed table states.

------------------------------------------------------------------------

# 17. Compare Current and Historical Data

Current:

``` sql
SELECT *
FROM gold.day21_delta_lab;
```

Historical:

``` sql
SELECT *
FROM gold.day21_delta_lab VERSION AS OF <version>;
```

### Challenge

Choose a historical version and identify:

-   Records that were added later
-   Records that disappeared later
-   Values that changed
-   The transaction responsible for the change

This is a useful troubleshooting pattern.

------------------------------------------------------------------------

# 18. Delta Transaction Log Mental Model

The transaction log is central to Delta Lake.

``` text
                    _delta_log
                         │
              ┌──────────┼──────────┐
              │          │          │
           Version 0  Version 1  Version 2
              │          │          │
              ▼          ▼          ▼
           Initial     UPDATE     DELETE
              │          │          │
              └──────────┼──────────┘
                         │
                         ▼
                  Current Table State
```

Think of the transaction log as the record of how the table changed over
time.

------------------------------------------------------------------------

# 19. OPTIMIZE

Delta tables can accumulate many data files.

A table with many small files can create unnecessary file-management
overhead and affect query performance.

`OPTIMIZE` is a table-maintenance operation that improves the physical
organization of Delta data files.

Run:

``` sql
OPTIMIZE gold.day21_delta_lab;
```

Then:

``` sql
DESCRIBE HISTORY gold.day21_delta_lab;
```

Observe the operation recorded in the history.

------------------------------------------------------------------------

# 20. OPTIMIZE Mental Model

``` text
Many small files
       │
       ▼
   OPTIMIZE
       │
       ▼
Better-organized files
       │
       ▼
Potentially more efficient reads
```

Important:

> `OPTIMIZE` is about physical data layout and file organization. It is
> not a replacement for good query design.

------------------------------------------------------------------------

# 21. Z-Ordering Concept

Z-Ordering is a data-layout optimization technique that can help queries
that frequently filter on particular columns.

Example:

``` sql
OPTIMIZE gold.day21_delta_lab
ZORDER BY (CustomerId);
```

The goal is to improve data locality for suitable query patterns.

Mental model:

``` text
Frequently filtered column
          ↓
Better data organization
          ↓
Potentially less data read
          ↓
Better query performance
```

Do not think of Z-Ordering as:

``` text
ZORDER = every query becomes faster
```

The benefit depends on the workload and query patterns.

------------------------------------------------------------------------

# 22. When Should You Think About OPTIMIZE?

Think about optimization when:

``` text
Table receives frequent writes
        ↓
Many files are created
        ↓
File layout becomes inefficient
        ↓
Maintenance is required
```

Production optimization should be based on actual workload and observed
performance rather than running maintenance commands blindly.

------------------------------------------------------------------------

# 23. VACUUM

Delta tables can contain old files that are no longer required for the
current table state.

`VACUUM` removes obsolete files according to the configured retention
rules.

Conceptually:

``` text
Current files
      +
Obsolete files
      ↓
    VACUUM
      ↓
Remove eligible obsolete files
```

------------------------------------------------------------------------

# 24. Important Time Travel + VACUUM Relationship

This is one of the most important concepts of Day 21.

``` text
Time Travel
     ↓
Needs historical table state
     ↓
Historical state may depend on older files
     ↓
VACUUM removes eligible obsolete files
     ↓
Older historical states may no longer be queryable
```

Therefore:

> Do not treat Time Travel as an unlimited backup mechanism.

Historical data availability depends on retention and whether the
underlying files required for that historical state are still available.

------------------------------------------------------------------------

# 25. Do Not Run Aggressive VACUUM Blindly

For today's lab, do not use an aggressive retention setting on an
important table just to experiment.

Before running `VACUUM`, understand:

-   Required historical retention
-   Recovery requirements
-   Time Travel requirements
-   Operational policies
-   Table usage
-   Data lifecycle requirements

Production maintenance should be deliberate.

------------------------------------------------------------------------

# 26. Production Recovery Scenario

Imagine a developer accidentally executes:

``` sql
DELETE FROM gold.day21_delta_lab;
```

The table now appears empty.

### Investigation process

First:

``` sql
DESCRIBE HISTORY gold.day21_delta_lab;
```

Identify the DELETE transaction.

Then identify the version immediately before the bad operation.

Query it:

``` sql
SELECT *
FROM gold.day21_delta_lab VERSION AS OF <previous_version>;
```

Validate the historical data.

Mental model:

``` text
Accidental change
       ↓
DESCRIBE HISTORY
       ↓
Identify bad transaction
       ↓
Find previous version
       ↓
Query historical state
       ↓
Validate data
       ↓
Choose recovery approach
```

------------------------------------------------------------------------

# 27. Time Travel Is Not the Same as Restore

These concepts should be separated.

### Time Travel

``` text
Read a previous state
```

Example:

``` sql
SELECT *
FROM gold.day21_delta_lab VERSION AS OF 2;
```

### Restore

``` text
Make a previous state the current state
```

For supported environments, Delta provides restore capabilities, but
recovery should be performed carefully and validated before changing
production data.

------------------------------------------------------------------------

# 28. Production Considerations

When working with Delta Lake in production, think about:

### History

``` text
What happened to the table?
```

Use:

``` sql
DESCRIBE HISTORY
```

### Historical Reads

``` text
What did the table look like previously?
```

Use:

``` text
VERSION AS OF
TIMESTAMP AS OF
```

### Physical Maintenance

``` text
Are files organized efficiently?
```

Use appropriate maintenance such as:

``` text
OPTIMIZE
```

### File Cleanup

``` text
Which obsolete files can be safely removed?
```

Use:

``` text
VACUUM
```

with appropriate retention and operational controls.

------------------------------------------------------------------------

# 29. Common Mistakes

## Mistake 1 --- Treating Time Travel as Backup

Time Travel is useful for historical reads and investigation, but it
should not automatically be treated as a permanent backup strategy.

------------------------------------------------------------------------

## Mistake 2 --- Running VACUUM Aggressively

Removing files too aggressively can reduce the availability of older
table states.

------------------------------------------------------------------------

## Mistake 3 --- Optimizing Everything

Do not assume every table needs constant optimization.

Consider:

``` text
Table size
Write frequency
File sizes
Query workload
Observed performance
```

------------------------------------------------------------------------

## Mistake 4 --- Ignoring History

When investigating a data problem, don't immediately start changing the
table.

First:

``` sql
DESCRIBE HISTORY table_name;
```

Understand what happened.

------------------------------------------------------------------------

## Mistake 5 --- Confusing Logical State With Physical Files

Think in two layers:

``` text
Logical table state
        ↓
Transaction log

Physical storage
        ↓
Data files
```

Delta Lake uses both to provide its table behavior.

------------------------------------------------------------------------

# 30. Hands-on Challenge

Complete this section without copying the commands from above.

## Challenge 1 --- History

Create:

``` text
gold.day21_delta_lab
```

and identify its initial version.

------------------------------------------------------------------------

## Challenge 2 --- Update

Update one record.

Then identify:

``` text
Previous version
New version
Operation
Timestamp
```

------------------------------------------------------------------------

## Challenge 3 --- Time Travel

Query the record before the UPDATE.

Explain the difference between:

``` text
Current state
Historical state
```

------------------------------------------------------------------------

## Challenge 4 --- Delete

Delete one test record.

Use Time Travel to prove that the record existed in the previous
version.

------------------------------------------------------------------------

## Challenge 5 --- Insert

Insert a new record.

Inspect the transaction history.

------------------------------------------------------------------------

## Challenge 6 --- Version Timeline

Build a timeline:

``` text
Version
Operation
Timestamp
Purpose
```

for every version in your practice table.

------------------------------------------------------------------------

## Challenge 7 --- Optimize

Run:

``` sql
OPTIMIZE gold.day21_delta_lab;
```

Then inspect:

``` sql
DESCRIBE HISTORY gold.day21_delta_lab;
```

Explain what changed.

------------------------------------------------------------------------

## Challenge 8 --- Recovery Investigation

Simulate an accidental DELETE.

Use:

``` sql
DESCRIBE HISTORY
```

and Time Travel to identify the previous valid state.

Do not modify the production table.

------------------------------------------------------------------------

# 31. Validation Checklist

### Table

-   [ ] Verify `gold.day21_delta_lab` exists
-   [ ] Verify it is a Delta table
-   [ ] Query current data

### History

-   [ ] Run `DESCRIBE HISTORY`
-   [ ] Identify versions
-   [ ] Identify operations
-   [ ] Identify timestamps

### Time Travel

-   [ ] Query using `VERSION AS OF`
-   [ ] Query using `TIMESTAMP AS OF`
-   [ ] Compare historical and current data

### Transactions

-   [ ] Perform UPDATE
-   [ ] Perform DELETE
-   [ ] Perform INSERT
-   [ ] Observe new Delta versions

### Optimization

-   [ ] Understand `OPTIMIZE`
-   [ ] Run `OPTIMIZE` on the practice table
-   [ ] Inspect the operation in history
-   [ ] Understand Z-Ordering concept

### Cleanup

-   [ ] Understand `VACUUM`
-   [ ] Understand retention
-   [ ] Understand Time Travel limitations after file cleanup
-   [ ] Avoid aggressive VACUUM on important tables

### Production

-   [ ] Investigate accidental changes using history
-   [ ] Query historical state
-   [ ] Understand recovery considerations
-   [ ] Understand logical state vs physical files

------------------------------------------------------------------------

# Key Learnings

## Delta History

``` text
Every important table change
        ↓
Transaction history
        ↓
Versioned table state
```

## Time Travel

``` text
Current table
      ↓
Historical version
      ↓
Read previous state
```

## OPTIMIZE

``` text
Data files
      ↓
Physical organization
      ↓
Potentially better read efficiency
```

## Z-Ordering

``` text
Frequently filtered columns
        ↓
Better data locality
        ↓
Potentially less data read
```

## VACUUM

``` text
Obsolete files
      ↓
Retention rules
      ↓
VACUUM
      ↓
Remove eligible files
```

## Production Recovery

``` text
Bad transaction
      ↓
DESCRIBE HISTORY
      ↓
Previous version
      ↓
Time Travel
      ↓
Validate
      ↓
Recovery decision
```

------------------------------------------------------------------------

# Day 21 Final Mental Model

``` text
                         DELTA LAKE
                              │
              ┌───────────────┼────────────────┐
              │               │                │
              ▼               ▼                ▼
          TRANSACTIONS     HISTORY          DATA FILES
              │               │                │
              │               │                │
              ▼               ▼                ▼
            UPDATE          VERSION          STORAGE
            DELETE          TIMESTAMP
            INSERT
              │               │
              └───────┬───────┘
                      ▼
                 TIME TRAVEL
                      │
                      ▼
              Historical State
                      │
                      ▼
                Investigation
                      │
                      ▼
                 Recovery

                      +

                TABLE MAINTENANCE
                      │
             ┌────────┴────────┐
             ▼                 ▼
          OPTIMIZE           VACUUM
             │                 │
             ▼                 ▼
       Better physical    Remove eligible
          layout          obsolete files
```

The core mental model is:

> **Delta Lake maintains a versioned logical table state through its
> transaction history while storing data in files. Time Travel lets you
> read historical states, `OPTIMIZE` improves physical data
> organization, and `VACUUM` removes eligible obsolete files. Production
> engineers must understand how these capabilities interact before
> performing maintenance or recovery operations.**

------------------------------------------------------------------------

# Day 21 Outcome

By completing Day 21, you should be able to explain:

> **Delta Lake goes beyond basic table storage by maintaining
> transaction history and versioned table states. You can inspect
> history, query previous versions using Time Travel, investigate
> accidental changes, and understand table-maintenance operations such
> as `OPTIMIZE` and `VACUUM`. You should also understand that historical
> availability depends on retention and that Time Travel should not be
> treated as an unlimited backup mechanism.**

------------------------------------------------------------------------

**Next:** Day 22 --- Databricks Jobs / Workflows
