# Day 22 SCD Type 2 Customer Dimension with MERGE INTO

Databricks practice · Monday, 2026-10-26 · Medium · ~25 min

## Overview

Apply a staging table of customer updates to a customer dimension as
SCD Type 2: a changed attribute closes the old row with an end date and
opens a new current row, while history stays queryable. A brand-new customer
is added as current, and an unchanged customer is left untouched.

## What you'll practice

- `MERGE INTO` to close changed current rows and insert brand-new keys
- An idempotent second INSERT (NOT EXISTS) for the new version of each
  just-closed row
- Time travel with `VERSION AS OF` to read the dimension as it looked
  before the load

## Prerequisites

- Databricks Free Edition workspace, Unity Catalog enabled
- Run in the SQL editor on the SQL warehouse
- Schema `workspace.dp750_practice` (created in the Day 1 setup check)

## Setup

```sql
CREATE SCHEMA IF NOT EXISTS workspace.dp750_practice;

CREATE OR REPLACE TABLE workspace.dp750_practice.dim_customers (
  customer_id INT,
  customer_name STRING,
  city STRING,
  effective_from DATE,
  effective_to DATE,
  is_current BOOLEAN
);

INSERT INTO workspace.dp750_practice.dim_customers VALUES
  (101, 'Arjun',  'Chennai',   DATE '2026-10-20', NULL, true),
  (102, 'Priya',  'Bengaluru', DATE '2026-10-20', NULL, true),
  (103, 'Vikram', 'Hyderabad', DATE '2026-10-21', NULL, true);

CREATE OR REPLACE TABLE workspace.dp750_practice.stg_customer_updates (
  customer_id INT,
  customer_name STRING,
  city STRING,
  effective_date DATE
);

INSERT INTO workspace.dp750_practice.stg_customer_updates VALUES
  (101, 'Arjun', 'Madurai',    DATE '2026-10-26'),
  (102, 'Priya', 'Bengaluru',  DATE '2026-10-26'),
  (104, 'Divya', 'Coimbatore', DATE '2026-10-26');
```

## Task

Apply the staging table to `dim_customers` as SCD Type 2. Arjun moved
Chennai → Madurai, Divya is brand new, and Priya is unchanged. Then verify
with time travel.

Expected when done: 5 rows in `dim_customers`. Arjun's Chennai row closed
(effective_to 2026-10-25, is_current false) with a new Madurai current row;
Priya's row untouched; Divya added as current. And `VERSION AS OF 1` still
shows the original 3 rows.

## Solution

```sql
-- Step 1: MERGE closes changed current rows, inserts brand-new keys
MERGE INTO workspace.dp750_practice.dim_customers AS t
USING workspace.dp750_practice.stg_customer_updates AS s
  ON t.customer_id = s.customer_id AND t.is_current = true
WHEN MATCHED AND (t.city <> s.city) THEN
  UPDATE SET effective_to = date_add(s.effective_date, -1),
             is_current = false
WHEN NOT MATCHED THEN
  INSERT (customer_id, customer_name, city, effective_from, effective_to, is_current)
  VALUES (s.customer_id, s.customer_name, s.city, s.effective_date, NULL, true);

-- Step 2 (idempotent): new current row for each changed key — only where
-- no current row already matches the new values
INSERT INTO workspace.dp750_practice.dim_customers
  (customer_id, customer_name, city, effective_from, effective_to, is_current)
SELECT s.customer_id, s.customer_name, s.city, s.effective_date, NULL, true
FROM workspace.dp750_practice.stg_customer_updates s
WHERE NOT EXISTS (
  SELECT 1 FROM workspace.dp750_practice.dim_customers t
  WHERE t.customer_id = s.customer_id
    AND t.is_current = true
    AND t.city <=> s.city
);
```

## Verify

```sql
SELECT * FROM workspace.dp750_practice.dim_customers
ORDER BY customer_id, effective_from;

DESCRIBE HISTORY workspace.dp750_practice.dim_customers;
SELECT * FROM workspace.dp750_practice.dim_customers VERSION AS OF 1;
```

Why version 1, not 0: the setup splits creation and loading into two
statements, so version 0 is the empty CREATE TABLE commit and version 1 is
the INSERT with the original 3 rows. (Day 1 differed because a single CTAS
put the data straight into version 0.) If you re-ran the setup, trust
DESCRIBE HISTORY and use the version listed against the INSERT operation.

## Key concepts

- One MERGE cannot both UPDATE and INSERT the same matched row, so the close
  and the new-version insert split across two statements.
- The `is_current = true` condition in the MERGE stops a stale row from being
  closed twice, and Priya's unchanged row survives because the MATCHED
  condition requires an actual city change.
- The NOT EXISTS form of step 2 is idempotent: re-running it inserts nothing,
  because after the first run a current row matching the new values already
  exists. That is the property that makes pipeline retries safe.
- `<=>` is the null-safe equals, so a NULL-to-value change is not missed the
  way `<>` would miss it.
- Every write is a new Delta version, so time travel reads the pre-load state.

## DP-750 mapping

Domain: **Prepare and process data** (Delta Lake, SCD Type 2).
