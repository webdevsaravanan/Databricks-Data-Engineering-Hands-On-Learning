# Databricks Data Engineering - Hands-On Learning

A day-by-day practical journey to learn **Databricks and Data Engineering by building real-world pipelines**, starting from beginner concepts and gradually progressing toward production-style architecture.

## 🎯 Learning Approach

Instead of learning Databricks concepts separately, this repository follows a practical approach:

```text
Learn Concept
     ↓
Build It
     ↓
Test With Real Scenario
     ↓
Understand What Happens
     ↓
Connect It To Data Engineering Architecture
```

The projects progressively build a complete Data Engineering workflow using Databricks.

---

# 📚 Daily Learning Plan

| Day | Topic | Status |
|---|---|---|
| [Day 01](Day01/README.md) | Databricks Fundamentals & Basic DataFrame/Table Operations | ✅ Completed |
| [Day 02](Day02/README.md) | File Ingestion & Bronze Layer | ✅ Completed |
| [Day 03](Day03/README.md) | Bronze → Silver Transformation | ✅ Completed |
| [Day 04](Day04/README.md) | Silver → Gold & Data Joining | ✅ Completed |
| [Day 05](Day05/README.md) | Delta Lake Fundamentals | ✅ Completed |
| [Day 06](Day06/README.md) | Incremental Processing & MERGE / Upsert | ✅ Completed |
| [Day 07](Day07/README.md) | Data Quality & Validation | ✅ Completed |
| [Day 08](Day08/README.md) | Deduplication & Latest Record Processing | ✅ Completed |
| [Day 09](Day09/README.md) | Incremental ETL Pipeline | ✅ Completed |
| [Day 10](Day10/README.md) | Auto Loader & Incremental File Ingestion | ✅ Completed |
| [Day 11](Day11/README.md) | Auto Loader → Bronze → Silver Incremental Pipeline | ✅ Completed |
| [Day 12](Day12/README.md) | Structured Streaming with AvailableNow + foreachBatch | ✅ Completed |
| [Day 13](Day13/README.md) | Complete Incremental Auto Loader → Bronze → Silver Pipeline | ✅ Completed |
| [Day 14](Day14/README.md) | Data Quality & Quarantine | ✅ Completed |
| [Day 15](Day15/README.md) | Schema Evolution & _rescued_data | ✅ Completed |
| [Day 16](Day16/README.md) | SCD Type 1 & SCD Type 2 with Delta Lake | ✅ Completed |
| [Day 17](Day17/README.md) | Streaming SCD Type 2 with Delta Lake | ✅ Completed |
| [Day 18](Day18/README.md) | Data Quality & Quarantine | ✅ Completed |
| [Day 19](Day19/README.md) | Auto Loader + Structured Streaming + Data Quality | ✅ Completed |
| [Day 20](Day20/README.md) | Gold Layer & Business Analytics | ✅ Completed |
| [Day 21](Day21/README.md) | Advanced Delta Lake| ✅ Completed |

---

# 🏗️ Learning Architecture

The learning path is gradually building toward a production-style Medallion Architecture:

```text
                    SOURCE SYSTEMS
                         │
             ┌───────────┼───────────┐
             │           │           │
            CSV        APIs        Streaming
             │           │           │
             └───────────┼───────────┘
                         │
                         ▼
                  INGESTION LAYER
                  Auto Loader / ...
                         │
                         ▼
                     🥉 BRONZE
                  Raw / Append Data
                         │
                         ▼
                 DATA QUALITY
                         │
                         ▼
                  TRANSFORMATION
                         │
                         ▼
                     🥈 SILVER
                 Clean / Conformed
                         │
                         ▼
                  AGGREGATION / JOIN
                         │
                         ▼
                      🥇 GOLD
                Business-Ready Data
                         │
                         ▼
                BI / Analytics / ML
```

---

# 📈 Progress

Current progress:

```text
████████████████░░░░░░░░░░ 21 Days
```

### Completed

- ✅ Databricks workspace fundamentals
- ✅ DataFrames and tables
- ✅ Bronze / Silver / Gold concepts
- ✅ Delta Lake
- ✅ Incremental processing
- ✅ MERGE / Upsert
- ✅ Data quality
- ✅ Window functions and deduplication
- ✅ Incremental ETL
- ✅ Auto Loader
- ✅ Explicit schema handling
- ✅ Schema location
- ✅ Streaming checkpoints
- ✅ `_rescued_data`
- ✅ File-level vs record-level incremental processing
- ✅ SCD Type 1 & SCD Type 2 with Delta Lake
- ✅ Gold Layer & Business Analytics 
- ✅ Advanced Delta Lake

---

# 🧩 Main Technologies

The learning path focuses primarily on:

- **Databricks**
- **Apache Spark / PySpark**
- **Delta Lake**
- **SQL**
- **Structured Streaming**
- **Auto Loader**
- **Medallion Architecture**
- **Data Quality**
- **Incremental ETL**
- **MERGE / Upsert**
- **Window Functions**
- **Schema Management**

Additional technologies will be introduced as the learning progresses.

---

# 🛠️ Hands-On Philosophy

Each day is designed around a practical Data Engineering scenario.

For example:

```text
Customer CSV Files
        ↓
Auto Loader
        ↓
Bronze Delta Table
        ↓
Data Quality
        ↓
Deduplication
        ↓
MERGE
        ↓
Silver
        ↓
Gold
```

The goal is not just to memorize Databricks commands.

The goal is to understand:

> **Why the technology is used, where it fits in a pipeline, what happens internally, and how the pieces work together in a real project.**

---


## Current Status

**Day 21 completed.**

➡️ **Next: Day 22 - Setup + Delta Lake: SCD Type 2 with MERGE INTO **
