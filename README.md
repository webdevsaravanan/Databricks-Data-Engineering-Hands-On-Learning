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
| Day 11 | Auto Loader → Bronze → Silver Pipeline | 🔜 Coming Soon |
| Day 12+ | Advanced Data Engineering Topics | 🔜 Coming Soon |

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
██████████░░░░░░░░░░ 10 Days
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

# 🚀 Current Milestone: Day 10

The latest completed milestone is **Auto Loader**.

The current pipeline understands:

```text
New CSV File
     ↓
Auto Loader
     ↓
Explicit Schema
     ↓
_rescued_data
     ↓
Bronze Delta Table
```

And an important distinction:

```text
Auto Loader
     ↓
File-level incremental ingestion

Deduplication + MERGE
     ↓
Record-level incremental processing
```

This distinction will be used heavily in the upcoming Bronze → Silver pipeline.

---

# 🔮 Future Learning

Days 11+ will continue from the existing concepts and progressively introduce more advanced Data Engineering topics.

Potential areas include:

```text
Auto Loader
    ↓
Bronze → Silver
    ↓
Data Quality
    ↓
Deduplication
    ↓
MERGE
    ↓
Streaming
    ↓
Advanced Delta Lake
    ↓
Lakeflow
    ↓
Workflows / Jobs
    ↓
Performance Optimization
    ↓
Unity Catalog
    ↓
Production Architecture
```

The exact topics will be added day by day based on the progression of the hands-on projects.

---

# 📌 Learning Goal

The final objective is to be able to take a real Data Engineering requirement and design and implement a Databricks pipeline from:

```text
Source
  ↓
Ingestion
  ↓
Bronze
  ↓
Data Quality
  ↓
Transformation
  ↓
Silver
  ↓
Business Logic
  ↓
Gold
  ↓
Analytics
```

while understanding the **why, how, and trade-offs** behind each component.

---

## Current Status

**Day 10 completed.**

➡️ **Next: Day 11 - Auto Loader + Bronze → Silver Incremental Pipeline**
