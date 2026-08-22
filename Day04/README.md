# Day 04 - Multi-Table ETL & Joins

## Project
RetailMart Data Engineering Platform

## Objective
Build a multi-table ETL pipeline using PySpark.

Today we move from a single customer dataset to a realistic retail scenario containing Customers, Products, and Orders. The goal is to join Silver tables, perform business transformations, and create Gold tables.

## Architecture

```text
Bronze
   ↓
Silver
   ↓
Join + Transformation
   ↓
Gold
```

```text
Silver Customers ──┐
Silver Products  ──┼──> PySpark Joins ──> Gold
Silver Orders    ──┘
```

## Databricks Objects

```text
Catalog
│
├── bronze
│   └── customers
│
├── silver
│   ├── customers
│   ├── products
│   └── orders
│
└── gold
    ├── order_details
    └── customer_revenue
```

## Concepts Learned

- Multi-table ETL
- PySpark joins
- Inner joins
- Join conditions
- DataFrames
- `select()`
- `withColumn()`
- `groupBy()`
- `agg()`
- `sum()`
- Business transformations
- Gold layer
- Medallion Architecture

# 1. Create Product Data

```python
products = [
    (101, "Laptop", "Electronics", 60000),
    (102, "Mobile", "Electronics", 30000),
    (103, "Headphones", "Accessories", 3000),
    (104, "Keyboard", "Accessories", 2000),
    (105, "Monitor", "Electronics", 15000)
]

product_columns = [
    "ProductId",
    "ProductName",
    "Category",
    "Price"
]

products_df = spark.createDataFrame(
    products,
    product_columns
)

display(products_df)
```

# 2. Create Product Silver Table

```sql
CREATE SCHEMA IF NOT EXISTS silver;
```

```python
products_df.write.mode("overwrite").saveAsTable("silver.products")
```

Verify:

```sql
SELECT *
FROM silver.products;
```

# 3. Create Orders Data

```python
orders = [
    (1001, 1, 101, 1, "2026-08-01"),
    (1002, 2, 102, 2, "2026-08-01"),
    (1003, 3, 103, 3, "2026-08-02"),
    (1004, 1, 104, 2, "2026-08-02"),
    (1005, 4, 105, 1, "2026-08-03"),
    (1006, 5, 101, 1, "2026-08-03"),
    (1007, 2, 103, 2, "2026-08-04")
]

order_columns = [
    "OrderId",
    "CustomerId",
    "ProductId",
    "Quantity",
    "OrderDate"
]

orders_df = spark.createDataFrame(
    orders,
    order_columns
)

display(orders_df)
```

# 4. Create Orders Silver Table

```python
orders_df.write.mode("overwrite").saveAsTable("silver.orders")
```

Verify:

```sql
SELECT *
FROM silver.orders;
```

# 5. Understand the Data Model

### Customers

```text
CustomerId
CustomerName
Email
City
Age
```

### Products

```text
ProductId
ProductName
Category
Price
```

### Orders

```text
OrderId
CustomerId
ProductId
Quantity
OrderDate
```

Relationships:

```text
Customer
   │
   │ CustomerId
   ▼
Orders
   │
   │ ProductId
   ▼
Product
```

# 6. Read the Three Silver Tables

```python
customers_df = spark.table("silver.customers")
products_df = spark.table("silver.products")
orders_df = spark.table("silver.orders")
```

# 7. Join Orders with Customers

```python
order_customer_df = orders_df.join(
    customers_df,
    orders_df.CustomerId == customers_df.CustomerId,
    "inner"
)

display(order_customer_df)
```

# 8. Join Products

```python
order_details_df = order_customer_df.join(
    products_df,
    order_customer_df.ProductId == products_df.ProductId,
    "inner"
)

display(order_details_df)
```

The resulting DataFrame contains Order + Customer + Product information.

# 9. Select Required Columns

```python
order_details_df = order_details_df.select(
    orders_df.OrderId,
    customers_df.CustomerId,
    customers_df.CustomerName,
    customers_df.City,
    products_df.ProductId,
    products_df.ProductName,
    products_df.Category,
    products_df.Price,
    orders_df.Quantity,
    orders_df.OrderDate
)

display(order_details_df)
```

# 10. Calculate Order Amount

Business rule:

```text
OrderAmount = Price × Quantity
```

```python
from pyspark.sql.functions import col

order_details_df = order_details_df.withColumn(
    "OrderAmount",
    col("Price") * col("Quantity")
)

display(order_details_df)
```

# 11. Create the Gold Schema

```sql
CREATE SCHEMA IF NOT EXISTS gold;
```

# 12. Create Gold Order Details Table

```python
order_details_df.write.mode("overwrite").saveAsTable("gold.order_details")
```

Verify:

```sql
SELECT *
FROM gold.order_details;
```

# 13. Calculate Customer Revenue

Business requirement:

> How much revenue did each customer generate?

```python
from pyspark.sql.functions import sum

customer_revenue_df = order_details_df.groupBy(
    "CustomerId",
    "CustomerName"
).agg(
    sum("OrderAmount").alias("TotalRevenue")
)

display(customer_revenue_df)
```

# 14. Create Customer Revenue Gold Table

```python
customer_revenue_df.write.mode("overwrite").saveAsTable("gold.customer_revenue")
```

Verify:

```sql
SELECT *
FROM gold.customer_revenue;
```

# 15. Example Business Result

| OrderId | Customer | Product | Qty | Price | OrderAmount |
|---:|---|---|---:|---:|---:|
| 1001 | Saravana | Laptop | 1 | 60000 | 60000 |
| 1002 | John | Mobile | 2 | 30000 | 60000 |
| 1003 | Alice | Headphones | 3 | 3000 | 9000 |
| 1004 | Saravana | Keyboard | 2 | 2000 | 4000 |

# 16. Final Architecture

```text
                    SOURCE
                       │
              ┌────────┼────────┐
              │        │        │
          Customer   Product   Order
              │        │        │
              ▼        ▼        ▼
           BRONZE   BRONZE    BRONZE
              │        │        │
              ▼        ▼        ▼
           SILVER   SILVER    SILVER
              │        │        │
              └────────┼────────┘
                       │
                     JOIN
                       │
                       ▼
                 Transformation
                       │
                       ▼
                    GOLD
                       │
             ┌─────────┴─────────┐
             │                   │
      order_details       customer_revenue
```

# Key Learnings

## Join

```python
df1.join(
    df2,
    join_condition,
    "inner"
)
```

An inner join returns only records that have matching keys in both datasets.

## Business Transformation

```text
OrderAmount = Price × Quantity
```

## Aggregation

Customer revenue is calculated using:

```python
groupBy()
agg()
sum()
```

## Gold Layer

Gold tables are shaped around business and analytical requirements.

Examples:

```text
gold.order_details
gold.customer_revenue
```

# Medallion Architecture Progress

```text
Day 1
Databricks Fundamentals
        ↓
Day 2
File → Bronze
        ↓
Day 3
Bronze → Silver
        ↓
Day 4
Silver → Join → Gold
```

Current architecture:

```text
             RETAILMART
                  │
                  ▼
              RAW FILES
                  │
                  ▼
              BRONZE
                  │
                  ▼
              SILVER
             /     |      \
        Customers Products Orders
             \     |      /
              \    |     /
                  JOIN
                   │
                   ▼
                GOLD
                   │
           ┌───────┴────────┐
           │                │
      Order Details    Customer Revenue
```

# Outcome

By completing Day 4, the project has a multi-table Silver-to-Gold ETL pipeline.

The pipeline now:

1. Stores customer, product and order datasets in Silver.
2. Reads multiple Silver Delta tables using PySpark.
3. Joins Customers, Products and Orders.
4. Selects business-required columns.
5. Calculates order amount.
6. Aggregates customer revenue.
7. Writes business-ready Gold Delta tables.

---

**Next:** [Day 05](../Day05/README.md) - Delta Lake Fundamentals and ACID Transactions.
