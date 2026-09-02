# Day 20 - Gold Layer & Business Analytics

## Project

RetailMart Data Engineering Platform

## Objective

Move from a clean Silver layer to a business-oriented Gold layer.

Today you will learn:

- Gold layer responsibilities
- Business transformations
- Aggregations
- KPIs
- Analytical tables
- Reporting-ready datasets
- Table grain
- PySpark and Spark SQL analytics

The roadmap defines Day 20 as **Gold Layer & Business Analytics**, covering Gold, business transformations, aggregations, KPIs, analytical tables, and reporting-ready datasets. fileciteturn2file0L31-L48

---

## Scenario - RetailMart Business Analytics

RetailMart has already completed the Bronze and Silver layers.

The Silver layer contains clean, validated business data:

```text
silver.day20_customers
silver.day20_products
silver.day20_orders
```

The business now wants analytical datasets such as:

- Order details
- Customer revenue
- City sales
- Category sales
- Overall KPIs
- Monthly sales
- Product performance
- Top customers
- City-level KPIs

The goal is to transform detailed Silver data into datasets that answer business questions.

Architecture:

```text
Bronze
   ↓
Silver
   ↓
Business Transformations
   ↓
Aggregations
   ↓
Gold
   ↓
Business Analytics
```

---

# 1. Gold Layer

Silver contains clean and detailed data.

Gold contains business-oriented data.

```text
BRONZE
Raw / ingested data
        ↓
SILVER
Clean / validated detailed data
        ↓
GOLD
Business-oriented analytical data
```

### Example

Silver:

```text
OrderId
CustomerId
ProductId
Quantity
OrderDate
```

Gold:

```text
CustomerId
TotalOrders
TotalRevenue
AverageOrderValue
```

The Gold table is shaped around a business question:

> How much revenue has each customer generated?

---

# 2. Create the Gold Schema

Run:

```sql
CREATE SCHEMA IF NOT EXISTS gold;
```

Verify:

```sql
SHOW SCHEMAS;
```

---

# 3. Create Silver Customer Data

For this exercise, create a small Silver customer table.

```python
customers = [
    (1, "Arun", "Chennai"),
    (2, "Kumar", "Bangalore"),
    (3, "Priya", "Chennai"),
    (4, "Ravi", "Coimbatore"),
    (5, "Meena", "Madurai")
]

customer_columns = [
    "CustomerId",
    "CustomerName",
    "City"
]

customers_df = spark.createDataFrame(
    customers,
    customer_columns
)

display(customers_df)
```

Write to Silver:

```python
customers_df.write.mode("overwrite").saveAsTable("silver.day20_customers")
```

Verify:

```sql
SELECT *
FROM silver.day20_customers
ORDER BY CustomerId;
```

---

# 4. Create Silver Product Data

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

Write to Silver:

```python
products_df.write.mode("overwrite").saveAsTable("silver.day20_products")
```

Verify:

```sql
SELECT *
FROM silver.day20_products
ORDER BY ProductId;
```

---

# 5. Create Silver Order Data

```python
orders = [
    (1001, 1, 101, 1, "2026-09-01"),
    (1002, 2, 102, 2, "2026-09-01"),
    (1003, 3, 103, 3, "2026-09-02"),
    (1004, 1, 104, 2, "2026-09-02"),
    (1005, 4, 105, 1, "2026-09-03"),
    (1006, 5, 101, 1, "2026-09-03"),
    (1007, 2, 103, 2, "2026-09-04"),
    (1008, 3, 102, 1, "2026-09-04"),
    (1009, 1, 105, 2, "2026-09-05"),
    (1010, 4, 103, 4, "2026-09-05")
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

Write to Silver:

```python
orders_df.write.mode("overwrite").saveAsTable("silver.day20_orders")
```

Verify:

```sql
SELECT *
FROM silver.day20_orders
ORDER BY OrderId;
```

---

# 6. Understand the Data Model

### Customers

```text
CustomerId
CustomerName
City
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
Customers
   │
   │ CustomerId
   ▼
Orders
   │
   │ ProductId
   ▼
Products
```

The order table contains the transaction.

The customer table gives customer information.

The product table gives product information.

---

# 7. Calculate Order Amount

The business wants revenue for every order.

Business rule:

```text
OrderAmount = Quantity × Price
```

Read the Silver tables:

```python
silver_orders = spark.table("silver.day20_orders")
silver_products = spark.table("silver.day20_products")
```

Join Orders with Products:

```python
order_details = (
    silver_orders
    .join(
        silver_products,
        on="ProductId",
        how="inner"
    )
    .withColumn(
        "OrderAmount",
        F.col("Quantity") * F.col("Price")
    )
)
```

Display:

```python
display(order_details)
```

---

# 8. Add Customer Information

Read Customers:

```python
silver_customers = spark.table("silver.day20_customers")
```

Join the customer data:

```python
gold_order_details = (
    order_details
    .join(
        silver_customers,
        on="CustomerId",
        how="inner"
    )
    .select(
        "OrderId",
        "CustomerId",
        "CustomerName",
        "City",
        "ProductId",
        "ProductName",
        "Category",
        "Quantity",
        "Price",
        "OrderAmount",
        "OrderDate"
    )
)
```

Display:

```python
display(gold_order_details)
```

---

# 9. Understand Table Grain

**Grain** means:

> What does one row represent?

For `gold.day20_order_details`:

```text
1 row = 1 order
```

For `gold.day20_customer_revenue`:

```text
1 row = 1 customer
```

For `gold.day20_city_sales`:

```text
1 row = 1 city
```

For `gold.day20_category_sales`:

```text
1 row = 1 category
```

For `gold.day20_kpi_summary`:

```text
1 row = entire business
```

Always understand the grain before creating a Gold table.

---

# 10. Write Gold Order Details

Create the Gold table:

```python
gold_order_details.write.mode("overwrite").saveAsTable("gold.day20_order_details")
```

Verify:

```sql
SELECT *
FROM gold.day20_order_details
ORDER BY OrderId;
```

This table is useful for:

- Detailed reporting
- Order analysis
- Customer analysis
- Product analysis
- Downstream aggregations

---

# 11. Create Customer Revenue Analytics

Business question:

> How much revenue has each customer generated?

Start with:

```python
gold_order_details = spark.table("gold.day20_order_details")
```

Aggregate:

```python
customer_revenue = (
    gold_order_details
    .groupBy(
        "CustomerId",
        "CustomerName"
    )
    .agg(
        F.countDistinct("OrderId").alias("TotalOrders"),
        F.sum("OrderAmount").alias("TotalRevenue"),
        F.avg("OrderAmount").alias("AverageOrderValue")
    )
)
```

Display:

```python
display(customer_revenue)
```

Write to Gold:

```python
customer_revenue.write.mode("overwrite").saveAsTable("gold.day20_customer_revenue")
```

Verify:

```sql
SELECT *
FROM gold.day20_customer_revenue
ORDER BY TotalRevenue DESC;
```

---

# 12. Understand `groupBy()` and `agg()`

This:

```python
.groupBy("CustomerId", "CustomerName")
```

means:

```text
Create one group for each customer
```

Then:

```python
.agg(
    F.countDistinct("OrderId"),
    F.sum("OrderAmount"),
    F.avg("OrderAmount")
)
```

calculates metrics for each group.

Mental model:

```text
Detailed Orders
      ↓
groupBy(Customer)
      ↓
Aggregation
      ↓
Customer Analytics
```

---

# 13. Create City Sales Analytics

Business question:

> Which cities generate the most revenue?

```python
city_sales = (
    gold_order_details
    .groupBy("City")
    .agg(
        F.countDistinct("OrderId").alias("TotalOrders"),
        F.sum("OrderAmount").alias("TotalRevenue"),
        F.sum("Quantity").alias("TotalUnitsSold")
    )
    .orderBy(F.col("TotalRevenue").desc())
)
```

Display:

```python
display(city_sales)
```

Write to Gold:

```python
city_sales.write.mode("overwrite").saveAsTable("gold.day20_city_sales")
```

Verify:

```sql
SELECT *
FROM gold.day20_city_sales
ORDER BY TotalRevenue DESC;
```

---

# 14. Create Category Sales Analytics

Business question:

> Which product categories generate the most revenue?

```python
category_sales = (
    gold_order_details
    .groupBy("Category")
    .agg(
        F.countDistinct("OrderId").alias("TotalOrders"),
        F.sum("Quantity").alias("UnitsSold"),
        F.sum("OrderAmount").alias("TotalRevenue")
    )
    .orderBy(F.col("TotalRevenue").desc())
)
```

Write to Gold:

```python
category_sales.write.mode("overwrite").saveAsTable("gold.day20_category_sales")
```

Verify:

```sql
SELECT *
FROM gold.day20_category_sales
ORDER BY TotalRevenue DESC;
```

---

# 15. KPI - Key Performance Indicator

KPI means:

> **Key Performance Indicator**

A KPI is an important business metric used to measure performance.

Examples:

```text
Total Revenue
Total Orders
Total Units Sold
Average Order Value
Customer Count
```

A metric can be any measurable number.

A KPI is a metric that is particularly important for judging business performance.

---

# 16. Create Overall KPI Summary

Business question:

> How is the overall business performing?

```python
overall_kpis = (
    gold_order_details
    .agg(
        F.countDistinct("OrderId").alias("TotalOrders"),
        F.sum("OrderAmount").alias("TotalRevenue"),
        F.sum("Quantity").alias("TotalUnitsSold"),
        F.avg("OrderAmount").alias("AverageOrderValue")
    )
)
```

Display:

```python
display(overall_kpis)
```

Write to Gold:

```python
overall_kpis.write.mode("overwrite").saveAsTable("gold.day20_kpi_summary")
```

Verify:

```sql
SELECT *
FROM gold.day20_kpi_summary;
```

Expected structure:

```text
TotalOrders | TotalRevenue | TotalUnitsSold | AverageOrderValue
----------------------------------------------------------------
10          | ...          | ...            | ...
```

---

# 17. KPI Mental Model

```text
Detailed Gold Data
        ↓
     Aggregate
        ↓
      KPIs
```

Example:

```text
10 Orders
      ↓
Total Revenue
      ↓
Average Order Value
```

The KPI table is intentionally small because it summarizes the entire business.

---

# 18. Create Monthly Sales Analytics

Business question:

> How much revenue was generated each month?

Add a month column:

```python
monthly_sales = (
    gold_order_details
    .withColumn(
        "Month",
        F.date_format("OrderDate", "yyyy-MM")
    )
    .groupBy("Month")
    .agg(
        F.countDistinct("OrderId").alias("TotalOrders"),
        F.sum("OrderAmount").alias("TotalRevenue")
    )
    .orderBy("Month")
)
```

Display:

```python
display(monthly_sales)
```

Write to Gold:

```python
monthly_sales.write.mode("overwrite").saveAsTable("gold.day20_monthly_sales")
```

Verify:

```sql
SELECT *
FROM gold.day20_monthly_sales
ORDER BY Month;
```

---

# 19. Create Product Performance Analytics

Business question:

> Which products sell the most units and generate the most revenue?

```python
product_performance = (
    gold_order_details
    .groupBy(
        "ProductId",
        "ProductName",
        "Category"
    )
    .agg(
        F.sum("Quantity").alias("UnitsSold"),
        F.sum("OrderAmount").alias("TotalRevenue")
    )
    .orderBy(F.col("TotalRevenue").desc())
)
```

Write to Gold:

```python
product_performance.write.mode("overwrite").saveAsTable("gold.day20_product_performance")
```

Verify:

```sql
SELECT *
FROM gold.day20_product_performance
ORDER BY TotalRevenue DESC;
```

---

# 20. Create Top 3 Customers

Business question:

> Who are our top three customers by revenue?

```python
top_customers = (
    customer_revenue
    .orderBy(F.col("TotalRevenue").desc())
    .limit(3)
)
```

Display:

```python
display(top_customers)
```

Write to Gold:

```python
top_customers.write.mode("overwrite").saveAsTable("gold.day20_top_customers")
```

Verify:

```sql
SELECT *
FROM gold.day20_top_customers
ORDER BY TotalRevenue DESC;
```

---

# 21. Create City-Level KPIs

Business question:

> How is each city performing?

Required metrics:

```text
City
TotalCustomers
TotalOrders
TotalRevenue
AverageOrderValue
```

Create:

```python
city_kpi = (
    gold_order_details
    .groupBy("City")
    .agg(
        F.countDistinct("CustomerId").alias("TotalCustomers"),
        F.countDistinct("OrderId").alias("TotalOrders"),
        F.sum("OrderAmount").alias("TotalRevenue"),
        F.avg("OrderAmount").alias("AverageOrderValue")
    )
    .orderBy(F.col("TotalRevenue").desc())
)
```

Write to Gold:

```python
city_kpi.write.mode("overwrite").saveAsTable("gold.day20_city_kpi")
```

Verify:

```sql
SELECT *
FROM gold.day20_city_kpi
ORDER BY TotalRevenue DESC;
```

---

# 22. Gold Tables Created

By this point you should have:

```text
gold
│
├── day20_order_details
│
├── day20_customer_revenue
│
├── day20_city_sales
│
├── day20_category_sales
│
├── day20_kpi_summary
│
├── day20_monthly_sales
│
├── day20_product_performance
│
├── day20_top_customers
│
└── day20_city_kpi
```

Each table answers a different business question.

---

# 23. Gold Table Grain

| Gold Table | Grain |
|---|---|
| `day20_order_details` | 1 row = 1 order |
| `day20_customer_revenue` | 1 row = 1 customer |
| `day20_city_sales` | 1 row = 1 city |
| `day20_category_sales` | 1 row = 1 category |
| `day20_kpi_summary` | 1 row = entire business |
| `day20_monthly_sales` | 1 row = 1 month |
| `day20_product_performance` | 1 row = 1 product |
| `day20_top_customers` | 1 row = 1 selected top customer |
| `day20_city_kpi` | 1 row = 1 city |

---

# 24. Spark SQL - Customer Revenue

The same business logic can be written using Spark SQL.

```sql
SELECT
    CustomerId,
    CustomerName,
    COUNT(DISTINCT OrderId) AS TotalOrders,
    SUM(OrderAmount) AS TotalRevenue,
    AVG(OrderAmount) AS AverageOrderValue
FROM gold.day20_order_details
GROUP BY
    CustomerId,
    CustomerName
ORDER BY TotalRevenue DESC;
```

---

# 25. Spark SQL - Category Sales

```sql
SELECT
    Category,
    COUNT(DISTINCT OrderId) AS TotalOrders,
    SUM(Quantity) AS UnitsSold,
    SUM(OrderAmount) AS TotalRevenue
FROM gold.day20_order_details
GROUP BY Category
ORDER BY TotalRevenue DESC;
```

---

# 26. Spark SQL - Monthly Sales

```sql
SELECT
    date_format(OrderDate, 'yyyy-MM') AS Month,
    COUNT(DISTINCT OrderId) AS TotalOrders,
    SUM(OrderAmount) AS TotalRevenue
FROM gold.day20_order_details
GROUP BY date_format(OrderDate, 'yyyy-MM')
ORDER BY Month;
```

---

# 27. Spark SQL - Top Customers

```sql
SELECT
    CustomerId,
    CustomerName,
    TotalOrders,
    TotalRevenue,
    AverageOrderValue
FROM gold.day20_customer_revenue
ORDER BY TotalRevenue DESC
LIMIT 3;
```

---

# 28. Verify Gold Data Quality

Gold should be validated before it is used for reporting.

### Check for NULL revenue

```sql
SELECT *
FROM gold.day20_order_details
WHERE OrderAmount IS NULL;
```

Expected:

```text
0 rows
```

### Check negative revenue

```sql
SELECT *
FROM gold.day20_order_details
WHERE OrderAmount < 0;
```

Expected:

```text
0 rows
```

### Check duplicate OrderId

```sql
SELECT
    OrderId,
    COUNT(*) AS RecordCount
FROM gold.day20_order_details
GROUP BY OrderId
HAVING COUNT(*) > 1;
```

Expected:

```text
0 rows
```

### Check KPI consistency

```sql
SELECT
    COUNT(DISTINCT OrderId) AS TotalOrders,
    SUM(OrderAmount) AS TotalRevenue,
    SUM(Quantity) AS TotalUnitsSold
FROM gold.day20_order_details;
```

Compare the result with:

```sql
SELECT *
FROM gold.day20_kpi_summary;
```

---

# 29. Bronze → Silver → Gold

You have now completed another major part of the Medallion Architecture.

```text
BRONZE
Raw incoming data
        ↓
SILVER
Cleaned
Validated
Deduplicated
Standardized
        ↓
GOLD
Business transformations
Aggregations
KPIs
Analytical tables
        ↓
BUSINESS ANALYTICS
Reports
Dashboards
Business decisions
```

The roadmap explicitly defines this progression as:

```text
Bronze
   ↓
Silver
   ↓
Gold
   ↓
Business Analytics
```

with the goal of transforming raw data into business-oriented analytical datasets. fileciteturn2file2L1-L25

---

# 30. Complete Day 20 Architecture

```text
                         RETAILMART
                              │
                              ▼
                           BRONZE
                              │
                              ▼
                           SILVER
                 ┌────────────┼────────────┐
                 │            │            │
                 ▼            ▼            ▼
             Customers     Products      Orders
                 │            │            │
                 └────────────┼────────────┘
                              │
                              ▼
                        JOIN + TRANSFORM
                              │
                              ▼
                         ORDER DETAILS
                              │
               ┌──────────────┼──────────────┐
               │              │              │
               ▼              ▼              ▼
          Customer        City Sales     Category Sales
          Revenue
               │
               └──────────────┬──────────────┐
                              │              │
                              ▼              ▼
                         KPI Summary    Product Performance
                              │
                              ▼
                       Business Analytics
```

---

# 31. PySpark vs Spark SQL

Both can create the same analytical result.

### PySpark

Useful when:

```text
Complex transformation
Reusable Python logic
DataFrame-based pipelines
Functions and application code
```

Example:

```python
df.groupBy("City").agg(
    F.sum("OrderAmount").alias("TotalRevenue")
)
```

### Spark SQL

Useful when:

```text
SQL-based analytics
Ad-hoc analysis
Analyst-friendly queries
Reporting logic
```

Example:

```sql
SELECT
    City,
    SUM(OrderAmount) AS TotalRevenue
FROM gold.day20_order_details
GROUP BY City;
```

Both execute through Spark.

---

# 32. Common Mistake - Wrong Grain

Suppose you want:

```text
1 row = 1 customer
```

but you select:

```text
CustomerId
OrderId
TotalRevenue
```

Now a customer can have multiple rows.

Therefore, always ask:

> What should one row represent?

Then design the `groupBy()` accordingly.

---

# 33. Common Mistake - Aggregating Before the Correct Join

Suppose:

```text
Orders
Products
```

You need:

```text
Quantity × Price
```

The price exists in Products.

Therefore:

```text
Orders
   ↓
Join Products
   ↓
Calculate OrderAmount
   ↓
Aggregate
```

Do not aggregate before obtaining the required business attributes.

---

# 34. Common Mistake - Creating One Huge Gold Table

Avoid putting every business metric into one giant table.

Instead:

```text
Order Details
Customer Revenue
City Sales
Category Sales
KPI Summary
Product Performance
```

Each Gold table should have a clear purpose and grain.

---

# 35. Day 20 Hands-on Challenge

Build the following Gold tables independently without copying the solution above.

## Challenge 1 - Monthly Sales

Create:

```text
gold.day20_monthly_sales
```

Required columns:

```text
Month
TotalOrders
TotalRevenue
```

Sort by:

```text
Month
```

---

## Challenge 2 - Product Performance

Create:

```text
gold.day20_product_performance
```

Required columns:

```text
ProductId
ProductName
Category
UnitsSold
TotalRevenue
```

Sort by:

```text
TotalRevenue DESC
```

---

## Challenge 3 - Top Customers

Create:

```text
gold.day20_top_customers
```

Requirements:

```text
Top 3 customers
Sorted by TotalRevenue DESC
```

---

## Challenge 4 - City KPI

Create:

```text
gold.day20_city_kpi
```

Required columns:

```text
City
TotalCustomers
TotalOrders
TotalRevenue
AverageOrderValue
```

---

# 36. Day 20 Checklist

- [ ] Understand Gold layer
- [ ] Understand business transformations
- [ ] Create `gold` schema
- [ ] Create Silver Customers
- [ ] Create Silver Products
- [ ] Create Silver Orders
- [ ] Join Orders with Products
- [ ] Calculate `OrderAmount`
- [ ] Join Customer information
- [ ] Understand table grain
- [ ] Create `gold.day20_order_details`
- [ ] Create `gold.day20_customer_revenue`
- [ ] Understand `groupBy()`
- [ ] Understand `agg()`
- [ ] Create `gold.day20_city_sales`
- [ ] Create `gold.day20_category_sales`
- [ ] Understand KPI
- [ ] Create `gold.day20_kpi_summary`
- [ ] Create `gold.day20_monthly_sales`
- [ ] Create `gold.day20_product_performance`
- [ ] Create `gold.day20_top_customers`
- [ ] Create `gold.day20_city_kpi`
- [ ] Practice Spark SQL equivalents
- [ ] Validate Gold data
- [ ] Check for NULL revenue
- [ ] Check for negative revenue
- [ ] Check duplicate OrderIds
- [ ] Compare KPI totals
- [ ] Complete all Day 20 challenges

# Key Learnings

## Gold Layer

```text
Clean Silver data
        ↓
Business transformations
        ↓
Aggregations
        ↓
Gold
```

## Business Transformation

```text
Quantity × Price
       ↓
OrderAmount
```

## Aggregation

```text
Detailed records
       ↓
groupBy()
       ↓
agg()
       ↓
Business summary
```

## KPI

```text
Important business metric
        ↓
Performance measurement
```

## Table Grain

```text
What does one row represent?
```

This question should be answered before designing every Gold table.

## Reporting-Ready Data

```text
Gold
  ↓
Clear business meaning
  ↓
Consistent grain
  ↓
Aggregated metrics
  ↓
Reporting / Analytics
```

---

# Day 20 Final Mental Model

```text
                    SOURCE
                       │
                       ▼
                    BRONZE
              Raw incoming data
                       │
                       ▼
                    SILVER
       Clean + Validated + Deduplicated
                       │
                       ▼
             BUSINESS TRANSFORMATIONS
                       │
          ┌────────────┼────────────┐
          │            │            │
          ▼            ▼            ▼
       Joins       Calculations  Aggregations
          │            │            │
          └────────────┼────────────┘
                       │
                       ▼
                     GOLD
                       │
        ┌──────────────┼───────────────┐
        │              │               │
        ▼              ▼               ▼
 Customer Revenue   City Sales    Category Sales
        │              │               │
        └──────────────┼───────────────┘
                       │
                       ▼
                 KPI / Analytics
                       │
                       ▼
                BUSINESS DECISIONS
```

## Day 20 Outcome

By completing Day 20, you should be able to explain:

> **Bronze preserves incoming data, Silver provides clean and trusted detailed data, and Gold transforms that data into business-oriented analytical datasets. Gold tables are designed around business questions, have a clearly defined grain, contain calculated and aggregated metrics, and can provide KPIs and reporting-ready datasets for business analytics.**

---

**Next:** Day 21 - Advanced Delta Lake
