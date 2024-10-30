# BigQuery Workshop Guide
## Loading and Analyzing E-commerce Data

### Prerequisites
Before starting, replace pect in all commands and paths with your own username:
- **Project ID: Change cas-daeng-2024-pect to cas-daeng-2024-yourusername**
- **Bucket name: Change retail-data-pect to retail-data-yourusername**
- CSV files: customers.csv, products.csv
- Transactions table already created via Dataflow

### 1. Creating a Dataset

1. Open BigQuery Console
2. Click on your project `cas-daeng-2024-pect`
3. Click "Create Dataset"
4. Fill in the details:
   - Dataset ID: `ecommerce`
   - Data location: `europe-west6 (Zurich)`
   - Leave other settings as default
5. Click "Create dataset"

### 2. Loading Customers Table (UI Method)

1. Click on the `ecommerce` dataset
2. Click "Create Table" button
3. Configure source:
   - Create table from: `Google Cloud Storage`
   - Source path: `gs://retail-data-pect/customers.csv`
4. Configure destination:
   - Project: `cas-daeng-2024-pect`
   - Dataset: `ecommerce`
   - Table name: `customers`
5. Configure schema:
   - Click "Edit as text"
   - Paste this schema:
   ```
   customer_id:INTEGER,name:STRING,email:STRING,location:STRING,age:INTEGER,registration_date:DATE,segment:STRING
   ```
6. Under Advanced options:
   - Header rows to skip: 1
7. Click "Create table"

Verify customers data:
```sql
-- Check the data
SELECT * FROM `cas-daeng-2024-pect.ecommerce.customers` LIMIT 5;

-- Count customers by segment
SELECT 
  segment, 
  COUNT(*) as count 
FROM `cas-daeng-2024-pect.ecommerce.customers`
GROUP BY segment
ORDER BY count DESC;
```

### 3. Loading Products Table (SQL Method)

Copy and run these SQL commands:

```sql
-- Create products table
CREATE OR REPLACE TABLE `cas-daeng-2024-pect.ecommerce.products` (
  product_id INT64,
  name STRING,
  category STRING,
  price FLOAT64,
  stock_quantity INT64,
  affinity_group FLOAT64
);

-- Load data
LOAD DATA INTO `cas-daeng-2024-pect.ecommerce.products`
FROM FILES (
  format = 'CSV',
  uris = ['gs://retail-data-pect/products.csv'],
  skip_leading_rows = 1
);

-- Verify the data
SELECT * FROM `cas-daeng-2024-pect.ecommerce.products` LIMIT 5;
```

### 4. Basic Analysis Queries

Try these queries to analyze your data:

```sql
-- Products by category
SELECT 
  category,
  COUNT(*) as product_count,
  ROUND(AVG(price), 2) as avg_price
FROM `cas-daeng-2024-pect.ecommerce.products`
GROUP BY category
ORDER BY product_count DESC;

-- Top 5 selling products
SELECT 
  p.name as product_name,
  p.category,
  COUNT(*) as times_sold,
  ROUND(SUM(t.total_amount), 2) as total_revenue
FROM `cas-daeng-2024-pect.ecommerce.transactions` t
JOIN `cas-daeng-2024-pect.ecommerce.products` p 
  ON t.product_id = p.product_id
GROUP BY p.name, p.category
ORDER BY times_sold DESC
LIMIT 5;

-- Customer segments analysis
SELECT 
  c.segment,
  COUNT(DISTINCT c.customer_id) as number_of_customers,
  ROUND(AVG(t.total_amount), 2) as average_purchase_amount
FROM `cas-daeng-2024-pect.ecommerce.customers` c
JOIN `cas-daeng-2024-pect.ecommerce.transactions` t 
  ON c.customer_id = t.customer_id
GROUP BY c.segment
ORDER BY average_purchase_amount DESC;
```

### 5. Practice Exercise

Write a query to analyze products in a specific category:

```sql
-- Product category analysis
SELECT 
  p.name,
  p.price,
  COUNT(t.transaction_id) as number_of_sales,
  ROUND(SUM(t.total_amount), 2) as total_revenue
FROM `cas-daeng-2024-pect.ecommerce.products` p
LEFT JOIN `cas-daeng-2024-pect.ecommerce.transactions` t 
  ON p.product_id = t.product_id
WHERE p.category = 'Electronics'  -- Try different categories
GROUP BY p.name, p.price
ORDER BY number_of_sales DESC;
```

### Best Practices

1. Always preview queries before running them
2. Use LIMIT when exploring new data
3. Specify column names instead of using SELECT *
4. Write clear comments in your SQL
5. Check query costs before running large queries

### Common Issues and Solutions

1. If table creation fails:
   - Check if the dataset exists
   - Verify the schema matches your CSV
   - Ensure the bucket path is correct

2. If queries return no results:
   - Check table names and project ID
   - Verify JOIN conditions
   - Check WHERE clause conditions

3. For better query performance:
   - Filter data early in the query
   - Avoid SELECT *
   - Use appropriate WHERE clauses

### Additional Resources

- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [SQL Reference](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax)
- [Best Practices](https://cloud.google.com/bigquery/docs/best-practices-performance-overview)