# BigQuery ML Tutorial: Predicting High-Value Customers
This tutorial walks you through creating your first machine learning model using BigQuery ML to predict high-value customers.

## Prerequisites
- Access to Google Cloud Platform
- A project with BigQuery enabled
- Basic SQL knowledge
- Sample tables in your BigQuery dataset

> **Important**: Throughout this tutorial, you'll see example queries using the dataset `cas-daeng-2024-pect.ecommerce`. Replace this with your own dataset name following the pattern `cas-daeng-2024-[your-name].ecommerce`.

## 1. Understanding the Data

First, let's examine our source tables:

```sql
-- View customer data structure
SELECT *
FROM `cas-daeng-2024-pect.ecommerce.customers`
LIMIT 5;

-- Replace with your dataset name like:
-- SELECT *
-- FROM `cas-daeng-2024-yourname.ecommerce.customers`
-- LIMIT 5;

-- View transaction data structure
SELECT *
FROM `cas-daeng-2024-pect.ecommerce.transactions`
LIMIT 5;
```

## 2. Data Preparation

### 2.1 Create Customer Spending Features

First, we'll aggregate customer spending patterns:

```sql
-- Create a view with customer spending data
CREATE OR REPLACE VIEW `cas-daeng-2024-pect.ecommerce.customer_spending` AS
SELECT 
  c.customer_id,
  c.age,
  SUM(t.total_amount) as total_spent,
  COUNT(*) as purchase_count,
  AVG(t.total_amount) as avg_purchase
FROM `cas-daeng-2024-pect.ecommerce.customers` c
JOIN `cas-daeng-2024-pect.ecommerce.transactions` t
  ON c.customer_id = t.customer_id
GROUP BY c.customer_id, c.age;
```

This query:
- Joins customers with their transactions
- Calculates total spending per customer
- Counts number of purchases
- Computes average purchase amount

### 2.2 Create High-Value Labels

Now we'll label customers as high-value based on above-average spending:

```sql
CREATE OR REPLACE TABLE `cas-daeng-2024-pect.ecommerce.labeled_customers` AS
WITH avg_spending AS (
  SELECT AVG(total_spent) as avg_total_spent
  FROM `cas-daeng-2024-pect.ecommerce.customer_spending`
)
SELECT 
  cs.*,
  IF(cs.total_spent > avg.avg_total_spent, 1, 0) as high_value
FROM `cas-daeng-2024-pect.ecommerce.customer_spending` cs
CROSS JOIN avg_spending avg;

-- Check our label distribution
SELECT 
  high_value,
  COUNT(*) as count,
  ROUND(AVG(total_spent), 2) as avg_spending
FROM `cas-daeng-2024-pect.ecommerce.labeled_customers`
GROUP BY high_value;
```

### 2.3 Split Into Training and Test Sets

We'll create separate datasets for training and testing:

```sql
-- Create training dataset (80% of data)
CREATE OR REPLACE TABLE `cas-daeng-2024-pect.ecommerce.train_data` AS
SELECT *
FROM `cas-daeng-2024-pect.ecommerce.labeled_customers`
WHERE MOD(ABS(FARM_FINGERPRINT(CAST(customer_id AS STRING))), 10) < 8;

-- Create test dataset (20% of data)
CREATE OR REPLACE TABLE `cas-daeng-2024-pect.ecommerce.test_data` AS
SELECT *
FROM `cas-daeng-2024-pect.ecommerce.labeled_customers`
WHERE MOD(ABS(FARM_FINGERPRINT(CAST(customer_id AS STRING))), 10) >= 8;

-- Verify the split
SELECT
  'Training' as dataset,
  COUNT(*) as count
FROM `cas-daeng-2024-pect.ecommerce.train_data`
UNION ALL
SELECT
  'Test' as dataset,
  COUNT(*) as count
FROM `cas-daeng-2024-pect.ecommerce.test_data`;
```

The FARM_FINGERPRINT function ensures:
- Consistent split (same customers always go to same set)
- Random but deterministic distribution
- Approximately 80/20 split

## 3. Creating the Model

Now we'll create a logistic regression model:

```sql
CREATE OR REPLACE MODEL `cas-daeng-2024-pect.ecommerce.customer_value_model`
OPTIONS(
  model_type='logistic_reg',
  input_label_cols=['high_value']
) AS
SELECT
  age,
  purchase_count,
  avg_purchase,
  high_value
FROM `cas-daeng-2024-pect.ecommerce.train_data`;
```

## 4. Evaluating the Model

### 4.1 Check Model Performance

```sql
-- Evaluate on test data
SELECT 
  *
FROM ML.EVALUATE(MODEL `cas-daeng-2024-pect.ecommerce.customer_value_model`,
  (SELECT
    age,
    purchase_count,
    avg_purchase,
    high_value
   FROM `cas-daeng-2024-pect.ecommerce.test_data`));
```

Key metrics to look for:
- Accuracy: Overall prediction accuracy
- Precision: Accuracy of positive predictions
- Recall: Percentage of actual positives captured
- ROC AUC: Overall model quality (closer to 1 is better)

### 4.2 Examine Feature Weights

```sql
-- Check feature importance
SELECT 
  processed_input as feature,
  ROUND(weight, 4) as weight,
  ROUND(ABS(weight), 4) as weight_magnitude,
  CASE 
    WHEN weight > 0 THEN 'increases probability'
    WHEN weight < 0 THEN 'decreases probability'
    ELSE 'no effect'
  END as effect
FROM ML.WEIGHTS(MODEL `cas-daeng-2024-pect.ecommerce.customer_value_model`)
ORDER BY ABS(weight) DESC;
```

This shows:
- Which features are most important
- How each feature affects predictions
- Relative importance of each feature

## 5. Making Predictions

Create a view for easy access to predictions:

```sql
CREATE OR REPLACE VIEW `cas-daeng-2024-pect.ecommerce.customer_predictions` AS 
SELECT 
  c.customer_id,
  c.name,
  c.email,
  ROUND((SELECT prob 
         FROM UNNEST(p.predicted_high_value_probs) 
         WHERE label = 1) * 100, 2) as probability_high_value
FROM `cas-daeng-2024-pect.ecommerce.customers` c,
ML.PREDICT(MODEL `cas-daeng-2024-pect.ecommerce.customer_value_model`,
  (SELECT 
    customer_id,
    age,
    purchase_count,
    avg_purchase
   FROM `cas-daeng-2024-pect.ecommerce.labeled_customers`)) p
WHERE c.customer_id = p.customer_id;

-- View high-probability customers
SELECT *
FROM `cas-daeng-2024-pect.ecommerce.customer_predictions`
WHERE probability_high_value > 70
ORDER BY probability_high_value DESC
LIMIT 10;
```

## Common Issues and Solutions

1. Dataset name errors:
   ```
   Error: Dataset "cas-daeng-2024-yourname" not found
   ```
   - Replace all instances of `cas-daeng-2024-pect` with your dataset name

2. Prediction column errors:
   ```
   Error: Name predicted_high_value_proba not found
   ```
   - Make sure to use the correct column name from ML.PREDICT output
   - Use the UNNEST function to access array values

3. Type mismatch in predictions:
   ```
   Error: No matching signature for operator = for argument types: INT64, STRING
   ```
   - Use correct types in comparisons (e.g., `label = 1` not `label = '1'`)

## Next Steps

1. Try modifying the model:
   - Add more features
   - Adjust the high-value threshold
   - Try different model types

2. Analyze your results:
   - Look for patterns in predictions
   - Compare feature importances
   - Test different probability thresholds

3. Create visualizations:
   - Probability distributions
   - Feature importance charts
   - Prediction accuracy analysis

## Resources
- [BigQuery ML Documentation](https://cloud.google.com/bigquery-ml/docs)
- [SQL Reference](https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators)
- [Model Evaluation Metrics](https://cloud.google.com/bigquery-ml/docs/reference/standard-sql/bigqueryml-syntax-evaluate)