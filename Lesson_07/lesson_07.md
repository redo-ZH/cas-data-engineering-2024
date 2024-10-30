# BigQuery ML Tutorial: Your First ML Model
This tutorial walks you through creating your first machine learning model using BigQuery ML. You'll learn how to predict high-value customers using basic customer and transaction data.

## Prerequisites
- Access to Google Cloud Platform
- A project with BigQuery enabled
- Basic SQL knowledge
- The sample tables in your BigQuery dataset (created in previous video)

> **Important**: Throughout this tutorial, you'll see example queries using the dataset `cas-daeng-2024-pect.ecommerce`. Replace this with your own dataset name following the pattern `cas-daeng-2024-[your-name].ecommerce`.

## 1. Understanding the Data

First, let's look at our source tables:

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

### 2.1 Create Customer Spending View

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

-- Don't forget to replace the dataset name with yours:
-- CREATE OR REPLACE VIEW `cas-daeng-2024-yourname.ecommerce.customer_spending`
```

### 2.2 Add Labels for High-Value Customers

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
```

### 2.3 Split Data into Training and Test Sets

It's crucial to test our model on data it hasn't seen during training:

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

## 3. Creating the Model

Now we'll create a logistic regression model to predict high-value customers:

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

### 4.1 Check Training Performance

```sql
-- Evaluate on training data
SELECT 
  'Training' as dataset,
  *
FROM ML.EVALUATE(MODEL `cas-daeng-2024-pect.ecommerce.customer_value_model`,
  (SELECT
    age,
    purchase_count,
    avg_purchase,
    high_value
   FROM `cas-daeng-2024-pect.ecommerce.train_data`));
```

### 4.2 Check Test Performance

```sql
-- Evaluate on test data
SELECT 
  'Test' as dataset,
  *
FROM ML.EVALUATE(MODEL `cas-daeng-2024-pect.ecommerce.customer_value_model`,
  (SELECT
    age,
    purchase_count,
    avg_purchase,
    high_value
   FROM `cas-daeng-2024-pect.ecommerce.test_data`));
```

### 4.3 Examine Feature Weights

For logistic regression models, we use ML.WEIGHTS to understand how each feature affects predictions:

```sql
-- Detailed view of feature weights and their effects
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

This query shows:
- feature: The input variable name
- weight: The coefficient assigned by the model
- weight_magnitude: Absolute value of the weight (shows importance regardless of direction)
- effect: Whether the feature increases or decreases the probability of being a high-value customer

## 5. Making Predictions

Create a view for easy access to predictions:

```sql
CREATE OR REPLACE VIEW `cas-daeng-2024-pect.ecommerce.customer_predictions` AS
SELECT 
  c.customer_id,
  c.name,
  c.email,
  ROUND(p.predicted_high_value_proba[OFFSET(1)] * 100, 2) as probability_high_value
FROM `cas-daeng-2024-pect.ecommerce.customers` c,
ML.PREDICT(MODEL `cas-daeng-2024-pect.ecommerce.customer_value_model`,
  (SELECT 
    age,
    purchase_count,
    avg_purchase
   FROM `cas-daeng-2024-pect.ecommerce.labeled_customers`)) p
WHERE c.customer_id = p.customer_id;

-- View top predictions
SELECT *
FROM `cas-daeng-2024-pect.ecommerce.customer_predictions`
WHERE probability_high_value > 70  -- 70% probability threshold
ORDER BY probability_high_value DESC
LIMIT 10;
```

## Understanding the Results

### Key Metrics Explained
- **Accuracy**: Percentage of correct predictions (e.g., if 0.85, model is correct 85% of the time)
- **Precision**: Of those predicted as high-value, how many actually were
- **Recall**: Of actual high-value customers, how many did we find
- **ROC AUC**: Overall model quality (0.5 is random, 1.0 is perfect)

### Understanding Feature Weights
- Larger positive weights mean that higher values of that feature increase the likelihood of being a high-value customer
- Larger negative weights mean that higher values of that feature decrease the likelihood
- The magnitude (absolute value) tells you how important the feature is to the prediction
- Features with weights close to zero have little impact on predictions

## Exercises for Practice

1. Modify the prediction threshold:
```sql
-- Try different probability thresholds
SELECT 
  ROUND(probability_high_value/10) * 10 as probability_bucket,
  COUNT(*) as customer_count
FROM `cas-daeng-2024-pect.ecommerce.customer_predictions`
GROUP BY probability_bucket
ORDER BY probability_bucket DESC;
```

2. Analyze predictions by age group:
```sql
SELECT 
  FLOOR(age/10) * 10 as age_group,
  COUNT(*) as customer_count,
  AVG(probability_high_value) as avg_probability
FROM `cas-daeng-2024-pect.ecommerce.customer_predictions`
GROUP BY age_group
ORDER BY age_group;
```

## Troubleshooting Common Issues

1. Dataset errors:
   ```
   Error: Dataset "cas-daeng-2024-yourname" not found
   ```
   - Make sure you replaced all instances of `cas-daeng-2024-pect` with your dataset name

2. Permission errors:
   ```
   Error: Access Denied: Dataset cas-daeng-2024-...
   ```
   - Verify you're using your own dataset
   - Check your BigQuery access permissions

3. Model training errors:
   - Verify no NULL values in your features
   - Check data types match expectations
   - Ensure you have enough training data

4. ML.WEIGHTS errors:
   - Verify the model has finished training
   - Check the model name is correct
   - Ensure you're using logistic regression model type

## Next Steps

1. Experiment with different features:
   - Add customer location
   - Include product categories
   - Try different time periods

2. Try different model types:
   - BOOSTED_TREE_CLASSIFIER (supports ML.FEATURE_IMPORTANCE)
   - DNN_CLASSIFIER (for deep learning)
   - XGBoost (for better performance)

3. Visualize your results:
   - Create charts of predictions
   - Plot age group distributions
   - Show feature weight comparisons

## Resources
- [BigQuery ML Documentation](https://cloud.google.com/bigquery-ml/docs)
- [SQL Reference](https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators)
- [Model Evaluation Metrics](https://cloud.google.com/bigquery-ml/docs/reference/standard-sql/bigqueryml-syntax-evaluate)