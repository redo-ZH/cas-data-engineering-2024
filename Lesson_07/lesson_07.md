# Building a Customer Risk Analysis Pipeline
Data Engineering Course - ZHAW School of Management and Law

## Overview
In this lesson, you'll build a data pipeline that analyzes customer churn risk using Cloud Composer (managed Apache Airflow). The pipeline processes clickstream data and support tickets to identify customers who might need attention.

## Prerequisites
- Access to Google Cloud Console
- Project: cas-daeng-2024-pect
- Storage bucket: retail-data-pect
- Dataset: ecommerce

## Step 1: Create Cloud Composer Environment

```bash
# Set your project
gcloud config set project cas-daeng-2024-pect

# Create Cloud Composer environment
gcloud composer environments create customer-analysis \
    --location europe-west6 \
    --image-version composer-2.0.31 \
    --environment-size small
```

This will take approximately 20-25 minutes to complete.

## Step 2: Verify Data Files

```bash
# Check if your input files exist
gsutil ls gs://retail-data-pect/
```

You should see:
- clickstream.csv
- support_tickets.csv

Sample data format:
```csv
# clickstream.csv
click_id,customer_id,product_id,timestamp,action
1,2851,623,2024-01-26 16:54:34,view
2,2185,697,2024-01-14 20:21:29,add_to_cart

# support_tickets.csv
ticket_id,customer_id,category,status,created_at,resolved_at,churn_risk
1,7210,Shipping,Closed,2024-02-22 19:03:58,2024-04-26 05:51:04,0.5
2,4960,Shipping,In Progress,2024-09-24 11:45:36,,0.25
```

## Step 3: Create and Upload DAG File

1. Create a file named `customer_risk_analysis.py` with the following content:

```python
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Configuration
PROJECT_ID = "cas-daeng-2024-pect"
BUCKET = "retail-data-pect"
DATASET = "ecommerce"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'customer_risk_analysis',
    default_args=default_args,
    description='Analyze customer churn risk using clickstream and support tickets',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['ecommerce', 'risk_analysis'],
)

# Task 1: Load clickstream data to BigQuery
load_clickstream = GCSToBigQueryOperator(
    task_id='load_clickstream',
    bucket=BUCKET,
    source_objects=['clickstream.csv'],
    destination_project_dataset_table=f'{PROJECT_ID}.{DATASET}.clickstream',
    schema_fields=[
        {'name': 'click_id', 'type': 'INTEGER'},
        {'name': 'customer_id', 'type': 'INTEGER'},
        {'name': 'product_id', 'type': 'INTEGER'},
        {'name': 'timestamp', 'type': 'TIMESTAMP'},
        {'name': 'action', 'type': 'STRING'},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

# Task 2: Load support tickets to BigQuery
load_support_tickets = GCSToBigQueryOperator(
    task_id='load_support_tickets',
    bucket=BUCKET,
    source_objects=['support_tickets.csv'],
    destination_project_dataset_table=f'{PROJECT_ID}.{DATASET}.support_tickets',
    schema_fields=[
        {'name': 'ticket_id', 'type': 'INTEGER'},
        {'name': 'customer_id', 'type': 'INTEGER'},
        {'name': 'category', 'type': 'STRING'},
        {'name': 'status', 'type': 'STRING'},
        {'name': 'created_at', 'type': 'TIMESTAMP'},
        {'name': 'resolved_at', 'type': 'TIMESTAMP'},
        {'name': 'churn_risk', 'type': 'FLOAT'},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

# Task 3: Create customer risk analysis
create_risk_analysis = BigQueryInsertJobOperator(
    task_id='create_risk_analysis',
    configuration={
        'query': {
            'query': f"""
            CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.customer_risk_analysis` AS
            WITH customer_activity AS (
                SELECT 
                    customer_id,
                    COUNT(*) as total_clicks,
                    DATE_DIFF(CURRENT_DATE(), DATE(MAX(timestamp)), DAY) as days_since_last_visit
                FROM `{PROJECT_ID}.{DATASET}.clickstream`
                GROUP BY customer_id
            ),
            customer_support AS (
                SELECT
                    customer_id,
                    COUNT(*) as support_tickets,
                    MAX(churn_risk) as churn_risk
                FROM `{PROJECT_ID}.{DATASET}.support_tickets`
                GROUP BY customer_id
            )
            SELECT
                c.customer_id,
                c.name,
                c.email,
                c.segment,
                COALESCE(ca.total_clicks, 0) as total_clicks,
                COALESCE(ca.days_since_last_visit, 999) as days_since_last_visit,
                COALESCE(cs.support_tickets, 0) as support_tickets,
                COALESCE(cs.churn_risk, 0) as churn_risk
            FROM `{PROJECT_ID}.{DATASET}.customers` c
            LEFT JOIN customer_activity ca ON c.customer_id = ca.customer_id
            LEFT JOIN customer_support cs ON c.customer_id = cs.customer_id
            """,
            'useLegacySql': False
        }
    },
    dag=dag,
)

# Task 4: Create daily summary
create_daily_summary = BigQueryInsertJobOperator(
    task_id='create_daily_summary',
    configuration={
        'query': {
            'query': f"""
            CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.risk_summary` AS
            SELECT
                segment,
                COUNT(*) as customer_count,
                AVG(total_clicks) as avg_clicks,
                AVG(support_tickets) as avg_tickets,
                AVG(churn_risk) as avg_churn_risk,
                COUNT(CASE WHEN days_since_last_visit > 30 THEN 1 END) as inactive_customers,
                COUNT(CASE WHEN churn_risk > 0.5 THEN 1 END) as high_risk_customers
            FROM `{PROJECT_ID}.{DATASET}.customer_risk_analysis`
            GROUP BY segment
            ORDER BY avg_churn_risk DESC;
            """,
            'useLegacySql': False
        }
    },
    dag=dag,
)

# Set task dependencies
[load_clickstream, load_support_tickets] >> create_risk_analysis >> create_daily_summary
```

2. Upload the DAG file:
```bash
# Get the DAG folder location
BUCKET=$(gcloud composer environments describe customer-analysis \
    --location europe-west6 \
    --format="get(config.dagGcsPrefix)")

# Upload DAG file
gsutil cp customer_risk_analysis.py ${BUCKET}/dags/
```

## Step 4: Access Airflow UI

1. Get the Airflow web interface URL:
```bash
gcloud composer environments describe customer-analysis \
    --location europe-west6 \
    --format="get(config.airflowUri)"
```

2. Open the URL in your browser
3. Find your DAG named "customer_risk_analysis"
4. Turn on the DAG using the toggle switch
5. Wait for the DAG to run (about 5-10 minutes)

## Step 5: Validate Results

Open BigQuery in the Google Cloud Console and run these queries:

```sql
-- View top 5 customers by churn risk
SELECT 
    name, 
    email, 
    segment, 
    days_since_last_visit, 
    support_tickets, 
    churn_risk 
FROM `cas-daeng-2024-pect.ecommerce.customer_risk_analysis`
ORDER BY churn_risk DESC 
LIMIT 5;

-- View segment summary
SELECT * 
FROM `cas-daeng-2024-pect.ecommerce.risk_summary`
ORDER BY avg_churn_risk DESC;
```

## Step 6: Clean Up

When you're done with the exercise:

```bash
# Delete Cloud Composer environment
gcloud composer environments delete customer-analysis \
    --location europe-west6
```

## Troubleshooting

1. If DAG doesn't appear in Airflow UI:
   - Wait 5 minutes for sync
   - Check for Python syntax errors
   - Verify DAG file extension is .py

2. If tasks fail:
   - Check task logs in Airflow UI
   - Verify file names and paths
   - Confirm BigQuery dataset exists

3. If queries return no results:
   - Verify data was loaded successfully
   - Check table names
   - Confirm schema matches

## Exercise Questions

1. How many high-risk customers are in each segment?
2. What's the average number of support tickets per segment?
3. Is there a correlation between website visits and support tickets?
4. Which customer segment has the highest churn risk?

## Additional Resources

- [Cloud Composer Documentation](https://cloud.google.com/composer/docs)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)