from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# Fixed import for BigQuery operator
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

# Create the DAG
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
                    COUNT(*) as website_visits,
                    COUNTIF(action = 'add_to_cart') as cart_additions,
                    DATE_DIFF(CURRENT_DATE(), DATE(MAX(timestamp)), DAY) as days_since_last_visit
                FROM `{PROJECT_ID}.{DATASET}.clickstream`
                GROUP BY customer_id
            ),
            customer_support AS (
                SELECT
                    customer_id,
                    COUNT(*) as total_tickets,
                    COUNTIF(status = 'Open') as open_tickets,
                    MAX(churn_risk) as max_churn_risk
                FROM `{PROJECT_ID}.{DATASET}.support_tickets`
                GROUP BY customer_id
            )
            SELECT
                c.customer_id,
                c.name,
                c.email,
                c.segment,
                COALESCE(ca.website_visits, 0) as website_visits,
                COALESCE(ca.cart_additions, 0) as cart_additions,
                COALESCE(ca.days_since_last_visit, 999) as days_since_last_visit,
                COALESCE(cs.total_tickets, 0) as support_tickets,
                COALESCE(cs.open_tickets, 0) as open_tickets,
                COALESCE(cs.max_churn_risk, 0) as churn_risk,
                CASE 
                    WHEN ca.days_since_last_visit > 30 AND cs.max_churn_risk > 0.5 
                        THEN 'High Risk'
                    WHEN ca.days_since_last_visit > 30 OR cs.max_churn_risk > 0.5 
                        THEN 'Medium Risk'
                    ELSE 'Low Risk'
                END as risk_category
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
                risk_category,
                COUNT(*) as customer_count,
                AVG(website_visits) as avg_visits,
                AVG(cart_additions) as avg_cart_additions,
                AVG(support_tickets) as avg_support_tickets,
                AVG(churn_risk) as avg_churn_risk
            FROM `{PROJECT_ID}.{DATASET}.customer_risk_analysis`
            GROUP BY segment, risk_category
            ORDER BY 
                CASE risk_category
                    WHEN 'High Risk' THEN 1
                    WHEN 'Medium Risk' THEN 2
                    WHEN 'Low Risk' THEN 3
                END,
                segment;
            """,
            'useLegacySql': False
        }
    },
    dag=dag,
)

# Set task dependencies
[load_clickstream, load_support_tickets] >> create_risk_analysis >> create_daily_summary