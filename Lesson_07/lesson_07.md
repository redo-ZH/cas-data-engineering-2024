# Customer Risk Analysis Pipeline with Cloud Composer
Data Engineering Course - ZHAW School of Management and Law

## What is Cloud Composer? (5 minutes)
Cloud Composer is Google Cloud's managed Apache Airflow service for:
- Orchestrating data pipelines
- Managing workflows across cloud services
- Scheduling and monitoring tasks

When to use Cloud Composer vs. Dataflow:
```
Cloud Composer:
✓ Orchestrating multiple tasks
✓ Managing dependencies between jobs
✓ Scheduling regular jobs
✓ Coordinating different GCP services

Dataflow:
✓ Processing large datasets
✓ Real-time data transformations
✓ Complex data processing
✓ ETL operations
```

## Setup Instructions (10 minutes)

### 1. Find Your Service Account
```bash
# List service accounts in your project
gcloud iam service-accounts list

# The output will look like:
# NAME                                    EMAIL
# Compute Engine default service account  YOUR-PROJECT-NUMBER-compute@developer.gserviceaccount.com
```
Note: Look for the Compute Engine default service account, you'll need this for creating the Composer environment.

### 2. Create Cloud Composer Environment
```bash
# Set your project
gcloud config set project cas-daeng-2024-pect

# Create Cloud Composer environment (replace SERVICE-ACCOUNT with your account)
gcloud composer environments create customer-analysis \
    --location europe-west6 \
    --image-version composer-3-airflow-2 \
    --environment-size small \
    --service-account="YOUR-SERVICE-ACCOUNT-EMAIL"
```

### 3. Verify Data Files
```bash
# Check input files
gsutil ls gs://retail-data-pect/
```

Expected files:
- clickstream.csv
- support_tickets.csv

## Pipeline Implementation (10 minutes)

### 1. Create and Upload DAG File
Create a file named `customer_risk_analysis.py`:

```python
[Previous DAG code remains the same, no changes needed]
```

### 2. Deploy the DAG
```bash
# Get DAG folder location
BUCKET=$(gcloud composer environments describe customer-analysis \
    --location europe-west6 \
    --format="get(config.dagGcsPrefix)")

# Upload DAG file
gsutil cp customer_risk_analysis.py ${BUCKET}/dags/
```

### 3. Access Airflow UI
```bash
# Get the Airflow UI URL
gcloud composer environments describe customer-analysis \
    --location europe-west6 \
    --format="get(config.airflowUri)"
```

## Running and Monitoring (5 minutes)

### 1. Start the Pipeline
1. Open Airflow UI URL
2. Find "customer_risk_analysis" DAG
3. Toggle ON the DAG
4. Click "Trigger DAG" to run manually

### 2. Monitor Execution
In Airflow UI:
- Graph View: Check task dependencies
- Tree View: See execution history
- Logs: Debug any issues

### 3. Validate Results
```sql
-- Check processed data
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
```

## Troubleshooting Tips

1. **Environment Creation Issues**
   - Verify service account permissions
   - Check project quotas
   - Ensure APIs are enabled

2. **DAG Not Appearing**
   - Wait 3-5 minutes for sync
   - Check Python syntax
   - View Cloud Composer logs

3. **Task Failures**
   ```bash
   # View logs
   gcloud composer environments storage logs list customer-analysis \
       --location europe-west6
   ```

## Cleanup
```bash
# Delete environment when done
gcloud composer environments delete customer-analysis \
    --location europe-west6
```

## Best Practices for DAG Development

1. **Task Design**
   - Keep tasks atomic
   - Include error handling
   - Set appropriate retries

2. **Performance**
   - Avoid long-running tasks
   - Use appropriate operators
   - Set reasonable timeouts

3. **Monitoring**
   - Check task logs
   - Monitor task duration
   - Set up notifications

## Quick Reference

### Important URLs
- GCP Console: console.cloud.google.com
- IAM & Admin: console.cloud.google.com/iam-admin
- Cloud Composer: console.cloud.google.com/composer

### Key Commands
```bash
# List service accounts
gcloud iam service-accounts list

# Check environment status
gcloud composer environments describe customer-analysis \
    --location europe-west6

# View logs
gcloud composer environments storage logs list customer-analysis \
    --location europe-west6
```

### Common Issues & Solutions
1. **Permission Denied**
   - Check service account roles
   - Verify project permissions

2. **Resource Not Found**
   - Verify resource names
   - Check project/location

3. **Task Timeout**
   - Adjust timeout settings
   - Check resource constraints