# Data Processing with Dataflow

### System Architecture
Our e-commerce data processing system consists of three main components:
```
[Cloud Storage] → [Dataflow] → [BigQuery]
↑                    ↓            ↓
Raw Data        Processing     Analysis
CSV Files       Pipeline       Results
```

- **Cloud Storage**: Stores our raw transaction data (transactions.csv)
- **Dataflow**: Processes and transforms the data
- **BigQuery**: Enables analysis of the processed data

### Prerequisites
- Google Cloud Console access
- Your project ID (cas-daeng-2024-YOUR_USERNAME)
- Completed previous tutorial where we loaded data into Cloud Storage
- The `ecommerce_pipeline.py` file from your course materials

### Setup Steps

1. Open Google Cloud Console and navigate to Cloud Shell

2. Verify and set your project
   ```bash
   # Check your current project
   gcloud config get-value project
   
   # If needed, set your project (replace YOUR_USERNAME)
   gcloud config set project cas-daeng-2024-YOUR_USERNAME
   ```

3. Install Apache Beam with GCP support
   ```bash
   pip install apache-beam[gcp]
   ```

4. Enable and verify the required Google Cloud APIs
   ```bash
   # Enable APIs
   gcloud services enable dataflow.googleapis.com bigquery.googleapis.com
   
   # Verify Dataflow is enabled
   gcloud services list --enabled | grep dataflow
   ```

5. Create a BigQuery dataset to store our data
   ```bash
   # Replace YOUR_USERNAME with your username
   bq mk --dataset \
       --description "E-commerce analytics dataset" \
       --location US \
       cas-daeng-2024-YOUR_USERNAME:ecommerce
   ```

6. Upload the pipeline code
   - Create a working directory:
     ```bash
     mkdir dataflow_demo
     cd dataflow_demo
     ```
   - Click the 'Upload file' button in Cloud Shell (three-dot menu or folder icon)
   - Select the `ecommerce_pipeline.py` file from your local computer
   - Verify the upload:
     ```bash
     ls
     ```

7. Run the Dataflow pipeline
   ```bash
   python ecommerce_pipeline.py
   ```
   
8. Monitor your pipeline:
   - Go to Navigation menu → Dataflow
   - Click on your running job
   - Watch the execution graph and job progress
   - Monitor elements processed and system metrics

### Analyzing the Data

Once the pipeline completes, analyze your data in BigQuery:
- Go to Navigation menu → BigQuery
- Find your project (cas-daeng-2024-YOUR_USERNAME)
- Look for the 'ecommerce' dataset
- Find the 'transactions' table

Try these initial analysis queries:

1. **Total sales by category**
   ```sql
   SELECT 
     category,
     COUNT(*) as number_of_sales,
     ROUND(SUM(total_amount), 2) as total_sales,
     ROUND(AVG(total_amount), 2) as avg_transaction_value
   FROM `cas-daeng-2024-YOUR_USERNAME.ecommerce.transactions`
   GROUP BY category
   ORDER BY total_sales DESC;
   ```

2. **Top 10 customers by spending**
   ```sql
   SELECT 
     customer_id,
     COUNT(*) as number_of_transactions,
     SUM(quantity) as total_items_bought,
     ROUND(SUM(total_amount), 2) as total_spent
   FROM `cas-daeng-2024-YOUR_USERNAME.ecommerce.transactions`
   GROUP BY customer_id
   ORDER BY total_spent DESC
   LIMIT 10;
   ```

### Troubleshooting

If you encounter:

1. **"Dataset not found" error**
   - Check that you replaced YOUR_USERNAME with your actual username
   - Run `bq ls` to verify your dataset exists
   - Try creating the dataset again

2. **Pipeline fails to start**
   - Verify both APIs are enabled (Dataflow and BigQuery)
   - Check that your data file exists in Cloud Storage

3. **Permission errors**
   - Verify you're in the correct project using `gcloud config get-value project`
   - Make sure you're properly authenticated in Cloud Shell

4. **File upload issues**
   - Make sure you're in the dataflow_demo directory before uploading
   - Try refreshing Cloud Shell if the upload button isn't responsive
   - Verify the file was uploaded successfully using `ls`

Remember: Always replace 'YOUR_USERNAME' in the queries with your actual username from your project ID.