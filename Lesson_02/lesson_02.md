# Getting Started with Google Cloud Platform (GCP)
## Data Engineering Course Reference Guide

## Table of Contents
1. [Initial Setup](#initial-setup)
2. [Billing and Credits](#billing-and-credits)
3. [Project Setup](#project-setup)
4. [Cloud Shell Basics](#cloud-shell-basics)
5. [Regions and Zones](#regions-and-zones)
6. [IAM Setup](#iam-setup)
7. [Logging Basics](#logging-basics)
8. [Practice Exercise](#practice-exercise)
9. [Important Commands Reference](#important-commands-reference)
10. [Best Practices](#best-practices)

## Initial Setup
1. Navigate to [Google Cloud Console](https://console.cloud.google.com)
2. If you don't have a Google account:
   - Click "Create account"
   - Follow the registration process
   - Choose "For myself" option
3. Once logged in, accept the terms of service

## Billing and Credits
### Applying Student Credits
1. Follow the link provided in Moodle
2. Verify your email address
3. Check your email and follow the link
4. Ensure you're logged in with the correct Google account
5. Enter the coupon code from your email

### Setting Up Budget Alerts
1. Go to Billing > Budgets & Alerts
2. Click "Create Budget"
3. Fill in the following:
   ```
   Budget name: [YourName]-Budget
   Projects: [Select your project]
   Amount: [Set to your credit amount]
   ```
4. Set up alerts:
   - 50% of budget
   - 75% of budget
   - 90% of budget
5. Add email notifications

## Project Setup
### Creating a New Project
1. Go to IAM & Admin > Manage Resources
2. Click "Create Project"
3. Use naming convention:
   ```
   Project name: [department]-[purpose]-[name]
   Example: cas-daeng-pect
   
   Where:
   - cas = Certificate of Advanced Studies (CAS)
   - daeng = Data Engineering
   - pect = Your name or identifier
   ```
4. Click "Create"

### Project Best Practices
- Use lowercase letters and hyphens
- Follow the cas-daeng-[name] format exactly
- Keep names under 30 characters
- Use only hyphens as separators, no underscores or spaces

### Important Notes About Project Names
- The project name must be unique across all of Google Cloud Platform
- Once created, the project ID cannot be changed
- You'll use this project throughout the CAS Data Engineering course
- Make sure to remember or note down your project ID

## Cloud Shell Basics
### Opening Cloud Shell
1. Click the Cloud Shell icon (>_) in top-right corner
2. Wait for environment to initialize

### Essential Cloud Shell Commands
```bash
# Check current project
gcloud config list project

# Set active project
gcloud config set project [PROJECT_ID]

# List available zones
gcloud compute zones list

# List available regions
gcloud compute regions list

# Create a new Cloud Storage bucket
gsutil mb gs://[BUCKET_NAME]

# Upload a file to bucket
gsutil cp [LOCAL_FILE] gs://[BUCKET_NAME]/

# List bucket contents
gsutil ls gs://[BUCKET_NAME]/

# Download file from bucket
gsutil cp gs://[BUCKET_NAME]/[FILE_NAME] [LOCAL_PATH]
```

### Cloud Shell Tips
- Sessions timeout after 1 hour of inactivity
- 5GB of persistent disk storage available
- Pre-authenticated with your GCP credentials
- Use `Tab` for command auto-completion
- Type `gcloud help` for command documentation

## Regions and Zones
### Checking Available Regions
```bash
gcloud compute regions list
```

### Region Selection Factors
1. Data residency requirements
2. Latency to users
3. Service availability
4. Pricing
5. Disaster recovery needs

### Zone Commands
```bash
# List all zones
gcloud compute zones list

# List zones in specific region
gcloud compute zones list | grep [REGION-NAME]
```

## IAM Setup
### Viewing Current IAM Roles
```bash
# List IAM policies for project
gcloud projects get-iam-policy [PROJECT_ID]
```

### Common Roles for Data Engineering
- roles/viewer: Read-only access
- roles/editor: Read-write access
- roles/bigquery.dataViewer: Read BigQuery data
- roles/bigquery.dataEditor: Write BigQuery data
- roles/storage.objectViewer: Read GCS objects
- roles/storage.objectCreator: Write GCS objects

### Adding a User (via Cloud Shell)
```bash
gcloud projects add-iam-policy-binding [PROJECT_ID] \
    --member="user:[EMAIL]" \
    --role="roles/viewer"
```

## Logging Basics
### Accessing Logs
1. Navigate to Operations > Logging > Logs Explorer
2. Basic query syntax:
```
resource.type = "gce_instance"
severity >= ERROR
```

### Common Log Filters
```
# View all errors
severity >= ERROR

# View specific service logs
resource.type = "cloud_function"

# Time-based filtering
timestamp >= "2024-01-01T00:00:00Z"

# Combined filtering
resource.type = "cloud_function" AND severity >= ERROR
```

## Practice Exercise
1. Create a bucket:
```bash
gsutil mb gs://[YOUR_NAME]-test-bucket
```

2. Create a sample CSV file:
```bash
echo "id,name,value
1,test1,100
2,test2,200" > sample.csv
```

3. Upload to bucket:
```bash
gsutil cp sample.csv gs://[YOUR_NAME]-test-bucket/
```

4. Make file public:
```bash
gsutil acl ch -u AllUsers:R gs://[YOUR_NAME]-test-bucket/sample.csv
```

5. Verify access in incognito window:
```
https://storage.googleapis.com/[YOUR_NAME]-test-bucket/sample.csv
```

## Important Commands Reference
```bash
# Project Management
gcloud config list
gcloud config set project [PROJECT_ID]
gcloud projects list

# Storage Commands
gsutil ls
gsutil mb gs://[BUCKET_NAME]
gsutil cp [SOURCE] [DESTINATION]
gsutil rm gs://[BUCKET_NAME]/[OBJECT_NAME]

# IAM Commands
gcloud projects get-iam-policy [PROJECT_ID]
gcloud projects add-iam-policy-binding [PROJECT_ID] --member=[MEMBER] --role=[ROLE]

# Region/Zone Commands
gcloud compute regions list
gcloud compute zones list
```

## Best Practices
1. Project Organization
   - Use consistent naming conventions
   - Implement proper access controls
   - Set up billing alerts early

2. Security
   - Follow principle of least privilege
   - Regularly review access permissions
   - Keep service account keys secure

3. Cost Management
   - Monitor billing dashboard regularly
   - Set up budget alerts
   - Clean up unused resources

4. Resource Management
   - Document resource creation
   - Use labels for resources
   - Keep track of created resources

### Important Links
- [GCP Console](https://console.cloud.google.com)
- [GCP Documentation](https://cloud.google.com/docs)
- [Cloud Shell Documentation](https://cloud.google.com/shell/docs)
- [GCP Pricing Calculator](https://cloud.google.com/products/calculator)

Remember to replace placeholders (marked with []) with your actual values when using commands.