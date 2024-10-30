# Data Ingestion with Cloud Storage and Pub/Sub

This guide covers how to use Google Cloud Storage for batch data ingestion and Cloud Pub/Sub for streaming data.

## Prerequisites
- Google Cloud account
- Python installed
- Access to Cloud Shell or local terminal

## Part 1: Cloud Storage - Batch Data Processing

### Setup via GUI
1. Navigate to Cloud Console (console.cloud.google.com)
2. Go to Cloud Storage in the navigation menu
3. Create a new bucket:
   - Click "CREATE BUCKET"
   - Name: `retail-data-[yourname]`
   - Location: `us-central1`
   - Leave other settings as default
4. Upload files through the web interface:
   - Click "UPLOAD FILES"
   - Select your data files
   - Monitor upload progress

### Setup via Cloud Shell
```bash
# Create a bucket
gsutil mb -l us-central1 gs://retail-data-[yourname]

# Upload files
gsutil cp data.csv gs://retail-data-[yourname]/

# Upload multiple files in parallel
gsutil -m cp *.csv gs://retail-data-[yourname]/

# List bucket contents
gsutil ls gs://retail-data-[yourname]/

# Get file info
gsutil stat gs://retail-data-[yourname]/data.csv
```

## Part 2: Pub/Sub - Streaming Data

### Initial Setup
```bash
# Install required library
pip install google-cloud-pubsub

# Authenticate (important!)
gcloud auth application-default login

# Create a topic
gcloud pubsub topics create retail-topic

# Create a subscription
gcloud pubsub subscriptions create retail-sub --topic=retail-topic

# Verify setup
gcloud pubsub topics list
gcloud pubsub subscriptions list
```

### Python Scripts

1. Create `publisher.py`:
```python
from google.cloud import pubsub_v1

# Create publisher
publisher = pubsub_v1.PublisherClient()

# Replace with your project ID (get it from: gcloud config get-value project)
project_id = "your-project-id"
topic_path = f"projects/{project_id}/topics/retail-topic"

# Publish messages
for i in range(5):  # Send 5 messages
    message = f"Message number {i+1}"
    future = publisher.publish(topic_path, message.encode('utf-8'))
    message_id = future.result()
    print(f"Published: {message} (ID: {message_id})")
```

2. Create `subscriber.py`:
```python
from google.cloud import pubsub_v1

# Create subscriber
subscriber = pubsub_v1.SubscriberClient()

# Replace with your project ID (get it from: gcloud config get-value project)
project_id = "your-project-id"
subscription_path = f"projects/{project_id}/subscriptions/retail-sub"

def callback(message):
    print(f"Received: {message.data.decode('utf-8')}")
    message.ack()

print("Waiting for messages...")
streaming_pull = subscriber.subscribe(subscription_path, callback=callback)

# Keep the main thread alive
try:
    streaming_pull.result()
except KeyboardInterrupt:
    streaming_pull.cancel()
    print("\nStopped listening.")
```

### Running the Demo

1. Get your project ID:
```bash
gcloud config get-value project
```

2. Update both Python scripts with your project ID

3. Open two terminal windows

4. In Terminal 1, run the subscriber:
```bash
python subscriber.py
```

5. In Terminal 2, run the publisher:
```bash
python publisher.py
```

You should see messages being published in Terminal 2 and received in Terminal 1.

### Cleanup

When you're done, clean up your resources:
```bash
# Delete Pub/Sub resources
gcloud pubsub subscriptions delete retail-sub
gcloud pubsub topics delete retail-topic

# Delete Cloud Storage bucket
gsutil rm -r gs://retail-data-[yourname]/
```

## Key Concepts

### Cloud Storage
- Object storage service for batch data
- Organizes files in buckets
- Supports parallel uploads
- Good for large datasets

### Pub/Sub
- Messaging service for streaming data
- Publishers send messages to topics
- Subscribers receive messages from subscriptions
- Real-time data processing
- Decoupled architecture (publishers don't know about subscribers)

## Common Issues and Solutions

1. Authentication errors:
   ```bash
   gcloud auth application-default login
   ```

2. Import errors:
   ```bash
   pip install google-cloud-pubsub
   ```

3. Project ID issues:
   - Check your project ID:
     ```bash
     gcloud config get-value project
     ```
   - Make sure you've updated the scripts with the correct ID

4. Permission issues:
   - Verify your roles in IAM
   - Make sure you're logged in
   - Check topic and subscription exist:
     ```bash
     gcloud pubsub topics list
     gcloud pubsub subscriptions list
     ```

## Best Practices

1. Always clean up resources after testing
2. Use parallel upload (-m) for multiple files
3. Properly handle message acknowledgment
4. Check authentication before running scripts
5. Verify infrastructure setup before running code

## Additional Resources
- [Cloud Storage Documentation](https://cloud.google.com/storage/docs)
- [Pub/Sub Documentation](https://cloud.google.com/pubsub/docs)
- [Python Client Libraries](https://cloud.google.com/python/docs/reference)