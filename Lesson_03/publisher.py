from google.cloud import pubsub_v1

PROJECT_ID = "cas-daeng-2024-pect"  # Students replace with their project
# Create publisher
publisher = pubsub_v1.PublisherClient()
topic_path = f"projects/{PROJECT_ID}/topics/retail-topic"  # Students replace with their project

# Publish messages
for i in range(5):  # Send 5 messages
    message = f"Message number {i+1}"
    future = publisher.publish(topic_path, message.encode('utf-8'))
    message_id = future.result()
    print(f"Published: {message} (ID: {message_id})")
