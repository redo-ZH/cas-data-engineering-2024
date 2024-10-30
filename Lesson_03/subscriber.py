from google.cloud import pubsub_v1


PROJECT_ID = "cas-daeng-2024-pect"  # Students replace with their project
# Create subscriber
subscriber = pubsub_v1.SubscriberClient()
subscription_path = f"projects/{PROJECT_ID}/subscriptions/retail-sub"  # Students replace with their project


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
