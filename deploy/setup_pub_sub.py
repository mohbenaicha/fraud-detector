import time
import os
import json
from dotenv import load_dotenv
from google.cloud import pubsub_v1


load_dotenv()
PROJECT_ID = os.environ.get("PROJECT_ID")
TOPIC_ID = os.environ.get("TOPIC_ID")
SUBSCRIPTION_ID = os.environ.get("SUBSCRIPTION_ID")

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

try:
    topic = publisher.create_topic(request={"name": topic_path})
except Exception as e:
    print(f"Topic may already exist: {e}")

try:
    subscription = subscriber.create_subscription(
        request={
            "name": subscription_path,
            "topic": topic_path,
            "ack_deadline_seconds": 60,
        }
    )
except Exception as e:
    print(f"Subscription may already exist: {e}")


def test_pubsub_service(project_id, topic_id, subscription_id):
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    topic_path = publisher.topic_path(project_id, topic_id)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    sample_transaction = {
        "Transaction Amount": 123.45,
        "Quantity": 2,
        "Customer Age": 35,
        "Account Age Days": 450,
        "Transaction Hour": 14,
        "Transaction Amount_missing": 0,
        "Quantity_missing": 0,
        "Customer Age_missing": 0,
        "Account Age Days_missing": 0,
        "Transaction Hour_missing": 0,
        "Payment Method_missing": 0,
        "Product Category_missing": 0,
        "Device Used_missing": 0,
        "Amount_x_AccountAge": 0.274,
        "Quantity_per_AccountAge": 0.004,
        "Transaction_DayNight": 1,
        "Log_Transaction_Amount": 4.812,
        "Payment Method_bank transfer": 0,
        "Payment Method_credit card": 1,
        "Payment Method_debit card": 0,
        "Product Category_electronics": 1,
        "Product Category_health & beauty": 0,
        "Product Category_home & garden": 0,
        "Product Category_toys & games": 0,
        "Device Used_mobile": 1,
        "Device Used_tablet": 0
    }

    data = json.dumps(sample_transaction).encode("utf-8")
    future = publisher.publish(topic_path, data)
    message_id = future.result()
    print(f"Published test message with ID: {message_id}")

    def callback(message):
        received = json.loads(message.data.decode("utf-8"))
        print("Received message:", received)
        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print("Listening for messages on subscription for 5 seconds ....")
    
    try:
        time.sleep(5)  # allow time to receive message
    finally:
        streaming_pull_future.cancel()
        print("Test complete, subscption listener stoped...")

if __name__ == "__main__":
    test_pubsub_service(PROJECT_ID, TOPIC_ID, SUBSCRIPTION_ID)