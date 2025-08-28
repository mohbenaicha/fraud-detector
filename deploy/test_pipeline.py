import json
import os
from dotenv import load_dotenv
import pandas as pd
from google.cloud import pubsub_v1

load_dotenv() 
PROJECT_ID = os.environ.get("PROJECT_ID")
TOPIC_ID = os.environ.get("TOPIC_ID")

df = pd.read_csv("../Fraudulent_E-Commerce_Transaction_Data_2.csv")
sample = df.sample(1)

print("Target feature favalue is :", sample["Is Fraudulent"].values[0])
inputs = sample.drop('Is Fraudulent', axis=1).to_dict(orient="records")[0]
print("Sameple data to be sent to Pub/Sub:", inputs)

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

message_json = json.dumps(inputs)
message_bytes = message_json.encode("utf-8")

future = publisher.publish(topic_path, message_bytes)
print(f"Published test message ID: {future.result()}")

# Prediction can be found in logs ; note to self: publish predictions to new sub queue
