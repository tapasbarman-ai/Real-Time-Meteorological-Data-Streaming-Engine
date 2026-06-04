import json
import random
import time 
from confluent_kafka import Producer
from src.client import producer_config

producer = Producer(producer_config)
print("Producing messages to topic 'rider-updates' for random location...")

riders = ["Alice", "Bob", "Charlie", "David", "Eve"]
locations = ["North", "South"]

print("Producing messages to topic 'rider-updates' for random location...")

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

while True:
    riders_name = random.choice(riders)
    locations_name = random.choice(locations)

    data = json.dumps(
        {
            "rider_name": riders_name,
            "location": locations_name
        }
    )

    producer.produce(
        topic="rider-updates",
        key="location-update",
        value=data,
        callback=delivery_report
    )
    producer.poll(0)  # Trigger delivery report callbacks
    time.sleep(1)  # Sleep for a while before sending the next message

