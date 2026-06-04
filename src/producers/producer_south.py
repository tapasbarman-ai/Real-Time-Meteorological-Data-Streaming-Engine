import json 
from confluent_kafka import Producer
from src.client import producer_config  


producer = Producer(producer_config)

print("Producing messages to topic 'rider-updates' for South location...")

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

while True:
    rider_name = input("South rider name : ")
    
    data = json.dumps(
        {
            "rider_name": rider_name,
            "location": "South"
        }
    )
    producer.produce(
        topic="rider-updates",
        key="location-update",
        value=data,
        partition=0,
        callback=delivery_report
    )

    producer.poll(0)  # Trigger delivery report callbacks