import json
from confluent_kafka import Producer # import producer class
from client import producer_config

producer = Producer(producer_config) # create a prodicer object to use 

print("Producing messages to topic 'my-topic'...")

while True:                           # Specifying which data to send which  topic and which partition to send to 
    line  = input("> ")
    rider_name, location  = line.split(",") 
    partition  = 0 if location.lower() == "north" else 1 

    data  = json.dumps(
        {
            "rider_name": rider_name,
            "location": location
        }
    )

    producer.produce(topic="rider-updates",
        key="location-update",
        value=data,
        partition=partition)
    
    producer.flush()