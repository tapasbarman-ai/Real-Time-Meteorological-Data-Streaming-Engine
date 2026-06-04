import sys
from confluent_kafka import Consumer # import consumer class
from client import consumer_config

group = sys.argv[1]

config = consumer_config.copy()
config["group.id"] = group


consumer = Consumer(config) # create a consumer object
consumer.subscribe(["rider-updates"])

# To consume from multiple topics
"""consumer.subscribe([
    "rider-updates",
    "driver-updates",
    "trip-events"
])"""


print(f"Consuming messages from topic 'rider-updates' as group '{group}'...")


while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue

    print(f"Received message: {msg.value().decode('utf-8')}")