from confluent_kafka.admin import AdminClient, NewTopic
from client import admin_config

admin = AdminClient(admin_config)


topic = NewTopic(
    topic="rider-updates",
    num_partitions=2,
    replication_factor=1
)

# Creating multiple topics at once
"""topics = [
    NewTopic(topic="rider-updates", num_partitions=2, replication_factor=1),
    NewTopic(topic="driver-updates", num_partitions=3, replication_factor=1),
    NewTopic(topic="trip-events", num_partitions=4, replication_factor=1),
]"""
print(f"Creating topic '{topic.topic}'...")

futures = admin.create_topics([topic])

for topic, future in futures.items():
    try:
        future.result()
        print(f"Topic '{topic}' created successfully.")
    except Exception as e:
        print(f"Failed to create topic '{topic}': {e}")