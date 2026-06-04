from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.watermark_strategy import WatermarkStrategy

import os

env = StreamExecutionEnvironment.get_execution_environment()

# Add the required Kafka connector jar
kafka_jar = "file:///app/jobs/flink-sql-connector-kafka-1.17.1.jar"
env.add_jars(kafka_jar)
env.set_parallelism(1)

source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:29092") \
    .set_topics("rider-updates") \
    .set_group_id("flink") \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

ds = env.from_source(
    source,
    WatermarkStrategy.no_watermarks(),
    "Kafka Source"
)

ds.print()

env.execute("Radiosonde Kafka Stream")