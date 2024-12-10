import six
import sys
import sqlite3
from confluent_kafka.serialization import StringDeserializer
from kafka.consumer import KafkaConsumer

topic = "lines-of-files-topic-web"

kafka_consumer = KafkaConsumer(
    key_deserializer=StringDeserializer(),
    value_deserializer=StringDeserializer(),
    group_id="file-consumer-app",
    enable_auto_commit= True,
    auto_offset_reset="earliest",
   # value_deserializer=lambda x: x.decode('utf-8')
)
kafka_consumer.subscribe([topic])

while(True):
    for record in kafka_consumer:
        if record is None:
            continue

        print(f"Key: {record.key}, Value: {record.value}")
        print(f"Partition: {record.partition}, offset: {record.offset}")
        sleep(1)
