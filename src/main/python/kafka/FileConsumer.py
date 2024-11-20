from confluent_kafka.serialization import StringDeserializer
from kafka.consumer import KafkaConsumer

host = "localhost:9092"
topic = "kafka-apex-python-file"
group_id = "file-consumer-app"

kafka_consumer = KafkaConsumer(
    bootstrap_servers = host,
    key_deserializer=StringDeserializer(),
    value_deserializer=StringDeserializer(),
    group_id=group_id,
    auto_offset_reset="earliest"
)
kafka_consumer.subscribe([topic])

while(True):
    for record in kafka_consumer:
        if record is None:
            continue

        print(f"Key: {record.key}, Value: {record.value}")
        print(f"Partition: {record.partition}, offset: {record.offset}")
