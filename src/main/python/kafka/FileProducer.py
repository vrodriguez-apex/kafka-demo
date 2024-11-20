from random import randint
from time import sleep

import kafka.errors
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.producer import KafkaProducer
from confluent_kafka.serialization import IntegerSerializer, StringSerializer

host = "localhost:9092"
topic = "kafka-apex-python-file"
path = "<<PATH>>"

admin_client = KafkaAdminClient(
    bootstrap_servers=host
)

try:
    topic_list = []
    topic_list.append(NewTopic(name=topic, num_partitions=3, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
except kafka.errors.TopicAlreadyExistsError as e:
    pass

kafka_producer = KafkaProducer(
    bootstrap_servers = host,
    key_serializer = StringSerializer(),
    value_serializer = StringSerializer()
)

try:
    with open(path, 'r') as file:
        for line in file:
            kafka_producer.send(topic = topic, value = line)
            sleep(1)
except (InterruptedError, KeyboardInterrupt) as e:
    kafka_producer.flush()
    kafka_producer.close()
    raise Exception(e)