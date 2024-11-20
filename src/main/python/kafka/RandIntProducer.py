from random import randint
from time import sleep

import kafka.errors
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.producer import KafkaProducer
from confluent_kafka.serialization import IntegerSerializer, StringSerializer

host = "localhost:9092"
topic = "kafka-apex-python-rand-int"

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
    value_serializer = IntegerSerializer()
)

integer_type = ""

try:
    while(True):
        rand_int = int(randint(0, 1000))
        if rand_int % 2 == 0:
            integer_type = "even"
        else:
            integer_type = "odd"

        kafka_producer.send(topic = topic, key = integer_type, value = rand_int)
        sleep(1)
except (InterruptedError, KeyboardInterrupt) as e:
    kafka_producer.flush()
    kafka_producer.close()
    raise Exception(e)