from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import random

topic_name = 'rand-int-topic'

conf = {
    'bootstrap.servers': 'localhost:9092'
}

admin_client = AdminClient(conf)

topic_list = [NewTopic(topic_name, num_partitions=2, replication_factor=1)]
admin_client.create_topics(new_topics=topic_list, validate_only=False)

producer = Producer(conf)

for _ in range(10):
    rand_int = random.randint(0, 1000)
    key = 'even' if rand_int % 2 == 0 else 'odd'
    producer.produce(topic_name, str(rand_int).encode('utf-8'), key=key.encode('utf-8'))
    producer.poll(0)

producer.flush()
print("Random integers have been sent to the topic.")