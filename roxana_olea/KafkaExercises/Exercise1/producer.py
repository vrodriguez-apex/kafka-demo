from confluent_kafka import Producer
import os

topic_name = 'lines-of-files-topic'

conf = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

file_path = os.path.join('resources', 'la_biblioteca_de_babel.txt')
with open(file_path, 'r', encoding='utf-8') as file:
    for line in file:
        producer.produce(topic_name, line.encode('utf-8'), callback=delivery_report)
        producer.poll(0)

producer.flush()
print("All lines have been sent to the topic.")