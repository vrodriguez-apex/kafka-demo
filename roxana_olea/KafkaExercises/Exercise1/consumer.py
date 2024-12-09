from confluent_kafka import Consumer, KafkaException, KafkaError

topic_name = 'lines-of-files-topic'

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'file-consumer-app',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe([topic_name])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            print("Received message: NONE")
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                break
            else:
                raise KafkaException(msg.error())
        print(f"Received message: {msg.value().decode('utf-8')}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()