from confluent_kafka import Consumer, KafkaException, KafkaError

topic_name = 'rand-int-topic'

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe([topic_name])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            print("msg: None")
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                break
            else:
                raise KafkaException(msg.error())
        print(f"Received message: {msg.value().decode('utf-8')} with key: {msg.key().decode('utf-8')} from partition: {msg.partition()}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()