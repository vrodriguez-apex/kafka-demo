from kafka import KafkaConsumer
import sys

TOPIC = 'bitusdt-avg-topic'

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=['localhost:9092'],
    enable_auto_commit=False,
    group_id='my_group_id',
    value_deserializer=lambda x: x.decode('utf-8'),
    auto_offset_reset='earliest'
)

try:
    for message in consumer:
        print(message.value)
except KeyboardInterrupt:
    print('Cerrando consumer...')
    sys.exit()