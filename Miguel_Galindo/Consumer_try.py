import six
import sys
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

from confluent_kafka.serialization import StringDeserializer
from kafka.consumer import KafkaConsumer
import matplotlib.pyplot as plt
import json

#host = "localhost:9092"
topic = "lines-of-files-topic-web"
group_id = "file-consumer-app"

kafka_consumer = KafkaConsumer(
#    bootstrap_servers = host,
    key_deserializer=StringDeserializer(),
    value_deserializer=StringDeserializer(),
    group_id=group_id,
    auto_offset_reset="earliest"
)
kafka_consumer.subscribe([topic])

crypto_data = []

while(True):
    for record in kafka_consumer:
        if record is None:
            continue

        print(f"Key: {record.key}, Value: {record.value}")
        print(f"Partition: {record.partition}, offset: {record.offset}")
        
        # Parse the JSON data
        data = json.loads(record.value)
        
        # Extract relevant information (assuming 'data' is a list of dictionaries)
        for item in data['data']:
            crypto_data.append({
                'name': item['name'],
                'price': item['quote']['USD']['price']
            })
        
        # Plot the data
        names = [item['name'] for item in crypto_data]
        prices = [item['price'] for item in crypto_data]
        
        plt.figure(figsize=(10, 5))
        plt.bar(names, prices)
        plt.xlabel('Cryptocurrency')
        plt.ylabel('Price (USD)')
        plt.title('Cryptocurrency Prices')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()