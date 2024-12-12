import six
import sys
import os
import json
from time import sleep
from random import randint
from kafka.producer import KafkaProducer
from kafka.errors import KafkaError
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from kafka.admin import KafkaAdminClient, NewTopic

if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

API_KEY = '' 
API_URL = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "cryptocurrency"

parameters = {
    'start': '1',
    'limit': '10',
    'convert': 'USD'
}

headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': API_KEY,
}

session = Session()
session.headers.update(headers)

# Configuración de Kafka
try:
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
    topic_list = [NewTopic(name=TOPIC, num_partitions=3, replication_factor=1)]
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
except KafkaError as e:
    print(f"Error creando el tópico: {e}")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    key_serializer=lambda k: json.dumps(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

try:
    response = session.get(API_URL, params=parameters)
    if response.status_code == 200:
        data = response.json()

        # Contador de mensajes
        message_count = 0

        for item in data['data']:
            if message_count >= 30:  # Enviar solo 30 mensajes
                break

            message = {
                "id": item["id"],
                "name": item["name"],
                "symbol": item["symbol"],
                "price": item["quote"]["USD"]["price"]
            }
            producer.send(TOPIC, value=message)
            print(f"Mensaje enviado: {message}")

            message_count += 1
            sleep(1) 
    else:
        print(f"Error en la solicitud API: {response.status_code}")
except (ConnectionError, Timeout, TooManyRedirects) as e:
    print(f"Error al conectarse al API: {e}")
finally:
    producer.flush()
    producer.close()