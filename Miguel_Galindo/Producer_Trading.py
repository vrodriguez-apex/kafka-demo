import six
import sys
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

from random import randint
from time import sleep

import kafka.errors
from kafka.admin import KafkaAdminClient, NewTopic
from requests import Request, Session #Librería para conectarse y adquirir los datos de la URL
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from kafka.producer import KafkaProducer
from confluent_kafka.serialization import IntegerSerializer, StringSerializer
import json

#host = "localhost:9092" -- It is not necesary to run the demo
api_url = 'https://Pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
#kafka_bootstrap_servers = 'localhost:9092'

parameters = {
  'start': '1',
  'limit': '10',  
  'convert': 'USD'
}

headers = {
  'Accepts': 'application/json',
  'X-CMC_PRO_API_KEY': '3c0b0061-1188-446c-87d7-081a5d05be88',
}

topic = "lines-of-files-topic-web"
#path = "C:\\Users\\wcetina\\kafka-demo\\src\\resources\\files\\la_biblioteca_de_babel.txt" -- It is not necesary to run the demo

session = Session()
session.headers.update(headers)

#admin_client = KafkaAdminClient(
    #bootstrap_servers=kafka_bootstrap_servers
#)

try:
    topic_list = []
    topic_list.append(NewTopic(name=topic, num_partitions=3, replication_factor=1))
    #admin_client.create_topics(new_topics=topic_list, validate_only=False)
except kafka.errors.TopicAlreadyExistsError as e:
    pass

kafka_producer = KafkaProducer(
    #bootstrap_servers=kafka_bootstrap_servers,
    key_serializer = StringSerializer(),
    value_serializer = StringSerializer()
)

try:
    response = session.get(api_url, params=parameters)
    data = json.loads(response.text)

    # Asumiendo que 'data' es una lista de diccionarios
    for item in data['data']:
        line = json.dumps(data, sort_keys=True, indent=2) #Indenta el Json para que se vea bien.
        kafka_producer.send(topic=topic, value=line) #Creación del mensaje.
        print(data)
        print(line)  # Agregado para ver lo que se genera el Producer
        sleep(1)

except (InterruptedError, KeyboardInterrupt) as e:
    kafka_producer.flush()
    kafka_producer.close()
    raise Exception(e)