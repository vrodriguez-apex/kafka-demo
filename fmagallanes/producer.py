import six
import sys
from time import sleep
import kafka.errors
from kafka.admin import KafkaAdminClient, NewTopic
from requests import Request,Session
from requests.exceptions import ConnectionError,Timeout,TooManyRedirects 
from kafka.producer import KafkaProducer
from confluent_kafka.serialization import IntegerSerializer, StringSerializer
import json

#URL de la API de coinmarketcap
url = 'https://Pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'

#Parametros para la extraccion de data
parameters={
'start':'1',
'limit':'5',
'convert':'USD'
}
#Headers para la API 
headers ={
    'Accepts': 'application/json',
    'X-CMC_Pro_API_KEY': '<API-KEY>',
    'id': '1',
}

#Nombre del Topic
topic = "lines-of-files-topic-web"

#Creando llamada a la API
session = Session()
session.headers.update(headers)

#Caracteristicas del Topic
try:
    topic_list = []
    topic_list.append(NewTopic(name=topic, num_partitions=3, replication_factor=1))
   

except kafka.errors.TopicAlreadyExistsError as e:
    pass

kafka_producer = KafkaProducer(
    key_serializer = StringSerializer(),
    value_serializer = StringSerializer()
)

#enviando mensajes
try:
    response = session.get(url, params=parameters)
    data = json.loads(response.text)
    for item in data['data']:
        line = json.dumps(data, sort_keys=True, indent=2) #Indenta el Json para que se vea bien. 
        kafka_producer.send(topic=topic, value=line) #Creación del mensaje.
        #print(data)
        print(line)
    

except (ConnectionError, Timeout, TooManyRedirects) as e:
  print(e)

except (InterruptedError, KeyboardInterrupt) as e:
    kafka_producer.flush()
    kafka_producer.close()
    raise Exception(e)