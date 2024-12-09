import six
import sys
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves
from time import sleep
import kafka.errors
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.producer import KafkaProducer
from confluent_kafka.serialization import IntegerSerializer, StringSerializer
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
 
url = 'https://Pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
parameters = {
 
  'start':'1',
  'limit':'20',
  'convert':'USD'
}
headers = {
  'Accepts': 'application/json',
  'X-CMC_PRO_API_KEY': '<API-KEY>',
}
 
topic = "kafka-coinmarketcap_caib" 
session = Session()
session.headers.update(headers)
 
try:
    topic_list = []
    topic_list.append(NewTopic(name=topic, num_partitions=3, replication_factor=1))
   
except kafka.errors.TopicAlreadyExistsError as e:
    pass

kafka_producer = KafkaProducer(
    key_serializer = StringSerializer(),
    value_serializer = StringSerializer()
)

try:
    response = session.get(url,params=parameters)
    data=json.loads(response.text)
  
    for item in data['data']:
        line = json.dumps(data,sort_keys=True,indent=2)  
        kafka_producer.send(topic=topic,value=line)
        print(data)
        print(line)
        sleep(1)
except (InterruptedError, KeyboardInterrupt) as e:
    kafka_producer.flush()
    kafka_producer.close()
    raise Exception(e)  