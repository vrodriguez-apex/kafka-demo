import time
import requests
import sys
from kafka import KafkaProducer

TOPIC = 'bitusdt-avg-topic'
API = 'https://data-api.binance.vision'
ENDPOINT = 'api/v3/avgPrice'
parameters = {
    'symbol': 'BTCUSDT'
}

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"]
)

try:
    while True:
        response = requests.get(
            f'{API}/{ENDPOINT}',
            params=parameters
        )
        producer.send(TOPIC, value=response.content)
        print(response.json())
        time.sleep(30)
except KeyboardInterrupt:
    print('Cerrando producer...')
    sys.exit()