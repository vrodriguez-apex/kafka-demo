import time
import sys
from src.producer_functions import create_producer, get_market_data

topic = 'bitusdt-avg-topic'
api = 'https://data-api.binance.vision'
endpoint = 'api/v3/avgPrice'
parameters = {
    'symbol': 'BTCUSDT'
}

producer = create_producer()

try:
    while True:
        response = get_market_data(api, endpoint, parameters)
        producer.send(topic, value=response.content)
        print(response.json())
        time.sleep(30)
except KeyboardInterrupt:
    print('Cerrando producer...')
    sys.exit()