import time
import sys
from src.producer_functions import create_producer, processing_market_data

type_market_data = 'avgPrice'
symbol = 'BTCUSDT'
interval = 10
topic = f'{symbol}_{type_market_data}'
producer = create_producer()

try:
    while True:
        response = processing_market_data(type_market_data, symbol)
        producer.send(topic, value=str(response).encode("utf-8"))
        print(response)
        time.sleep(interval)
except KeyboardInterrupt:
    print('Cerrando producer...')
    sys.exit()