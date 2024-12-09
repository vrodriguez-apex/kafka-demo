from producer_functions import create_producer, processing_market_data, transformations
from json import dumps
from time import sleep

type_market_data = 'kline'
symbol = 'BTCUSDT'
interval = '1m'
rows = 100

producer = create_producer()
topic = 'market_data'

while True:
    recent_data = processing_market_data(type_market_data, symbol, interval, rows)
    transformed_data = transformations(recent_data, 10, 0.1)
    
    for index, row in transformed_data.iterrows():
        producer.produce(topic, value=dumps(row.to_dict()))
        producer.flush()
    
    sleep(60)