import requests
import sys
from kafka import KafkaProducer
from typing import Union

def create_producer(server:str = 'localhost', port: int = 9092):
    producer = KafkaProducer(
        bootstrap_servers=[f"{server}:{port}"]
    )
    return producer

def get_market_data(type_market: str, symbol: str, interval: Union[int, str, None] = None, rows: Union[int, str, None] = None):
    api = 'https://data-api.binance.vision'
    parameters = {
        'symbol': symbol,
        'interval': interval,
        'rows': rows
    }

    if type_market == 'avgPrice':
        endpoint = 'api/v3/avgPrice'
    else:
        print('Sin implementar. Adiós')
        sys.exit()
    
    response = requests.get(
            f'{api}/{endpoint}',
            params=parameters
        )
    return response

def clean_market_data(response: requests.Response, type_market: str):
    json_response = response.json()
    if type_market == 'avgPrice':
        json_response = {key: json_response[key] for key in ['price', 'closeTime']}
        return str(json_response).replace("\'", "\"")

def processing_market_data(type_market_data: str, symbol: str, interval: Union[int, str, None] = None, rows: Union[int, str, None] = None):
    response = get_market_data(type_market_data,symbol,interval,rows)
    return clean_market_data(response, type_market_data)
    