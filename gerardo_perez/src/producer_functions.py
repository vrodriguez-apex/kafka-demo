import requests
from kafka import KafkaProducer

def create_producer(server = 'localhost', port = 9092):
    producer = KafkaProducer(
        bootstrap_servers=[f"{server}:{port}"]
    )
    return producer

def get_market_data(api, endpoint, parameters):
    response = requests.get(
            f'{api}/{endpoint}',
            params=parameters
        )
    return response

