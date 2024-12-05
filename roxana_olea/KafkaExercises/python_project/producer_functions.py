from confluent_kafka import Producer
import pandas as pd
import requests
from json import dumps

def create_producer():
    producer = Producer({
        'bootstrap.servers': 'localhost:9092',
        'security.protocol': 'PLAINTEXT'
    })
    return producer

def get_market_data(type_market_data, symbol, interval, rows):
    url = f'https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={rows}'
    response = requests.get(url)
    data = response.json()
    return data

def clean_market_data(data):
    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

def processing_market_data(type_market_data, symbol, interval, rows):
    data = get_market_data(type_market_data, symbol, interval, rows)
    clean_data = clean_market_data(data)
    return clean_data

def transformations(data, smooth_interval, smooth_exp):
    data['ema'] = data['close'].ewm(span=smooth_interval, adjust=False).mean()
    data['timestamp'] = data['timestamp'].astype(str)
    return data