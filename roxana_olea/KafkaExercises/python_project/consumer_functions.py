from confluent_kafka import Consumer
import pandas as pd
from sqlalchemy import create_engine
from json import loads

def db_engine():
    engine = create_engine('sqlite:///market_data.db')
    return engine

def consumer(topic):
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my-group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([topic])
    return consumer

def handling_message(dw_mk_msg):
    data = loads(dw_mk_msg.value().decode('utf-8'))
    df = pd.DataFrame([data])
    return df

def db_materialization(table_name, engine, json_df):
    try:
        json_df.to_sql(table_name, engine, if_exists='append', index=False)
    except Exception as e:
        print(f"Wrong data materialization: {e}")