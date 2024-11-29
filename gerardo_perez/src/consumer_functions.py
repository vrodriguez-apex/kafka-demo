from kafka import KafkaConsumer
import sys
import json
import sqlite3
from sqlite3.dbapi2 import Cursor, Connection
from datetime import datetime
from kafka.consumer.fetcher import ConsumerRecord

TOPIC = 'bitusdt-avg-topic'

def handling_message(message: ConsumerRecord):
    message_value = message.value
    json_value = json.loads(message_value)
    json_value['closeTime'] = datetime.fromtimestamp(json_value['closeTime']/1000).strftime('%Y-%m-%d %H:%M:%S')
    return json_value

def db_engine(db_name: str = ':memory:'):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    return conn, cur

def create_db_table(name: str, cur: Cursor):
    cur.execute(f'CREATE TABLE IF NOT EXISTS {name}(id INTEGER PRIMARY KEY, closeTime TEXT, price REAL, mins TEXT)')

def db_materialization(record, cur: Cursor):
    cur.execute('INSERT INTO bitusdt(closeTime, price, mins) VALUES (:closeTime, :price, :mins)', record)

def create_consumer(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        enable_auto_commit=False,
        group_id='my_group_id',
        value_deserializer=lambda x: x.decode('utf-8'),
        auto_offset_reset='earliest'
    )
    return consumer
