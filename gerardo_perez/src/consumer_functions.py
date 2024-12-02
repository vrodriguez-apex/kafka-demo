from kafka import KafkaConsumer
import json
import sqlite3
from sqlite3.dbapi2 import Cursor
from datetime import datetime
from kafka.consumer.fetcher import ConsumerRecord

def handling_message(message: ConsumerRecord) -> str:
    message_value = message.value
    json_value = json.loads(message_value)
    json_value['closeTime'] = datetime.fromtimestamp(json_value['closeTime']/1000).strftime('%Y-%m-%d %H:%M:%S')
    return json_value

def db_engine(db_name: str = ':memory:'):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    return conn, cur

def create_db_table(name: str, cur: Cursor):
    cur.execute(f'CREATE TABLE IF NOT EXISTS {name}(id INTEGER PRIMARY KEY, closeTime TEXT, price REAL)')

def db_materialization(table, record, cur: Cursor):
    try:
        cur.execute(f'INSERT INTO {table}(closeTime, price) VALUES (:closeTime, :price)', record)
    except sqlite3.IntegrityError as e:
        print(f'Integrity error occurred: {e}')
    except sqlite3.OperationalError as e:
        print(f'Operational error occurred: {e}')
    except Exception as e:
        print(f'An unexpected error occurred: {e}')

def create_consumer(topic, server = 'localhost', port = 9092, offset_reset = 'earliest'):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[f'{server}:{port}'],
        enable_auto_commit=False,
        group_id='my_group_id',
        value_deserializer=lambda x: x.decode('utf-8'),
        auto_offset_reset=offset_reset
    )
    return consumer
