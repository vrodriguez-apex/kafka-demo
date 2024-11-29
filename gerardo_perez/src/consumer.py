from kafka import KafkaConsumer
import sys
import json
from datetime import datetime
import sqlite3

TOPIC = 'bitusdt-avg-topic'

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=['localhost:9092'],
    enable_auto_commit=False,
    group_id='my_group_id',
    value_deserializer=lambda x: x.decode('utf-8'),
    auto_offset_reset='earliest'
)

conn = sqlite3.connect(":memory:")
cur = conn.cursor()
cur.execute('CREATE TABLE bitusdt(id INTEGER PRIMARY KEY, closeTime TEXT, price REAL, mins TEXT)')

try:
    for message in consumer:
        message_topic = message.topic
        message_value = message.value
        json_value = json.loads(message_value)
        json_value['closeTime'] = datetime.fromtimestamp(json_value['closeTime']/1000).strftime('%Y-%m-%d %H:%M:%S')
        print(json_value)
        
        cur.execute('INSERT INTO bitusdt(closeTime, price, mins) VALUES (:closeTime, :price, :mins)', json_value)
        conn.commit()

except KeyboardInterrupt:
    print('Cerrando consumer...')
    cur.close()
    conn.close()
    sys.exit()


