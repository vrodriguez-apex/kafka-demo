import faust
import json
import numpy as np
import sqlite3	
import sys
from sqlite3.dbapi2 import Cursor
from datetime import datetime

from consumer_functions import db_engine, create_db_table

def db_materialization(table, record, cur: Cursor):
    try:
        cur.execute(f'INSERT INTO {table}(closeTime, price) VALUES (:closeTime, :price)', record)
    except sqlite3.IntegrityError as e:
        print(f'Integrity error occurred: {e}')
    except sqlite3.OperationalError as e:
        print(f'Operational error occurred: {e}')
    except Exception as e:
        print(f'An unexpected error occurred: {e}')

topic_name = 'BTCUSDT_avgPrice'

conn, cur = db_engine()
create_db_table('agg_table', cur)
conn.commit()

class CurrencyDataAvg(faust.Record, serializer='json'):
    price: str
    closeTime: str

app = faust.App('demo-agg-btc', broker='kafka://localhost:9092')
schema = faust.Schema(value_serializer='json')

topic = app.topic(topic_name, schema=schema, partitions=1)

avg_price = app.Table("BTCUSDT_avgPrice_min5", default=int, partitions=1).tumbling(6)

try:
    @app.agent(topic)
    async def processor(stream):
        
        async for message in stream:
            json_value = json.loads(str(message).replace("\'", "\""))
            json_value['closeTime'] = datetime.fromtimestamp(json_value['closeTime']/1000).strftime('%Y-%m-%d %H:%M:%S')
            avg_price['close_time'] = json_value['closeTime']
            avg_price['qty'] += 1
            avg_price['price_sum'] += float(json_value['price'])
            avg_price['avg'] = np.divide(float(avg_price['price_sum'].value()), avg_price['qty'].value())
            # if avg_price['qty'].value() == 3:
            print("Average Price: ", avg_price['avg'].value())
            string = '{"closeTime": ' + '"' + str(avg_price["close_time"].value()) + '"' + ', "price":' + str(avg_price["avg"].value()) + '}'
            print(string)
            db_materialization('agg_table', json.loads(string), cur)
            conn.commit()
            print(cur.execute(f'select * from agg_table').fetchall())

except KeyboardInterrupt:
    print('Cerrando consumer...')
    print(cur.execute(f'select * from agg_table').fetchall())
    cur.close()
    conn.close()
    sys.exit()
