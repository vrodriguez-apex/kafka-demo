import faust
import json
import numpy as np
import sys
from datetime import datetime
from src.consumer_functions import db_engine, create_db_table, db_materialization

topic_name = 'BTCUSDT_avgPrice'

conn, cur = db_engine('db.sqlite3')
create_db_table('agg_table', cur)
conn.commit()

class CurrencyDataAvg(faust.Record, serializer='json'):
    price: str
    closeTime: str

app = faust.App('demo-agg-btc', broker='kafka://localhost:9092')
schema = faust.Schema(value_serializer='json')

topic = app.topic(topic_name, schema=schema, partitions=1)

avg_price = app.Table("BTCUSDT_avgPrice", default=int, partitions=1).tumbling(60)

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
            print('Current Price:', float(json_value['price']))
            if avg_price['qty'].value() == 5:
                print("Last Minute Average Price: ", avg_price['avg'].value())
                string = '{"closeTime": ' + '"' + str(avg_price["close_time"].value()) + '"' + ', "price":' + str(avg_price["avg"].value()) + '}'
                db_materialization('agg_table', json.loads(string), cur)
                conn.commit()

except KeyboardInterrupt:
    print('Cerrando consumer...')
    print(cur.execute(f'select * from agg_table').fetchall())
    cur.close()
    conn.close()
    sys.exit()
