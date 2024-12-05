import sys
from src.consumer_functions import create_consumer, db_engine, create_db_table, handling_message, db_materialization

topic = 'BTCUSDT_avgPrice'

consumer = create_consumer(topic, offset_reset='latest')
conn, cur = db_engine('db.sqlite3')
create_db_table(topic, cur)
conn.commit()

try:
    for message in consumer:
        json_value = handling_message(message)
        print(json_value)
        db_materialization(topic, json_value, cur)
        conn.commit()

except KeyboardInterrupt:
    print('Cerrando consumer...')
    cur.close()
    conn.close()
    sys.exit()

