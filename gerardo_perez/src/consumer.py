import sys
from src.consumer_functions import create_consumer, db_engine, create_db_table, handling_message, db_materialization

topic = 'BTCUSDT_avgPrice'

consumer = create_consumer(topic)
conn, cur = db_engine()
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
    print(cur.execute(f'select * from {topic}').fetchall())
    cur.close()
    conn.close()
    sys.exit()

