import sys
from src.consumer_functions import create_consumer, db_engine, create_db_table, handling_message, db_materialization

topic = 'bitusdt-avg-topic'

consumer = create_consumer(topic)
conn, cur = db_engine()
create_db_table('bitusdt', cur)
conn.commit()

try:
    for message in consumer:
        json_value = handling_message(message)
        print(json_value)
        db_materialization(json_value, cur)
        conn.commit()

except KeyboardInterrupt:
    print('Cerrando consumer...')
    print(cur.execute('select * from bitusdt').fetchall())
    cur.close()
    conn.close()
    sys.exit()

