from consumer_functions import consumer, handling_message, db_materialization, db_engine

topic = 'market_data'
engine = db_engine()

consumer = consumer(topic)

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue
    
    df = handling_message(msg)
    db_materialization('market_data_table', engine, df)