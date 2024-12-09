from consumer_functions import consumer, handling_message, db_materialization, db_materialization_transformed, db_engine
from project_functions import strategy
from producer_functions import transformations

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
    
    transformed_df = transformations(df, 10, 0.1)
    strategy_df = strategy(transformed_df)
    
    db_materialization_transformed('transformed_data_table', engine, transformed_df)
    db_materialization_transformed('strategy_data_table', engine, strategy_df)