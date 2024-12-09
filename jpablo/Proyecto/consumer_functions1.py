import scipy as sc
import pandas as pd
import numpy as np
from datetime import datetime as dt, timezone
import requests
from sqlalchemy import create_engine, types
from json import loads
import json
from kafka import KafkaConsumer, TopicPartition

def db_engine(credentials_path="db_credentials.json"):
	
	with open(credentials_path) as f:
		credentials = json.load(f)

	user = credentials["user"]
	password = credentials["password"]
	host = credentials["host"]
	db = credentials["db"]

	connection_string = f'postgresql://{user}:{password}@{host}/{db}'
	engine = create_engine(connection_string)

	return engine
	  

def consumer(topic, host = 'localhost', port = 9092): # (offset_reset, host, port, partition, ...)
	bootstrap_servers = f"{host}:{port}"

	consumer = KafkaConsumer(
		topic,
		bootstrap_servers = [bootstrap_servers],
		value_deserializer = lambda x: loads(x.decode('utf-8')),
		auto_offset_reset = 'latest',
		enable_auto_commit = True,
		group_id = "my_consumer_group"
	)

	return consumer

	
def handling_message(dw_mk_msg):
	message_content = dw_mk_msg.value

	df = pd.DataFrame([message_content])
	return df


def db_materialization(table_name,engine, json_df):
    try:
        '''
        # Verificar si las columnas del DataFrame coinciden con las de la tabla
        with engine.connect() as conn:
            table_columns = conn.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
            """).fetchall()
            table_columns = [col[0] for col in table_columns]

        # Validar columnas del DataFrame
        missing_columns = set(json_df.columns) - set(table_columns)
        if missing_columns:
            raise ValueError(f"Las siguientes columnas están ausentes en la tabla '{table_name}': {missing_columns}")
		
        # Evitar duplicados basados en la columna 'timestamp'
        if "timestamp" in json_df.columns:
            with engine.connect() as conn:
                existing_timestamps = conn.execute(f"""
                    SELECT DISTINCT timestamp FROM {table_name};
                """).fetchall()
                existing_timestamps = {row[0] for row in existing_timestamps}
            
            # Filtrar filas que ya existen
            json_df = json_df[~json_df["timestamp"].isin(existing_timestamps)]
		'''
        # Insertar datos si hay filas nuevas
        if not json_df.empty:
            json_df.to_sql(table_name, con=engine, if_exists="append", index=False)
            print(f"Datos guardados exitosamente en la tabla '{table_name}'.")
        else:
            print(f"No hay datos nuevos para insertar en la tabla '{table_name}'.")

        return True

    except ValueError as ve:
        print(f"Error de validación: {ve}")
        return False
    except Exception as e:
        print(f"Error al guardar los datos en la tabla '{table_name}': {e}")
        return False