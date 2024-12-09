#import scipy as sc
import pandas as pd
import numpy as np
from datetime import datetime as dt, timezone
import requests
from sqlalchemy import create_engine, types
import  json 
from kafka import KafkaConsumer, TopicPartition
from confluent_kafka.serialization import IntegerDeserializer, StringDeserializer
import mysql.connector

def db_engine():
	"""
    Create the connection between your db and python, this will provide a way to materialize data into
    your db directly using kafka and python.
    
    Kafka Streams could be reviewed in the next meetings, but nos 100% sure. Even though, you can explore
    by yourself their use.
    """
	with open('secrets\\dbcredentials.json') as jsonfile:
		dbcredentials = json.load(jsonfile)

	mydb = mysql.connector.connect(
  		host=dbcredentials['host'],
  		database=dbcredentials['database'],
  		user=dbcredentials['dbuser'],
  		password=dbcredentials['passd'],
	)
	table_name = 'assethist'
		
	mycursor = mydb.cursor()

	return mycursor
    

def consumer(topic): # (offset_reset, host, port, partition)
	"""
    Create a consumer and assign the topic and a partition.
    """
	host = "localhost:9092"
	group_id = "dw-market-data-consumer-app"

	kafka_consumer = KafkaConsumer(
    	bootstrap_servers = host,
    	key_deserializer=StringDeserializer(),
    	value_deserializer=StringDeserializer(),
    	group_id=group_id,
    	auto_offset_reset="earliest",
	)

	kafka_consumer.subscribe([topic])

	return kafka_consumer
    
def handling_message(dw_mk_msg):
	
	"""
	You need to handle your message here from the producer, then pass out into the next function.

	The loop can be over the script and just call the function inside the loop.
	"""
	print (f'dw_mk_msg {dw_mk_msg.value()}')

def db_materialization(engine, json_df, mydb):
	"""
	Here you need to materialize the message into your db, I recommend do so with error handling, as
	you want to maintain your kafka consumer isolated from errors in non-kafka components.

	This means that you could easily continue receiving messages even when the code goes wrong.

	Also, is helpful to do so, as in a 24/7 running system you could go into to many different issues
	and you may need to know every single one of them to not just "try and except", but do the right
	corrections	to your code and register the errors that you could have.
	"""

	dfasset = pd.read_json(json_df)
	#table_name = dfasset['name'][0,table_name.find('-') - 1].join('data')
	sql_insert =f''' Insert into btcdata (name,tdate,tadj_close,tclose,thigh,tlow,topen,tvolume)
	#values (%s,%s,%s,%s,%s,%s,%s,%s);
	#'''
	data_tuples=[(row['name']
                ,row['tdate'].date()
                ,row['tadj_close']
                ,row['tclose']
                ,row['thigh']
                ,row['tlow']
                ,row['topen']
                ,row['tvolume'])
                for _,row in dfasset.iterrows()]
	#print(f'size of tuples {len(data_tuples[0])}')
	print(f' insert query:{sql_insert}')
	#print(f'tuples value 1: {data_tuples[0]}')
	#print(f'tuples value 1: {data_tuples[0][0]}')
	#print(f'tuples value 8: {data_tuples[0][7]}')

  	#start to save into a mysql table
  	
	for x in range(0,len(data_tuples)):
		batch = data_tuples[x]
		print(f'size of tuples {len(batch[0])}')
		print(f'tuples value 1: {batch[0][0]}')
		engine.executemany(sql_insert,batch)
		#mydb.commit()

