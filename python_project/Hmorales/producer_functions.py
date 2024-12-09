#import scipy as sc
import six
import sys
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

import json
import pandas as pd
import numpy as np
from datetime import datetime as dt, timezone, timedelta
import requests
import datetime
import yfinance as yf
from json import dumps
import kafka.errors
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.producer import KafkaProducer
from confluent_kafka.serialization import IntegerSerializer, StringSerializer
import mysql.connector


#################################################################
# Funciones objetivo:
#   Es lo mínimo que debes hacer para tener tu producer funcionando.
#################################################################
def create_producer():
    
    """
    Create the producer with the right config for your use case.
    """
    host = "localhost:9092"
    topic='dw.market.data'

    admin_client = KafkaAdminClient(
        bootstrap_servers=host
    )

    #una pratición por cada asset
    try:
        topic_list = []
        topic_list.append(NewTopic(name=topic, num_partitions=5, replication_factor=1))
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except kafka.errors.TopicAlreadyExistsError as e:
        pass

    kafka_producer = KafkaProducer(
        bootstrap_servers = host,
        key_serializer = StringSerializer(),
        value_serializer = StringSerializer()
    )
    
    return kafka_producer

def get_market_data(type_market_data,symbol,interval,rows,startdate):
    
    """
    Connect to an exchange like OKX, Binance, ByBit, etc...
    Stream market data.
    """
    enddate=datetime.datetime.now()

    dfasset = yf.download(symbol, start=startdate, end=enddate,interval=interval)
    dfasset.reset_index(inplace=True)
    dfasset.columns=['tdate','tadj_close','tclose','thigh','tlow','topen','tvolume']
    dfasset['name']=symbol

    return dfasset



def clean_market_data():
    
    """
    You need to be sure that you will can process the data you are receiving.
    """

def processing_market_data(type_market_data,symbol,interval,rows):
    """
	Processing market data and know the schema that you are receiving.
	You can return either a dataframe, a dict or both, depending on what you want to do.
    You can use get_market_data() and clean_market_data() inside here.
	"""
 	
    lstTickers = ['XRP-USD','BTC-USD','DOGE-USD','ETH-USD','MANA-USD']

    with open('secrets\\dbcredentials.json') as jsonfile:
        dbcredentials = json.load(jsonfile)
    
    #mydb = mysql.connector.connect(
    #    host=dbcredentials['host'],
    #    database=dbcredentials['database'],
    #    user=dbcredentials['dbuser'],
    #    password=dbcredentials['passd'],
    #    )
    
    #mycursor = mydb.cursor()

    sql = f'Select max(tdate) as startdate from assetshist '
    
    #mycursor.execute(sql)
                    
    #result = mycursor.fetchone()
    result =['2024-12-04']  #solo paraue no marque error
    if result[0] == None:
        startdate='2008-01-01'
    else:
        startdate=result[0]
    
    dfasset = get_market_data(type_market_data='',symbol=symbol,interval='1h',rows=0,startdate=startdate)

    return dfasset

    


def transformations(data,smooth_interval,smooth_exp):
    """
	Here you will need to do some transformation into your messages before materialize it into the db.
	I'm referring to operations over your numbers, not managing schemas or something else; it could be means,
	averages, and other operations over the prices, volumes, etc.
	"""
    print ('do some transformations')

    return data