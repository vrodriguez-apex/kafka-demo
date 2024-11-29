
import scipy as sc
import pandas as pd
import numpy as np
from datetime import datetime as dt, timezone, timedelta
import requests
from kafka import KafkaProducer # type: ignore
from json import dumps




#################################################################
# Funciones objetivo:
#   Es lo mínimo que debes hacer para tener tu producer funcionando.
#################################################################
def create_producer():
    
    """
    Create the producer with the right config for your use case.
    """

def get_market_data(type_market_data,symbol,interval,rows):
    
    """
    Connect to an exchange like OKX, Binance, ByBit, etc...
    Stream market data.
    """

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

def transformations(data,smooth_interval,smooth_exp):
	
	"""
	Here you will need to do some transformation into your messages before materialize it into the db.
	I'm referring to operations over your numbers, not managing schemas or something else; it could be means,
	averages, and other operations over the prices, volumes, etc.
	"""