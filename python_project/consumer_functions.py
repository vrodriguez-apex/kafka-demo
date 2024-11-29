import scipy as sc
import pandas as pd
import numpy as np
from datetime import datetime as dt, timezone
import requests
from sqlalchemy import create_engine, types
from json import loads
from kafka import KafkaConsumer, TopicPartition

def db_engine(user,password,host,db):
    
    """
    Create the connection between your db and python, this will provide a way to materialize data into
    your db directly using kafka and python.
    
    Kafka Streams could be reviewed in the next meetings, but nos 100% sure. Even though, you can explore
    by yourself their use.
    """

def consumer(topic): # (offset_reset, host, port, partition, ...)
    
    """
    Create a consumer and assign the topic and a partition.
    """
    
def handling_message(dw_mk_msg):
	
	"""
	You need to handle your message here from the producer, then pass out into the next function.

	The loop can be over the script and just call the function inside the loop.
	"""

def db_materialization(table_name,engine, json_df):
	
	"""
	Here you need to materialize the message into your db, I recommend do so with error handling, as
	you want to maintain your kafka consumer isolated from errors in non-kafka components.

	This means that you could easily continue receiving messages even when the code goes wrong.

	Also, is helpful to do so, as in a 24/7 running system you could go into to many different issues
	and you may need to know every single one of them to not just "try and except", but do the right
	corrections	to your code and register the errors that you could have.
	"""