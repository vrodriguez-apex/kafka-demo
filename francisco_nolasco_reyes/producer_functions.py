from dotenv import load_dotenv

load_dotenv()
import os
import logging

from api_cat_model import Cat
from dataclasses import asdict

import json
from time import sleep

import kafka.errors
from kafka.producer import KafkaProducer
from confluent_kafka.serialization import StringSerializer
from kafka.admin import KafkaAdminClient, NewTopic

import requests
from http import HTTPStatus

CAT_API_URL = os.environ.get("CAT_API")
CAT_API_KEY = os.environ.get("API_KEY")
KAFKA_HOST = os.environ.get("KAFKA_HOST")
KAFKA_TOPIC = os.environ.get("TOPIC")
HAS_BREEDING = 1

admin_client = KafkaAdminClient(
    bootstrap_servers=KAFKA_HOST
)

logger = logging.getLogger()


def create_producer():
    """
    Create the producer with the right config for your use case.
    """
    try:
        logger.info(f"Creating producer...")
        kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_HOST,
                                       key_serializer=StringSerializer(),
                                       value_serializer=StringSerializer())
        logger.info(f"The producer was created successfully")
        return kafka_producer
    except Exception as ex:
        logger.exception(f"An exception occurred while creating producer: {ex}")
        return None


def get_market_data(has_breeds: int = 0):
    """
    Connect to an exchange like OKX, Binance, ByBit, etc...
    Stream market data.
    """
    try:
        response = requests.get(url=f"{CAT_API_URL}?has_breeds={has_breeds}", headers={'x-api-key': CAT_API_KEY},
                                verify=False)
        if response.status_code == HTTPStatus.OK.value:
            cat_information = response.json()
            if cat_information:
                return cat_information[0]
        else:
            logger.error(f"Error while requesting API CAT")
    except Exception as err:
        logger.error(f"An exception was found: {err}")
    return None


def clean_market_data(cat_information):
    """
    You need to be sure that you can process the data you are receiving.
    """
    cat_dict = None
    if cat_information:
        cat_object = Cat(
            id=cat_information.get("id", ""),
            breeds=cat_information.get("breeds", []),
            url=cat_information.get("url", ""),
            width=cat_information.get("width", ""),
            height=cat_information.get("height", ""),
        )
        cat_dict = asdict(cat_object)
    return cat_dict


def processing_market_data():
    """
    Processing market data and know the schema that you are receiving.
    You can return either a dataframe, a dict or both, depending on what you want to do.
    You can use get_market_data() and clean_market_data() inside here.
    """
    data = get_market_data(HAS_BREEDING)
    data = clean_market_data(data)
    return data


def transformations():
    """
    Here you will need to do some transformation into your messages before materialize it into the db.
    I'm referring to operations over your numbers, not managing schemas or something else; it could be means,
    averages, and other operations over the prices, volumes, etc.
    """
    cat_information = processing_market_data()

    return cat_information


if __name__ == "__main__":
    try:
        admin_client.create_topics(new_topics=[NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)],
                                   validate_only=False)
    except kafka.errors.TopicAlreadyExistsError as e:
        pass

    producer = create_producer()

    if producer:
        while True:
            try:
                api_data = transformations()
                if api_data:
                    logger.info(f"Message to sent: {json.dumps(api_data)}")
                    producer.send(topic=KAFKA_TOPIC, key="cat-info'", value=json.dumps(api_data))
                    sleep(5)
                else:
                    logger.error(f"The API CAT Information could not be processed")
                    sleep(2)
            except (InterruptedError, KeyboardInterrupt) as e:
                producer.flush()
                producer.close()
                raise Exception(e)
            except Exception as e:
                logger.exception(f"An exception was found: {e}")
    else:
        logger.error("The producer could not be created. The process has ended.")
