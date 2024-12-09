from dotenv import load_dotenv

load_dotenv()
import os
import json

import logging
import requests
import cv2
from io import BytesIO
from PIL import Image
import numpy as np

import mysql.connector
from kafka.consumer import KafkaConsumer
from confluent_kafka.serialization import StringDeserializer

logger = logging.getLogger()

KAFKA_HOST = os.environ.get("KAFKA_HOST")
KAFKA_TOPIC = os.environ.get("TOPIC")

DB_HOST = os.environ.get("DB_HOST")
DB_DATABASE = os.environ.get("DB_DATABASE")
DB_USER = os.environ.get("DB_USER")
DB_PSWD = os.environ.get("DB_PSWD")
DB_MAIN_TABLE = os.environ.get("DB_MAIN_TABLE")


def db_engine():
    """
    Create the connection between your db and python, this will provide a way to materialize data into
    your db directly using kafka and python.
    
    Kafka Streams could be reviewed in the next meetings, but nos 100% sure. Even though, you can explore
    by yourself their use.
    """
    mydb = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PSWD,
        database=DB_DATABASE
    )

    return mydb


def get_consumer():  # (offset_reset, host, port, partition, ...)

    """
    Create a consumer and assign the topic and a partition.
    """
    try:
        kafka_consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_HOST,
            key_deserializer=StringDeserializer(),
            value_deserializer=StringDeserializer(),
            auto_offset_reset="latest"
        )
        kafka_consumer.subscribe([KAFKA_TOPIC])
        logger.info("The Kafka producer was created successfully.")
        return kafka_consumer
    except Exception as ex:
        logger.exception(f"An error was found: {ex}")
    return None


def handling_message(kafka_consumer: KafkaConsumer):
    """
    You need to handle your message here from the producer, then pass out into the next function.

    The loop can be over the script and just call the function inside the loop.
    """
    for record in kafka_consumer:
        if record is None:
            continue
        logger.info(f"Key: {record.key}, Value: {record.value}")
        logger.info(f"Partition: {record.partition}, offset: {record.offset}")
        show_image(record.value)
        db_materialization(json.dumps(record.value))


def show_image(json_detail):
    try:
        response = requests.get(json.loads(json_detail).get("url"), verify=False)
        img = Image.open(BytesIO(response.content))

        img_np = np.array(img)
        img_resized = cv2.resize(img_np, (1280, 720))
        img_cv = cv2.cvtColor(img_resized, cv2.COLOR_RGB2BGR)

        cv2.imshow('Imagen', img_cv, )
        cv2.waitKey(4000)  # Mostrar la imagen por 2000 milisegundos (2 segundos)
        cv2.destroyAllWindows()
    except Exception as ex:
        logger.exception(f"The image could not be processed and showed: {ex}")


def db_materialization(json_obtained):
    """
    Here you need to materialize the message into your db, I recommend do so with error handling, as
    you want to maintain your kafka consumer isolated from errors in non-kafka components.

    This means that you could easily continue receiving messages even when the code goes wrong.

    Also, is helpful to do so, as in a 24/7 running system you could go into to many different issues
    and you may need to know every single one of them to not just "try and except", but do the right
    corrections	to your code and register the errors that you could have.
    """
    try:
        mydb = db_engine()
        my_cursor = mydb.cursor()
        sql = f"INSERT INTO {DB_MAIN_TABLE}(data_obtained) VALUES (%s)"
        val = (json_obtained,)

        my_cursor.execute(sql, val)

        mydb.commit()
    except Exception as ex:
        logger.exception(f"The json information could not be processed inside our database. Error: {ex}")


if __name__ == "__main__":
    consumer = get_consumer()
    if consumer:
        while True:
            handling_message(consumer)
    else:
        logger.error("The consumer could not be created. The process has ended.")
