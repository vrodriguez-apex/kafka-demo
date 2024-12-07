from kafka import KafkaConsumer

# Configuración del consumidor
consumer = KafkaConsumer(
    "rand-int-topic",
    bootstrap_servers="localhost:9092",
    group_id="rand-int-consumer-group",
    key_deserializer=lambda k: k.decode('utf-8'),
    value_deserializer=lambda v: int(v.decode('utf-8')),
    auto_offset_reset="earliest"
)

try:
    for message in consumer:
        partition = message.partition
        key = message.key
        value = message.value
        print(f"Mensaje consumido -> Partition: {partition}, Key: {key}, Value: {value}")
except Exception as e:
    print(f"Error al consumir mensajes: {e}")
finally:
    consumer.close()