import random
from kafka import KafkaProducer

# Configuración del productor
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k: k.encode('utf-8'),  # Serializa la clave como string
    value_serializer=lambda v: str(v).encode('utf-8')  # Serializa el valor como string
)

topic_name = "rand-int-topic"

try:
    for _ in range(10):  # Producir 10 números aleatorios
        number = random.randint(0, 1000)
        key = "even" if number % 2 == 0 else "odd"
        producer.send(topic_name, key=key, value=number)
        print(f"Mensaje enviado -> Key: {key}, Value: {number}")
except Exception as e:
    print(f"Error al producir mensajes: {e}")
finally:
    producer.close()
