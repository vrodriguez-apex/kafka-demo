import six
import sys
import json
import matplotlib.pyplot as plt
from kafka.consumer import KafkaConsumer
from kafka.errors import KafkaError
from collections import defaultdict

if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

KAFKA_BROKER = "localhost:9092"
TOPIC = "cryptocurrency"
GROUP_ID = "file-consumer-app"

# Configuración del consumidor
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    key_deserializer=lambda k: json.loads(k.decode('utf-8')) if k else None,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Diccionario para almacenar precios por criptomoneda
crypto_data = defaultdict(list)

# Función para graficar los datos
def graficar(crypto_data):
    plt.figure(figsize=(12, 6))
    for name, prices in crypto_data.items():
        plt.plot(prices, label=name)
    
    plt.title("Precios en Tiempo Real (Criptomonedas)")
    plt.xlabel("Iteración")
    plt.ylabel("Precio (USD)")
    plt.legend()
    plt.grid()
    plt.show()

# Procesar mensajes del consumidor
try:
    message_count = 0 
    for record in consumer:
        if record and record.value:
            data = record.value
            crypto_name = data.get("name")
            crypto_price = data.get("price")
            
            # Asegurarnos de que tenemos datos válidos
            if crypto_name and crypto_price:
                crypto_data[crypto_name].append(crypto_price)
                print(f"Criptomoneda: {crypto_name}, Precio: {crypto_price}")

                # Graficar cada 10 mensajes
                message_count += 1
                if message_count % 10 == 0:
                    graficar(crypto_data)
except KafkaError as e:
    print(f"Error consumiendo mensajes: {e}")
finally:
    consumer.close()
