from kafka import KafkaConsumer

# Configuración del consumidor de Kafka
consumer = KafkaConsumer(
    "lines-of-files-topic",  # Nombre del tópico
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # Leer desde el inicio
    enable_auto_commit=True,
    group_id='line-validator',
    value_deserializer=lambda x: x.decode('utf-8')  # Decodificar los mensajes
)

print("Consumiendo mensajes:")

# Leer y mostrar mensajes
try:
    for message in consumer:
        print(f"Offset: {message.offset}, Mensaje: {message.value}")
except Exception as e:
    print(f"Error al consumir mensajes: {e}")
finally:
    consumer.close()