from kafka import KafkaProducer

# Configuración del productor de Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Nombre del tópico
topic_name = "lines-of-files-topic"

# Leer el archivo línea por línea y enviarlo al tópico
try:
    with open("la_biblioteca_de_babel.txt", "r", encoding="utf-8") as file:
        for line in file:
            # Eliminar espacios en blanco innecesarios y enviar al tópico
            producer.send(topic_name, line.strip().encode('utf-8'))
    print("Mensajes enviados exitosamente.")
except Exception as e:
    print(f"Error al enviar mensajes: {e}")
finally:
    producer.close()