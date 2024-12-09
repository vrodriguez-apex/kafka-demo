from kafka.admin import KafkaAdminClient, NewTopic

# Configuración del cliente de administración de Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id="create_rand_int_topic"
)

# Nombre del tópico
topic_name = "rand-int-topic"

# Crear el tópico
try:
    topic = NewTopic(name=topic_name, num_partitions=2, replication_factor=1)
    admin_client.create_topics(new_topics=[topic], validate_only=False)
    print(f"Tópico '{topic_name}' creado exitosamente.")
except Exception as e:
    print(f"Error al crear el tópico: {e}")
finally:
    admin_client.close()
