from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092", 
    client_id='topic_creator'
)

topic_list = []
topic_list.append(NewTopic(name="lines-of-files-topic", num_partitions=1, replication_factor=1))
admin_client.create_topics(new_topics=topic_list, validate_only=False)

print("Topic created successfully!")
'''
# Configuración del cliente de administración de Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",  # Dirección y puerto del broker
    client_id='topic_creator',
)


# Nombre del tópico
topic_name = "lines-of-files-topic"

# Crear el tópico
try:
    topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
    admin_client.create_topics(new_topics=[topic], validate_only=False)
    print(f"Tópico '{topic_name}' creado exitosamente.")
except Exception as e:
    print(f"Error al crear el tópico: {e}")
finally:
    admin_client.close()
'''