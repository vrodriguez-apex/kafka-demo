import json
import threading
from producer_functions1 import create_producer, processing_market_data
from consumer_functions1 import db_engine, consumer, db_materialization, handling_message
import time 

# ============================
# Función principal
# ============================

def producer_flow(interval=1, symbol="BTCUSDT", topic="market_data", engine=None, table_name="market_data"):
    """
    Flujo del productor: obtiene datos del mercado, los procesa y los envía al tópico Kafka.
    """
    try:
        producer = create_producer()
        if not producer:
            print("No se pudo inicializar el productor Kafka. Terminando el flujo del productor.")
            return

        while True:
            # Obtener y procesar los datos de mercado
            processed_data = processing_market_data(symbol=symbol)
            if processed_data is None:
                print("Error al procesar los datos del mercado. Reintentando en el siguiente intervalo...")
                time.sleep(interval)
                continue

            # Convertir datos procesados a diccionario y enviar al tópico
            message = processed_data.to_dict(orient="records")[0]
            producer.send(topic, value=message)
            print(f"Datos enviados al tópico '{topic}': {message}")

            # Guardar en la base de datos si se proporciona un engine
            if engine:
                db_materialization(table_name, engine, processed_data)

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nProductor detenido manualmente.")
    except Exception as e:
        print(f"Error en el flujo del productor: {e}")


def consumer_flow(topic="market_data", engine=None, table_name="consumer_data"):
    """
    Flujo del consumidor: consume datos del tópico Kafka y los guarda en la base de datos.
    """
    try:
        kafka_consumer = consumer(topic=topic)
        if not kafka_consumer:
            print("No se pudo inicializar el consumidor Kafka. Terminando el flujo del consumidor.")
            return

        while True:
            for message in kafka_consumer:
                print(f"Mensaje recibido del tópico '{topic}': {message.value}")
                
                # Manejar el mensaje y convertirlo a DataFrame
                handled_data = handling_message(message)
                if handled_data is not None and engine:
                    db_materialization(table_name, engine, handled_data)

    except KeyboardInterrupt:
        print("\nConsumidor detenido manualmente.")
    except Exception as e:
        print(f"Error en el flujo del consumidor: {e}")


# ============================
# Iniciar flujos
# ============================

if __name__ == "__main__":
    try:
        # Cargar configuración de la base de datos
        with open("db_credentials.json") as f:
            db_config = json.load(f)

        # Inicializar conexión a la base de datos
        engine = db_engine(credentials_path="db_credentials.json")
        print("Conexión a la base de datos establecida.")

        # Crear hilos para productor y consumidor
        producer_thread = threading.Thread(
            target=producer_flow,
            args=(5, "BTCUSDT", "market_data", engine, "market_data")
        )
        consumer_thread = threading.Thread(
            target=consumer_flow,
            args=("market_data", engine, "consumer_data")
        )

        # Iniciar los hilos
        producer_thread.start()
        consumer_thread.start()

        # Esperar a que los hilos terminen
        producer_thread.join()
        consumer_thread.join()

    except KeyboardInterrupt:
        print("\nPrograma detenido manualmente.")
    except Exception as e:
        print(f"Error inesperado en el programa principal: {e}")