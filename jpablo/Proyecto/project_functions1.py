#################################################################
# Funciones extras:
#   Puedes acceder a los puntos de estas funciones después de completar
#   las funciones objetivo.
#################################################################

from sklearn.linear_model import LinearRegression
import pandas as pd
import numpy as np
from consumer_functions1 import db_engine

def ml_model(data):
    """
    Modelo de regresión lineal para predecir el precio de cierre.
    
    Args:
        data (DataFrame): Un DataFrame con las columnas necesarias para entrenar el modelo.

    Returns:
        dict: Contiene el precio real, el precio predicho y el error.
    """
    # Verificar si las columnas necesarias están en el DataFrame
    required_columns = ['open', 'high', 'low', 'volume', 'sma', 'ema', 'close']
    for col in required_columns:
        if col not in data.columns:
            raise ValueError(f"Falta la columna requerida: {col}")

    # Dividir los datos en características (X) y objetivo (y)
    X = data[['open', 'high', 'low', 'volume', 'sma', 'ema']].values
    y = data['close'].values

    if X.shape[0] <= 1:
        raise ValueError("Faltan datos")
    
    # Entrenar el modelo con datos históricos (todas las filas excepto la última)
    model = LinearRegression()
    model.fit(X[:-1], y[:-1])

    # Predecir el precio de cierre para la última fila
    predicted_close = model.predict([X[-1]])
    actual_close = y[-1]
    error = abs(actual_close - predicted_close[0])

    # Resultado
    result = {
        "actual_close": actual_close,
        "predicted_close": predicted_close[0],
        "error": error
    }
    print(f"Modelo de ML: {result}")
    return result

def register_ml_results(result, engine, table_name="ml_results"):
    """
    Registra los resultados del modelo de Machine Learning en una tabla de la base de datos.
    """
    try:
        result_df = pd.DataFrame([result])  # Convertir resultado a DataFrame
        result_df.to_sql(table_name, con=engine, if_exists="append", index=False)
        print(f"Resultado del modelo registrado en la tabla '{table_name}': {result}")
    except Exception as e:
        print(f"Error al registrar el resultado del modelo: {e}")


from project_functions1 import ml_model, register_ml_results

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

def real_time_plot(data):
    """
    Visualización en tiempo real de los precios de cierre y los indicadores.
    
    Args:
        data (DataFrame): Un DataFrame con las columnas necesarias para la gráfica.
    """
    fig, ax = plt.subplots(figsize=(10, 6))
    plt.style.use("ggplot")

    def update(frame):
        engine = db_engine()
        data = pd.read_sql("SELECT * FROM consumer_data ORDER BY timestamp DESC LIMIT 100;", engine)
        if data.empty:
            return
        
        ax.clear()
        ax.plot(data['timestamp'], data['close'], label='Close Price', color='blue', marker="o")
        ax.plot(data['timestamp'], data['sma'], label='SMA', color='orange')
        ax.plot(data['timestamp'], data['ema'], label='EMA', color='green')
        ax.set_title("Precios en tiempo real", fontsize=16)
        ax.set_xlabel("Timestamp")
        ax.set_ylabel("Precio")
        ax.legend()
        plt.xticks(rotation=45)

    ani = FuncAnimation(fig, update, interval=1000, cache_frame_data=False)  # Actualiza cada 1 segundo
    plt.show()



#################################################################
# Servicios:
#   Puedes elegir uno de estos servicios o varios, cada uno de ellos tendrá la misma cantidad de puntos, puesto que el objetivo no es que
#   hagas un LLM que compita con chatgpt, o que codifiques gráficas super lindas.
# 
#   El objetivo de esto es que aprendas a conectar Kafka con la lógica de servicios como los que te proponemos a continuación.
#################################################################

"""
* Modelo de ML: puede ser una regresión lineal o un algoritmo de clasificación, lo importante es que no sea complejo para que sea sencilla su integración con Kafka,
incluso puede ser un proyecto dummy.

* Visualización de datos: Puedes crear una gráfica que te muestre la gráfica en tiempo real, proveyendo los datos mediante Kafka.

* Bot de trading: Creación de una estrategia y la ejecución de órdenes de compra y venta en la testnet del exchange.
No necesitas dinero, pero sí necesitas crear una cuenta y una API Key.
	
	-

* Data Analytics: Debes crear KPI's o indicadores que te digan algo acerca del mercado, los cuales se actualicen en tiempo real para tomar decisiones de compra/venta.

* ...

"""