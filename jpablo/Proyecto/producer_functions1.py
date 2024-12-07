
import scipy as sc
import pandas as pd
import numpy as np
from datetime import datetime as dt, timezone, timedelta
import requests
from kafka import KafkaProducer # type: ignore
from json import dumps
import random
import time




#################################################################
# Funciones objetivo:
#   Es lo mínimo que debes hacer para tener tu producer funcionando.
#################################################################
def create_producer(bootstrap_servers="localhost:9092"):
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: dumps(v).encode('utf-8')  # Serializar mensajes a JSON
        )
        print(f"Conexión exitosa al servidor Kafka: {bootstrap_servers}")
        return producer

    except Exception as e:
        print(f"Error al crear el productor Kafka: {e}")
        return None

def get_market_data(symbol="BTCUSDT", interval="1m"):

    url = f"https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": 1  # Solo queremos el último dato
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        kline = response.json()[0]
        market_data = {
            "symbol": symbol,
            "open": float(kline[1]),
            "high": float(kline[2]),
            "low": float(kline[3]),
            "close": float(kline[4]),
            "volume": float(kline[5]),
            "timestamp": dt.fromtimestamp(int(kline[6]) / 1000)
            # Convertir de milisegundos a segundos
        }
        print(kline)
        return market_data

    except requests.exceptions.RequestException as e:
        print(f"Error al obtener datos de la API: {e}")
        return None


def clean_market_data(data):
    
    try:
        # Convertir los datos a un DataFrame
        df = pd.DataFrame([data])

        # Verificar columnas requeridas
        required_columns = {"symbol", "open", "high", "low", "close", "volume", "timestamp"}
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            raise ValueError(f"Faltan las columnas requeridas: {', '.join(missing_columns)}")

        # Convertir timestamp a formato legible
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Validar valores numéricos
        numeric_columns = ["open", "high", "low", "close", "volume"]
        invalid_values = df[numeric_columns].apply(lambda x: (x <= 0).any(), axis=0)
        if invalid_values.any():
            raise ValueError(f"Las columnas con valores inválidos: {', '.join(invalid_values[invalid_values].index)}")

        # Opcional: Rellenar datos faltantes con valores predeterminados
        df = df.fillna({
            "volume": 0  # Por ejemplo, rellenar volumen faltante con 0
        })
        df["timestamp"] = df["timestamp"].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
        return df

    except ValueError as ve:
        print(f"Error de validación: {ve}")
        return None
    except Exception as e:
        print(f"Error inesperado al limpiar los datos: {e}")
        return None

def processing_market_data(symbol="BTCUSDT", interval="1m"):

    try:
        # Obtener datos brutos del mercado
        raw_data = get_market_data(symbol=symbol, interval=interval)
        if not raw_data:
            raise ValueError(f"No se pudieron obtener datos de la API para el símbolo '{symbol}' con intervalo '{interval}'.")

        # Limpiar y procesar los datos
        processed_data = clean_market_data(raw_data)
        if processed_data is None:
            raise ValueError(f"Error al limpiar los datos obtenidos para el símbolo '{symbol}'.")

        print(f"Datos procesados correctamente para '{symbol}' con intervalo '{interval}'.")
        return processed_data

    except ValueError as ve:
        print(f"Error de validación: {ve}")
        return None
    except Exception as e:
        print(f"Error inesperado al procesar los datos de mercado: {e}")
        return None
        

def transformations(data,smooth_interval,smooth_exp):
    try:
        if "close" not in data.columns:
            raise ValueError("La columna 'close' es necesaria para las transformaciones.")

        data["sma"] = data["close"].rolling(window=smooth_interval, min_periods = 1).mean()
        data["ema"] = data["close"].ewm(span=smooth_interval, adjust=False).mean()

        return data
    
    except Exception as e:
        print(f"Error al aplicar transformaciones: {e}")
    
        return None


