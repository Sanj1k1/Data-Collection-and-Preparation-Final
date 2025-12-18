from kafka import KafkaConsumer
import pandas as pd
import sqlite3
import json

def data_cleaning():
    consumer = KafkaConsumer(
        "raw_events",
        bootstrap_servers = ['kafka:9092'],
        auto_offset_reset = 'earliest',
        enable_auto_commit=True,
        value_deserializer = lambda v: json.loads(v.decode('utf-8')),
        consumer_timeout_ms=5000 
    )
    
    raw_data = []
    for data in consumer:
        raw_data.append(data.value)
        
    consumer.close()

    
    df = pd.DataFrame(raw_data)
    
    #type conversion
    df["time"] = pd.to_datetime(df['time'])
    df = df.dropna()
    df = df[(df['temperature_2m'] >= -60) & (df['temperature_2m'] <= 60)]
    df = df[(df['relative_humidity_2m'] >= 0) & (df['relative_humidity_2m'] <= 100)]
    df = df[df['wind_speed_10m'] >= 0]
    
    return df