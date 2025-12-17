from kafka import KafkaConsumer
import pandas as pd
import sqlite3
import json

def data_cleaning():
    #reading data
    consumer = KafkaConsumer(
        "raw_events",
        bootstrap_servers = ['kafka:9092'],
        auto_offset_reset = 'earliest',
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

def sql_write(df,output_db):
    conn = sqlite3.connect(output_db)
    cursor = conn.cursor()
    
    create_stmt = '''
        CREATE TABLE IF NOT EXISTS events (
            time TIMESTAMP,
            temperature_2m REAL,
            apparent_temperature REAL,
            relative_humidity_2m INTEGER,
            precipitation REAL,
            wind_speed_10m REAL,
            wind_direction_10m INTEGER,
            weather_code INTEGER,
            pressure_msl REAL,
            visibility REAL
        )
    '''
    
    cursor.execute(create_stmt)
    
    df.to_sql('events', conn, if_exists='append', index=False)
    conn.commit()
    conn.close()