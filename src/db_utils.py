import sqlite3
import pandas as pd

db_path = '/opt/airflow/data/weather_event.db'

def sql_write(df,output_db):
    conn = sqlite3.connect(output_db)
    cursor = conn.cursor()
    
    create_stmt = '''
        CREATE TABLE IF NOT EXISTS events (
            time TIMESTAMP PRIMARY KEY,
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

    upsert_query = '''
        INSERT INTO events (
            time, temperature_2m, apparent_temperature, relative_humidity_2m, 
            precipitation, wind_speed_10m, wind_direction_10m, 
            weather_code, pressure_msl, visibility
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(time) DO UPDATE SET
            temperature_2m=excluded.temperature_2m,
            apparent_temperature=excluded.apparent_temperature,
            relative_humidity_2m=excluded.relative_humidity_2m,
            precipitation=excluded.precipitation,
            wind_speed_10m=excluded.wind_speed_10m,
            wind_direction_10m=excluded.wind_direction_10m,
            weather_code=excluded.weather_code,
            pressure_msl=excluded.pressure_msl,
            visibility=excluded.visibility
    '''
    
    df_records = df.copy()
    df_records['time'] = df_records['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    data_to_insert = df_records.values.tolist()
    cursor.executemany(upsert_query, data_to_insert)
    
    conn.commit()
    conn.close()