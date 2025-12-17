import pandas as pd
import sqlite3

def daily_analytics():
    conn = sqlite3.connect('/opt/airflow/data/weather_event.db')
    
    
    query = "SELECT * FROM events"
    df = pd.read_sql(query,conn)
    
    df["time"] = pd.to_datetime(df["time"]).dt.date
    
    summary_df = df.groupby('time').agg({
        'temperature_2m': ['mean', 'min', 'max'],
        'relative_humidity_2m': 'mean',
        'wind_speed_10m': 'max',
        'precipitation': 'sum'
    }).reset_index()
    
    summary_df.columns = [
        'date', 'avg_temp', 'min_temp', 'max_temp', 
        'avg_humidity', 'max_wind_speed', 'total_precipitation'
    ]

    summary_df.to_sql('daily_summary', conn, if_exists='replace', index=False)
    
    conn.close()