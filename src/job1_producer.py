import requests
import json
from kafka import KafkaProducer
import time
import logging

def job_producer():
    producer = KafkaProducer(
        bootstrap_servers = ['kafka:9092'],
        value_serializer =lambda v: json.dumps(v).encode('utf-8')
    )
    
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 52.52,
        "longitude": 13.41,
        "hourly": ["temperature_2m", "apparent_temperature", "relative_humidity_2m", "precipitation", "wind_speed_10m", "wind_direction_10m", "weather_code", "pressure_msl", "visibility"],
        "forecast_days": 1,
    }
    
    hourly_keys = ["time"] + params["hourly"]
    
    topic_name = "raw_events"
    try:
        response = requests.get(url=url,params=params)
        data = response.json()
        hourly = data["hourly"]
            
        for i in range(len(hourly["time"])):
            message = {key:hourly[key][i] for key in hourly_keys}
            producer.send('raw_events',value=message)
        producer.flush()       
    except Exception as e:
        logging.error(f"Ingestion error: {e}")