# Weather Data Pipeline (Open-Meteo, Kafka, Airflow)

## Team Members
| Name             | ID          |
|-----------------|------------|
| Amirgali Sanzhar | 22B030621  |
| Duman Bayron     | 22B030147  |
| Beketay Symbat   | 22B030325  |

---

## Project Description
This project implements an end-to-end data pipeline for collecting, cleaning, storing, and analyzing weather data using a real public API.  

The pipeline demonstrates key data engineering concepts:
- Streaming data ingestion with Kafka
- Batch data processing
- Data cleaning and validation
- Analytical aggregation
- Workflow orchestration using Apache Airflow  

Developed as a final assignment for the **Data Collection & Preparation** course.

---

##  Quick Start

### 1. Launch the Pipeline
Run the following command to build and start all services (Kafka, Postgres, Airflow) in the background:
```bash
docker compose up -d --build
```

### 2. Access Airflow
Open in your browser:
```
http://localhost:8080
```
Login credentials:
```text
Username: admin
Password: admin
```
### 3.Stop and Clean
Stop all services:
```bash
docker compose down
```
Stop and delete all data (reset everything):
```bash
docker compose down -v
```

---

## Data Source
- **API:** [Open-Meteo Weather Forecast API](https://open-meteo.com/)  
- Collects hourly weather forecast data for a fixed geographic location.

**Hourly weather parameters collected:**
- `temperature_2m` – Temperature at 2 meters
- `apparent_temperature` – Apparent temperature
- `relative_humidity_2m` – Relative humidity
- `precipitation`
- `wind_speed_10m`
- `wind_direction_10m`
- `weather_code`
- `pressure_msl` – Sea level pressure
- `visibility`

---

## Pipeline Architecture

### Job 1 — Data Ingestion (Streaming)
- Runs every **5 minutes** (Airflow schedule `*/5 * * * *`).
- Requests hourly weather data from Open-Meteo API.
- Sends raw weather events to Kafka topic: `raw_events`.
- Each Kafka message represents one forecast hour.

### Job 2 — Data Cleaning and Storage (Hourly Batch)
- Runs **hourly** (`@hourly` schedule in Airflow).
- Reads messages from Kafka `raw_events`.
- Converts timestamps to datetime, drops missing values.
- Applies validation rules:
  - Temperature between [-60, 60] °C
  - Relative humidity between [0, 100]
  - Wind speed ≥ 0
- Stores cleaned data into SQLite table `events`.
- Uses UPSERT to avoid duplicates.

### Job 3 — Daily Analytics (Batch)
- Runs **daily** (`@daily` schedule in Airflow).
- Reads cleaned data from SQLite table `events`.
- Aggregates data by date and computes:
  - Average, min, and max temperature
  - Average humidity
  - Maximum wind speed
  - Total daily precipitation
- Writes results to table `daily_summary`.

---

## Database
**Database type:** SQLite  
**Database file:** `data/weather_event.db`

### Table: `events`
Stores cleaned hourly weather data:
- `time` TIMESTAMP PRIMARY KEY
- `temperature_2m` REAL
- `apparent_temperature` REAL
- `relative_humidity_2m` INTEGER
- `precipitation` REAL
- `wind_speed_10m` REAL
- `wind_direction_10m` INTEGER
- `weather_code` INTEGER
- `pressure_msl` REAL
- `visibility` REAL

### Table: `daily_summary`
Stores aggregated daily statistics:
- `date`
- `avg_temp`
- `min_temp`
- `max_temp`
- `avg_humidity`
- `max_wind_speed`
- `total_precipitation`

---

## Project Structure

```text
.
├── airflow/
│   └── dags/
│       ├── job1_ingestion_dag.py    # Оркестрация сбора данных из API в Kafka
│       ├── job2_clean_store_dag.py  # Оркестрация обработки данных из Kafka в SQLite
│       └── job3_daily_summary_dag.py # Оркестрация ежедневной аналитики
├── src/
│   ├── job1_producer.py             # Логика работы Kafka Producer (API fetch)
│   ├── job2_cleaner.py              # Логика валидации и очистки данных
│   ├── job3_analytics.py            # SQL-запросы для агрегации данных
│   └── db_utils.py                  # Вспомогательные функции для работы с БД
├── data/
│   └── weather_event.db             # Локальная БД SQLite (хранилище)
├── docker-compose.yaml              # Конфигурация сервисов (Airflow, Kafka, Postgres)
├── dockerfile                       # Образ с предустановленными зависимостями
├── requirements.txt                 # Список Python-библиотек
└── README.md
```
