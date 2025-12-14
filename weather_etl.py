import os
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
from io import BytesIO
import requests
import pandas as pd
from minio import Minio
from minio.error import S3Error
from clickhouse_driver import Client
from prefect import flow, task

from dotenv import load_dotenv
load_dotenv()

OPEN_METEO_API = "https://api.open-meteo.com/v1/forecast"

CITIES = {
    "Москва": {"latitude": 55.7558, "longitude": 37.6173},
    "Самара": {"latitude": 53.1959, "longitude": 50.1091}
}

MINIO_HOST = os.getenv("MINIO_HOST", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "weather-data")
MINIO_USE_SSL = os.getenv("MINIO_USE_SSL", "False").lower() == "true"

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9002"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "weather_db")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


def get_minio_client() -> Minio:
    return Minio(
        MINIO_HOST,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_USE_SSL
    )


def get_clickhouse_client() -> Client:
    return Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD or "",
        database=CLICKHOUSE_DATABASE,
        settings={'use_numpy': False}
    )


@task(name="fetch_weather_data")
def fetch_weather_data(city_name: str, coordinates: Dict[str, float]) -> Dict:
    """Получает прогноз погоды для города на завтра"""
    params = {
        "latitude": coordinates["latitude"],
        "longitude": coordinates["longitude"],
        "hourly": "temperature_2m,precipitation,weather_code,wind_speed_10m,wind_direction_10m",
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,weather_code,wind_speed_10m_max",
        "timezone": "Europe/Moscow",
        "forecast_days": 2
    }
    
    response = requests.get(OPEN_METEO_API, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    data["city"] = city_name
    return data


@task(name="save_raw_data_to_minio")
def save_raw_data_to_minio(weather_data: Dict, city_name: str) -> str:
    """Сохраняет сырые данные API в MinIO"""
    client = get_minio_client()
    
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    object_name = f"raw/{city_name.lower()}/{timestamp}.json"
    
    data_bytes = json.dumps(weather_data, ensure_ascii=False).encode('utf-8')
    data_stream = BytesIO(data_bytes)
    
    client.put_object(
        MINIO_BUCKET,
        object_name,
        data=data_stream,
        length=len(data_bytes),
        content_type="application/json"
    )
    return object_name


@task(name="normalize_hourly_data")
def normalize_hourly_data(weather_data: Dict, city_name: str) -> pd.DataFrame:
    """Нормализует почасовые данные для таблицы weather_hourly"""
    hourly = weather_data.get("hourly", {})
    times = pd.to_datetime(hourly.get("time", []), utc=True)
    
    now_utc = datetime.now(timezone.utc)
    tomorrow = now_utc.date() + timedelta(days=1)
    tomorrow_start = pd.Timestamp(tomorrow, tz="UTC")
    tomorrow_end = tomorrow_start + timedelta(days=1)
    
    mask = (times >= tomorrow_start) & (times < tomorrow_end)
    mask_list = mask.tolist()
    
    filtered_times = times[mask]
    filtered_temp = [val for val, m in zip(hourly.get("temperature_2m", []), mask_list) if m]
    filtered_precip = [val for val, m in zip(hourly.get("precipitation", []), mask_list) if m]
    filtered_weather_code = [val for val, m in zip(hourly.get("weather_code", []), mask_list) if m]
    filtered_wind_speed = [val for val, m in zip(hourly.get("wind_speed_10m", []), mask_list) if m]
    filtered_wind_dir = [val for val, m in zip(hourly.get("wind_direction_10m", []), mask_list) if m]
    
    df = pd.DataFrame({
        "timestamp": filtered_times,
        "city": city_name,
        "temperature_celsius": filtered_temp,
        "precipitation_mm": filtered_precip,
        "weather_code": filtered_weather_code,
        "wind_speed_kmh": filtered_wind_speed,
        "wind_direction_degrees": filtered_wind_dir,
    })
    
    df["inserted_at"] = datetime.now(timezone.utc)
    return df


@task(name="aggregate_daily_data")
def aggregate_daily_data(weather_data: Dict, city_name: str) -> pd.DataFrame:
    """Агрегирует дневные данные (min, max, avg температуру и количество осадков)"""
    daily = weather_data.get("daily", {})
    times = pd.to_datetime(daily.get("time", []), utc=True)
    
    now_utc = datetime.now(timezone.utc)
    tomorrow = now_utc.date() + timedelta(days=1)
    idx = 1 if len(times) > 1 else 0
    
    df = pd.DataFrame({
        "date": [tomorrow],
        "city": [city_name],
        "temp_min_celsius": [daily["temperature_2m_min"][idx] if idx < len(daily["temperature_2m_min"]) else None],
        "temp_max_celsius": [daily["temperature_2m_max"][idx] if idx < len(daily["temperature_2m_max"]) else None],
        "precipitation_mm": [daily["precipitation_sum"][idx] if idx < len(daily["precipitation_sum"]) else None],
        "weather_code": [daily["weather_code"][idx] if idx < len(daily["weather_code"]) else None],
        "wind_speed_max_kmh": [daily["wind_speed_10m_max"][idx] if idx < len(daily["wind_speed_10m_max"]) else None],
    })
    
    hourly_data = weather_data.get("hourly", {})
    hourly_times = pd.to_datetime(hourly_data.get("time", []), utc=True)
    hourly_temps = hourly_data.get("temperature_2m", [])
    
    tomorrow_start = pd.Timestamp(tomorrow, tz="UTC")
    tomorrow_end = tomorrow_start + timedelta(days=1)
    mask = (hourly_times >= tomorrow_start) & (hourly_times < tomorrow_end)
    
    temps_tomorrow = [temp for temp, m in zip(hourly_temps, mask) if m]
    avg_temp = sum(temps_tomorrow) / len(temps_tomorrow) if temps_tomorrow else None
    df["temp_avg_celsius"] = avg_temp
    
    df["inserted_at"] = datetime.now(timezone.utc)
    return df


def create_clickhouse_tables():
    """Создает таблицы weather_hourly и weather_daily в ClickHouse"""
    client = get_clickhouse_client()
    
    create_hourly_table = """
    CREATE TABLE IF NOT EXISTS weather_hourly (
        timestamp DateTime,
        city String,
        temperature_celsius Float32,
        precipitation_mm Float32,
        weather_code Int32,
        wind_speed_kmh Float32,
        wind_direction_degrees Float32,
        inserted_at DateTime
    ) ENGINE = MergeTree()
    ORDER BY (city, timestamp);
    """
    
    create_daily_table = """
    CREATE TABLE IF NOT EXISTS weather_daily (
        date Date,
        city String,
        temp_min_celsius Float32,
        temp_max_celsius Float32,
        temp_avg_celsius Float32,
        precipitation_mm Float32,
        weather_code Int32,
        wind_speed_max_kmh Float32,
        inserted_at DateTime
    ) ENGINE = MergeTree()
    ORDER BY (city, date);
    """
    
    client.execute(create_hourly_table)
    client.execute(create_daily_table)
    client.disconnect()


@task(name="load_hourly_data")
def load_hourly_data(df: pd.DataFrame) -> int:
    """Загружает почасовые данные в ClickHouse"""
    if df.empty:
        return 0
    
    client = get_clickhouse_client()
    records = [
        (
            row["timestamp"],
            row["city"],
            row["temperature_celsius"],
            row["precipitation_mm"],
            row["weather_code"],
            row["wind_speed_kmh"],
            row["wind_direction_degrees"],
            row["inserted_at"]
        )
        for _, row in df.iterrows()
    ]
    
    client.execute("INSERT INTO weather_hourly VALUES", records)
    count = len(records)
    client.disconnect()
    return count


@task(name="load_daily_data")
def load_daily_data(df: pd.DataFrame) -> int:
    """Загружает дневные данные в ClickHouse"""
    if df.empty:
        return 0
    
    client = get_clickhouse_client()
    records = [
        (
            row["date"],
            row["city"],
            row["temp_min_celsius"],
            row["temp_max_celsius"],
            row["temp_avg_celsius"],
            row["precipitation_mm"],
            row["weather_code"],
            row["wind_speed_max_kmh"],
            row["inserted_at"]
        )
        for _, row in df.iterrows()
    ]
    
    client.execute("INSERT INTO weather_daily VALUES", records)
    count = len(records)
    client.disconnect()
    return count


def get_weather_description(code: int) -> str:
    """Преобразует код погоды WMO в описание"""
    weather_codes = {
        0: "Ясно", 1: "Облачно", 2: "Переменная облачность", 3: "Пасмурно",
        45: "Туман", 48: "Туман с изморозью",
        51: "Легкая морось", 53: "Умеренная морось", 55: "Плотная морось",
        61: "Слабый дождь", 63: "Умеренный дождь", 65: "Сильный дождь",
        71: "Слабый снег", 73: "Умеренный снег", 75: "Сильный снег", 77: "Снежные зёрна",
        80: "Слабые ливни", 81: "Умеренные ливни", 82: "Сильные ливни",
        85: "Слабые ливни со снегом", 86: "Сильные ливни со снегом",
        95: "Гроза", 96: "Гроза с градом", 99: "Гроза с градом",
    }
    return weather_codes.get(code, f"Код {code}")


@task(name="send_telegram_notification")
def send_telegram_notification(daily_df: pd.DataFrame) -> bool:
    """Отправляет уведомление в Telegram с прогнозом и предупреждениями о сильном ветре/осадках"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    
    message_lines = ["📋 *Прогноз погоды на завтра:*\n"]
    
    for _, row in daily_df.iterrows():
        city = row["city"]
        temp_min = round(row["temp_min_celsius"], 1)
        temp_max = round(row["temp_max_celsius"], 1)
        temp_avg = round(row["temp_avg_celsius"], 1)
        precip = round(row["precipitation_mm"], 1)
        wind_max = round(row["wind_speed_max_kmh"], 1)
        weather_desc = get_weather_description(row["weather_code"])
        
        message_lines.append(f"🌍 *{city}*")
        message_lines.append(f"🌡️ Температура: min {temp_min}°C, max {temp_max}°C (avg {temp_avg}°C)")
        message_lines.append(f"🌧️ Осадки: {precip} мм")
        message_lines.append(f"💨 Макс ветер: {wind_max} км/ч")
        message_lines.append(f"🌤️ Погода: {weather_desc}")
        
        if wind_max > 30:
            message_lines.append("⚠️ ВНИМАНИЕ: Сильный ветер!")
        if precip > 10:
            message_lines.append("⚠️ ВНИМАНИЕ: Интенсивные осадки!")
        
        message_lines.append("")
    
    message = "\n".join(message_lines)
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    
    response = requests.post(url, json=payload, timeout=10)
    response.raise_for_status()
    return True


@flow(name="weather_etl")
def weather_etl(cities: Optional[Dict[str, Dict[str, float]]] = None):
    """Главный ETL flow: Extract → Transform → Load"""
    if cities is None:
        cities = CITIES
    
    create_clickhouse_tables()
    
    weather_data_list = []
    minio_paths = []
    
    for city_name, coordinates in cities.items():
        weather_data = fetch_weather_data(city_name, coordinates)
        weather_data_list.append(weather_data)
        minio_path = save_raw_data_to_minio(weather_data, city_name)
        minio_paths.append(minio_path)
    
    hourly_dfs = []
    daily_dfs = []
    
    for weather_data in weather_data_list:
        city_name = weather_data["city"]
        hourly_df = normalize_hourly_data(weather_data, city_name)
        hourly_dfs.append(hourly_df)
        daily_df = aggregate_daily_data(weather_data, city_name)
        daily_dfs.append(daily_df)
    
    combined_hourly = pd.concat(hourly_dfs, ignore_index=True) if hourly_dfs else pd.DataFrame()
    combined_daily = pd.concat(daily_dfs, ignore_index=True) if daily_dfs else pd.DataFrame()
    
    hourly_count = load_hourly_data(combined_hourly)
    daily_count = load_daily_data(combined_daily)
    
    send_telegram_notification(combined_daily)
    
    return {
        "hourly_records": hourly_count,
        "daily_records": daily_count,
        "minio_paths": minio_paths
    }


if __name__ == "__main__":
    result = weather_etl()
    print("Pipeline result:", result)
