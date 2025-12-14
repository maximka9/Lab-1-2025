from prefect.deployments import Deployment
from prefect.client.schemas.schedules import CronSchedule
from weather_etl import weather_etl

schedule = CronSchedule(cron="0 6 * * *", timezone="Europe/Moscow")

deployment = Deployment.build_from_flow(
    flow=weather_etl,
    name="weather-etl-daily",
    schedules=[schedule],
    tags=["weather", "etl", "daily"],
    description="ETL pipeline for weather data from Open-Meteo API",
    version="1.0.0"
)

if __name__ == "__main__":
    deployment.apply()
    print("Deployment 'weather-etl-daily' created successfully!")
