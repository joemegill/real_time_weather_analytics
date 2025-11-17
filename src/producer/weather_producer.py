import json
import time
import requests
from kafka import KafkaProducer, errors
import os
from geopy.geocoders import Nominatim
from typing import Any, Dict


def start_kafka(broker: str) -> KafkaProducer:
    for y in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[broker],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            return producer
        except errors.NoBrokersAvailable:
            print("Kafka not ready, retrying in 5 seconds...")
            time.sleep(5)

            return False


def fetch_weather(lat: str, lon: str, api_key: str) -> Dict[str, Any]:
    part = "daily,monthly,alerts"
    url = (
        f"https://api.openweathermap.org/data/3.0/onecall?"
        f"lat={lat}&lon={lon}"
        f"&exclude={part}&appid={api_key}"
    )
    response = requests.get(url)
    return response.json()


def main() -> None:
    broker = os.getenv("KAFKA_BROKER")
    topic = "weather_data"

    # Initialize the Nominatim geocoder with a user agent
    geolocator = Nominatim(user_agent="my-geocoder-app")
    # Specify the zip code you want to convert
    zip_code = "90210"
    # Geocode the zip code
    location = geolocator.geocode(zip_code)

    lat = location.latitude
    lon = location.longitude
    api_key = os.getenv("OPEN_WEATHER_API_KEY")

    producer = start_kafka(broker)

    while producer:
        data = fetch_weather(lat, lon, api_key)
        producer.send(topic, data)

        print(data)
        time.sleep(90)


if __name__ == "__main__":
    main()
