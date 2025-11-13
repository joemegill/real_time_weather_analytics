import json
import time
import requests
from kafka import KafkaProducer, errors
import os
from geopy.geocoders import Nominatim

BROKER = os.getenv("KAFKA_BROKER")
TOPIC = "weather_data"
OPEN_WEATHER_API_KEY = os.getenv("OPEN_WEATHER_API_KEY")


# Initialize the Nominatim geocoder with a user agent
geolocator = Nominatim(user_agent="my-geocoder-app")

# Specify the zip code you want to convert
zip_code = "90210"

# Geocode the zip code
location = geolocator.geocode(zip_code)


for y in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers=[BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        break
    except errors.NoBrokersAvailable:
        print("Kafka not ready, retrying in 5 seconds...")
        time.sleep(5)


def fetch_weather():
    part = "daily,monthly,alerts"
    url = (f"https://api.openweathermap.org/data/3.0/onecall?"
           f"lat={location.latitude}&lon={location.longitude}"
           f"&exclude={part}&appid={OPEN_WEATHER_API_KEY}"
           )
    # url = f"https://api.openweathermap.org/data/3.0/onecall?lat={location.latitude}&lon={location.longitude}&exclude={part}&appid={OPEN_WEATHER_API_KEY}"
    response = requests.get(url)
    return response.json()


if __name__ == "__main__":
    while True:
        data = fetch_weather()
        producer.send(TOPIC, data)

        print(data)
        time.sleep(90)
