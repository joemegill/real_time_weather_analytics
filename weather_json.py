import os
from geopy.geocoders import Nominatim
import json
import time
import requests


CITY="London"


# Initialize the Nominatim geocoder with a user agent
geolocator = Nominatim(user_agent="my-geocoder-app") 

# Specify the zip code you want to convert
zip_code = "90210"

# Geocode the zip code
location = geolocator.geocode(zip_code)
OPEN_WEATHER_API_KEY = os.getenv("OPEN_WEATHER_API_KEY")





def fetch_weather():
    # part = "daily,monthly,alerts"
    part = ""
    url = f"https://api.openweathermap.org/data/3.0/onecall?lat={location.latitude}&lon={location.longitude}&exclude={part}&appid={OPEN_WEATHER_API_KEY}"
    response = requests.get(url)

    filename = "test_example.json"
    print(response.json)
    
    # Specify the filename for the JSON output

    # Open the file in write mode ('w') and use json.dump()
    with open(filename, 'w') as json_file:
        json.dump(response.json(), json_file, indent=4)
        

if __name__ == "__main__":
    print(OPEN_WEATHER_API_KEY)
    fetch_weather()