-- init_weather.sql
CREATE TABLE IF NOT EXISTS current_weather_data (
    lat FLOAT NOT NULL,
    lon FLOAT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    temp FLOAT,
    humidity FLOAT,
    description TEXT,
    PRIMARY KEY (lat, lon, timestamp)
);

CREATE TABLE IF NOT EXISTS hourly_weather_data (
    lat FLOAT NOT NULL,
    lon FLOAT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    temp FLOAT,
    feels_like FLOAT,
    wind_speed FLOAT,
    wind_gust FLOAT,
    clouds FLOAT,
    uvi FLOAT,
    rain FLOAT,
    snow FLOAT,
    pop FLOAT,
    humidity FLOAT,
    description TEXT,
    PRIMARY KEY (lat, lon, timestamp)
);


CREATE TABLE IF NOT EXISTS minute_weather_data (
    lat FLOAT NOT NULL,
    lon FLOAT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    precipitation FLOAT,
    PRIMARY KEY (lat, lon, timestamp)
);