-- init_weather.sql
CREATE TABLE IF NOT EXISTS weather_data (
    lat FLOAT NOT NULL,
    lon FLOAT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    temp FLOAT,
    humidity FLOAT,
    description TEXT,
    PRIMARY KEY (lat, lon, timestamp)
);
