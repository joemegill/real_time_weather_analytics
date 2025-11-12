from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window, from_unixtime, explode, cast, current_timestamp
from pyspark.sql.types import StructType, StructField, FloatType, ArrayType, StringType, LongType, TimestampType
import os
import time
import psycopg2
from datetime import datetime
from PostgresWriter import PostgresWriter



DB_CONFIG = {
    "host": os.environ.get("DB_HOST"),
    "database": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASS" ),
    "port": 5432
}



def insert_current(cur, row):
    # 1. Define the SQL template using %s placeholders for data
    sql = """
        INSERT INTO current_weather_data 
            (timestamp, lat, lon, temp, humidity, description)
        VALUES 
            (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (lat, lon, timestamp) 
        DO UPDATE SET
            temp = EXCLUDED.temp,
            humidity = EXCLUDED.humidity,
            description = EXCLUDED.description;
    """
    
    # 2. Prepare the data tuple. The order MUST match the column order in the INSERT statement.
    # We must use the column names from your Spark DataFrame Row object.
    # Assuming your DataFrame columns are named: 
    # (timestamp, lat, lon, temp, current_humidity, description)
    
    data = (
        row.timestamp,
        row.lat,
        row.lon,
        row.temp,
        row.humidity, # NOTE: If your Spark DF column is 'current_humidity', change this to row.current_humidity
        row.description
    )

   

def insert_hourly(cur, row):
    
        
    sql = """
        INSERT INTO hourly_weather_data 
        ( lat, lon, timestamp, temp, feels_like, wind_speed, wind_gust, clouds, uvi, rain, snow, pop, humidity, description)
        VALUES ( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s)
        ON CONFLICT (lat, lon, timestamp) 
        DO UPDATE SET
            temp = EXCLUDED.temp,
            feels_like = EXCLUDED.feels_like,
            wind_speed = EXCLUDED.wind_speed,
            wind_gust = EXCLUDED.wind_gust,
            clouds = EXCLUDED.clouds,
            uvi = EXCLUDED.uvi,
            rain = EXCLUDED.rain,
            snow = EXCLUDED.snow,
            pop = EXCLUDED.pop,
            humidity = EXCLUDED.humidity,
            description = EXCLUDED.description;
    """


    
    data = (
            row.lat,
             row.lon,
             row.timestamp,  
             row.temp,
             row.feels_like,
             row.wind_speed, 
             row.wind_gust, 
             row.clouds, 
             row.uvi,
             row.rain, 
             row.snow, 
             row.pop, 
             row.humidity,
             row.description
             )
    cur.execute(sql, data)




def insert_minute(cur, row):
    sql ="""
        INSERT INTO minute_weather_data (timestamp, lat, lon, precipitation)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (timestamp, lat, lon) 
        DO UPDATE SET
            precipitation = EXCLUDED.precipitation;
    """

    data = (row.timestamp, row.lat, row.lon, row.precipitation)
    cur.execute(sql, data)



time.sleep(95)

BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")


spark = SparkSession.builder.appName("WeatherStreamProcessor").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", BROKER).option("subscribe", "weather_data").option("startingOffsets", "earliest").load()

# schema = StructType([
#     StructField("lat", FloatType(), False),
#     StructField("lon", FloatType(), False),
#     StructField("current", StructType([
#         StructField("dt", LongType(), False),
#         StructField("temp", FloatType(), True),
#         StructField("humidity", FloatType(), True),
#         StructField("feels_like", FloatType(), True),
#         StructField("clouds", FloatType(), True),
#         StructField("uvi", FloatType(), True),
#         StructField("weather", ArrayType(StructType([
#             StructField("id", FloatType(), True),
#             StructField("main", StringType(), True),
#             StructField("description", StringType(), True)
#         ])), True)
#     ]), True),
#     StructField("hourly", StructType([
#         StructField("dt", LongType(), False),
#         StructField("temp", FloatType(), True),
#         StructField("feels_like", FloatType(), True),
#         StructField("wind_speed", FloatType(), True),
#         StructField("wind_gust", FloatType(), True),
#         StructField("clouds", FloatType(), True),
#         StructField("uvi", FloatType(), True),
#         StructField("humidity", FloatType(), True),
#         StructField("pop", FloatType(), True),
#         StructField("rain", FloatType(), True),
#         StructField("snow", FloatType(), True),
#         StructField("weather", ArrayType(StructType([
#             StructField("id", FloatType(), True),
#             StructField("main", StringType(), True),
#             StructField("description", StringType(), True)
#         ])), True)
#     ]), True),
    
#     StructField("minutely", StructType([
#         StructField("dt", LongType(), False),
#         StructField("precipitation", FloatType(), True)
#     ]), True)   
# ])

schema = StructType([
    StructField("lat", FloatType(), False),
    StructField("lon", FloatType(), False),

    # --- 1. CURRENT (CORRECT: Single Struct) ---
    StructField("current", StructType([
        StructField("dt", LongType(), False),
        StructField("temp", FloatType(), True),
        StructField("humidity", FloatType(), True),
        StructField("feels_like", FloatType(), True),
        StructField("clouds", FloatType(), True),
        StructField("uvi", FloatType(), True),
        StructField("weather", ArrayType(StructType([
            StructField("id", FloatType(), True),
            StructField("main", StringType(), True),
            StructField("description", StringType(), True)
        ])), True)
    ]), True),

    # --- 2. HOURLY (FIXED: Array of Structs) ---
    StructField("hourly", ArrayType(StructType([
        StructField("dt", LongType(), False),
        StructField("temp", FloatType(), True),
        StructField("feels_like", FloatType(), True),
        StructField("wind_speed", FloatType(), True),
        StructField("wind_gust", FloatType(), True),
        StructField("clouds", FloatType(), True),
        StructField("uvi", FloatType(), True),
        StructField("humidity", FloatType(), True),
        StructField("pop", FloatType(), True),
        StructField("rain", FloatType(), True),
        StructField("snow", FloatType(), True),
        StructField("weather", ArrayType(StructType([
            StructField("id", FloatType(), True),
            StructField("main", StringType(), True),
            StructField("description", StringType(), True)
        ])), True)
    ])), True),  # <--- ArrayType applied here

    # --- 3. MINUTELY (FIXED: Array of Structs) ---
    StructField("minutely", ArrayType(StructType([
        StructField("dt", LongType(), False),
        StructField("precipitation", FloatType(), True)
    ])), True) # <--- ArrayType applied here
])

# Parse JSON
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data"))

#in really life i would get the imperial # but its nice to do some transforms in spark
parsed_df.data.current.temp = (parsed_df.data.current.temp  - 273.15) * 9/5 + 32

# Extract and print summary
summary_df_current = parsed_df.select(
    col("data.lat").alias("lat"),
    col("data.lon").alias("lon"),
    from_unixtime(col("data.current.dt")).cast(TimestampType()).alias("timestamp"),
    col("data.current.temp").alias("temp"),
    col("data.current.humidity").alias("humidity"),
    col("data.current.weather")[0]["description"].alias("description")
)


# summary_df_hourly = parsed_df.select(
#     col("data.lat").alias("lat"),
#     col("data.lon").alias("lon"),
#     from_unixtime(col("data.hourly.dt")).cast(TimestampType()).alias("timestamp"),

#     col("data.hourly.temp").alias("temp"),
#     col("data.hourly.feels_like").alias("feels_like"),
#     col("data.hourly.humidity").alias("humidity"),
    
#     # --- Other Hourly Fields ---
#     col("data.hourly.wind_speed").alias("wind_speed"),
#     col("data.hourly.wind_gust").alias("wind_gust"),
#     col("data.hourly.clouds").alias("clouds"),
#     col("data.hourly.uvi").alias("uvi"),
#     col("data.hourly.pop").alias("pop"),
#     col("data.hourly.rain").alias("rain"),
#     col("data.hourly.snow").alias("snow"),
    
#     # --- Description ---
#     col("data.hourly.weather")[0]["description"].alias("description")
# )

exploded_hourly_df = parsed_df.select(
    col("data.lat").alias("lat"),
    col("data.lon").alias("lon"),
    explode(col("data.hourly")).alias("hourly_data")
)

summary_df_hourly = exploded_hourly_df.select(
    col("lat"),
    col("lon"),
    # Convert epoch (dt) to timestamp
    from_unixtime(col("hourly_data.dt")).cast(TimestampType()).alias("timestamp"),
    
    # Selecting the requested fields using the simple aliases (as requested)
    col("hourly_data.temp").alias("temp"),
    col("hourly_data.feels_like").alias("feels_like"),
    col("hourly_data.humidity").alias("humidity"),
    col("hourly_data.wind_speed").alias("wind_speed"),
    col("hourly_data.wind_gust").alias("wind_gust"),
    col("hourly_data.clouds").alias("clouds"),
    col("hourly_data.uvi").alias("uvi"),
    col("hourly_data.pop").alias("pop"),
    col("hourly_data.rain").alias("rain"),
    col("hourly_data.snow").alias("snow"),
    col("hourly_data.weather")[0]["description"].alias("description")
)

exploded_minute_df = parsed_df.select(
    col("data.lat").alias("lat"),
    col("data.lon").alias("lon"),
    explode(col("data.minutely")).alias("minutely_data")
)

   

summary_df_minute = exploded_minute_df.select(
    col("lat"),
    col("lon"),
    from_unixtime(col("minutely_data.dt")).cast(TimestampType()).alias("timestamp"),
    col("minutely_data.precipitation").alias("precipitation"))


  
print("psycopg2 conntect normald")
conn = psycopg2.connect(**DB_CONFIG)


query_current = summary_df_current.writeStream.foreach(PostgresWriter(DB_CONFIG, insert_current)).start()
query_hourly = summary_df_hourly.writeStream.foreach(PostgresWriter(DB_CONFIG, insert_hourly)).start()
query_minute = summary_df_minute.writeStream.foreach(PostgresWriter(DB_CONFIG, insert_minute )).start()


query_current.awaitTermination()
query_hourly.awaitTermination()
query_minute.awaitTermination()


#TODO fix the usdaet queries
#add adjust the display to account for them
