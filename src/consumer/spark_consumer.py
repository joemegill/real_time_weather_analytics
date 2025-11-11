from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window
from pyspark.sql.types import StructType, StructField, FloatType, ArrayType, StringType, LongType
import os
import time
import psycopg2
from datetime import datetime


DB_USER=os.getenv("DB_USER")
DB_PASS=os.getenv("DB_PASS")
DB_HOST=os.getenv("DB_HOST")
DB_NAME=os.getenv("DB_NAME")

def write_row(row):
    conn = psycopg2.connect(
        host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS
    )

    cur = conn.cursor()
    cur.execute("""
            INSERT INTO weather_data (lat, lon, timestamp, temp, humidity, description)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (lat, lon, timestamp) DO UPDATE
            SET temp = EXCLUDED.temp,
                humidity = EXCLUDED.humidity,
                description = EXCLUDED.description;
        """, (row.lat, row.lon, datetime.fromtimestamp(row.timestamp), row.temp, row.humidity, row.description))
    conn.commit()
    cur.close()
    conn.close()


time.sleep(95)

BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")


spark = SparkSession.builder.appName("WeatherStreamProcessor").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", BROKER).option("subscribe", "weather_data").option("startingOffsets", "earliest").load()

schema = StructType([
    StructField("lat", FloatType(), True),
    StructField("lon", FloatType(), True),
    StructField("current", StructType([
        StructField("dt", LongType(), True),
        StructField("temp", FloatType(), True),
        StructField("humidity", FloatType(), True),
        StructField("weather", ArrayType(StructType([
            StructField("id", FloatType(), True),
            StructField("main", StringType(), True),
            StructField("description", StringType(), True)
        ])), True)
    ]), True)
])

# Parse JSON
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data"))

# Extract and print summary
summary_df = parsed_df.select(
    col("data.lat").alias("lat"),
    col("data.lon").alias("lon"),
    col("data.current.dt").alias("timestamp"),
    col("data.current.temp").alias("temp"),
    col("data.current.humidity").alias("humidity"),
    col("data.current.weather")[0]["description"].alias("description")
)


query = summary_df.writeStream.foreach(write_row).start()


query = summary_df.writeStream.outputMode("update").format("console").start()

#Todo write data to postgress
#change format of data to match syle



query.awaitTermination()
