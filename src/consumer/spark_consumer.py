from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window
from pyspark.sql.types import StructType, StructField, FloatType, ArrayType, StringType
import os
import time

time.sleep(95)

BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")


spark = SparkSession.builder.appName("WeatherStreamProcessor").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", BROKER).option("subscribe", "weather_data").option("startingOffsets", "earliest").load()

schema = StructType([
    StructField("lat", FloatType(), True),
    StructField("lon", FloatType(), True),
    StructField("current", StructType([
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
    col("data.current.temp").alias("temp"),
    col("data.current.humidity").alias("humidity"),
    col("data.current.weather")[0]["description"].alias("description")
)



query = summary_df.writeStream.outputMode("update").format("console").start()
query.awaitTermination()
