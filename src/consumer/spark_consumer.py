from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import os

BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")


spark = SparkSession.builder.appName("WeatherStreamProcessor").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("name", StringType()),
    StructField("main", StructType([
        StructField("temp", FloatType()),
        StructField("humidity", FloatType())
    ]))
])

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", BROKER).option("subscribe", "weather_data").load()
json_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

agg_df = json_df.groupBy(window(col("timestamp"), "10 minutes"), "name").agg(avg("main.temp").alias("avg_temp"), avg("main.humidity").alias("avg_humidity"))

query = agg_df.writeStream.outputMode("update").format("console").start()
query.awaitTermination()
