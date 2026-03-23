# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import *

# # -------------------------------
# # 1. Create Spark Session
# # -------------------------------
# spark = SparkSession.builder \
#     .appName("SessionWindowConsumer") \
#     .getOrCreate()


# spark.sparkContext.setLogLevel("ERROR")
# # spark.conf.set("spark.sql.shuffle.partitions", "2")

# # -------------------------------
# # 2. Define Schema
# # -------------------------------
# schema = StructType([
#     StructField("user_id", StringType()),
#     StructField("event_time", TimestampType()),
#     StructField("action", StringType())
# ])

# # -------------------------------
# # 3. Read Stream from Socket
# # -------------------------------
# raw_df = spark.readStream \
#     .format("socket") \
#     .option("host", "localhost") \
#     .option("port", 9999) \
#     .load()

# # -------------------------------
# # 4. Parse JSON Data
# # -------------------------------
# df = raw_df.select(
#     from_json(col("value"), schema).alias("data")
# ).select("data.*")

# # -------------------------------
# # 5. Apply Watermark + Session Window
# # -------------------------------
# # session_df = df \
# #     .withWatermark("event_time", "15 minutes") \
# #     .groupBy(
# #         window("event_time", "10 minutes"),
# #         col("user_id")
# #     ) \
# #     .agg(
# #         count("*").alias("event_count"),
# #         min("event_time").alias("session_start"),
# #         max("event_time").alias("session_end")
# #     )

# # -------------------------------
# # 6. Select Final Output
# # -------------------------------
# # result = session_df.select(
# #     col("user_id"),
# #     col("session_start"),
# #     col("session_end"),
# #     col("event_count")
# # )

# # -------------------------------
# # 7. Write Output to Console
# # -------------------------------
# query = df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", False) \
#     .start()

# query.awaitTermination()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SimpleTest") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read from socket
df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Print directly
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()