# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .getOrCreate()

# Đọc stream từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "mytopic") \
    .option("startingOffsets", "earliest") \
    .load()

# Schema cho value nếu là JSON
schema = StructType().add("id", StringType()).add("message", StringType())

# Convert key, value từ binary -> string, parse JSON
df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Ghi ra console để test
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .start()

# Giữ streaming sống
query.awaitTermination()
