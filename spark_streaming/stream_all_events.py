# -*- coding: utf-8 -*-
# Run with:
# spark-submit \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.apache.hadoop:hadoop-aws:3.2.0 \
#   stream_all_events.py

from __future__ import print_function
import os
from streaming_functions import *
from schema import schema
from pyspark.sql.functions import lit


TOPICS = ["listen_events", "page_view_events", "auth_events"]

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker")
KAFKA_PORT = int(os.getenv("KAFKA_PORT", 29092))


MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'minio')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'minio123')
MINIO_BUCKET = os.environ.get('MINIO_BUCKET', 'streamify')


spark = create_or_get_spark_session('Eventsim Stream')
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
hadoop_conf.set("fs.s3a.path.style.access", "true")
spark.streams.resetTerminated()

groupid = "streamify"

def create_file_write_stream(stream, storage_path, checkpoint_path,
                             trigger="10 seconds", output_mode="append", file_format="parquet"):
    return (
        stream.writeStream
              .format(file_format)
              .partitionBy("year", "month", "day", "hour", "five_minute_interval")
              .option("path", storage_path)
              .option("checkpointLocation", checkpoint_path)
              .trigger(processingTime=trigger)
              .outputMode(output_mode)
    )


for topic in TOPICS:
 
    stream_df = create_kafka_read_stream(
        spark, groupid, KAFKA_BOOTSTRAP, KAFKA_PORT, topic
    )
    
    stream_df = process_stream(stream_df, schema[topic], topic)
    
    print("--- Schema for topic: {} ---".format(topic))
    stream_df.printSchema()
    
 
    (
        create_file_write_stream(
            stream_df,
            "s3a://{}/{}/".format(MINIO_BUCKET, topic),
            "s3a://{}/checkpoints/{}/".format(MINIO_BUCKET, topic),
            trigger="10 seconds"
        )
        .queryName("{}_minio_sink".format(topic))
        .start()
    )


spark.streams.awaitAnyTermination()
