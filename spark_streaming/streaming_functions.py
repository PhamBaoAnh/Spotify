from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, month, hour, dayofmonth, expr, year, udf, minute, second
import os


@udf
def string_decode(s, encoding='utf-8'):
    if s:
        return (s.encode('latin1')         
                .decode('unicode-escape') 
                .encode('latin1')         
                .decode(encoding)         
                .strip('\"'))

    else:
        return s

def create_or_get_spark_session(app_name, stream=False, master="spark://spark-master:7077"):
    spark = (SparkSession
             .builder
             .appName(app_name)
             .master(master)
             .getOrCreate())
    return spark


def create_kafka_read_stream(spark, groupid, kafka_address, kafka_port, topic, starting_offset="earliest"):
    """
    Creates a kafka read stream

    Parameters:
        spark : SparkSession
            A SparkSession object
        kafka_address: str
            Host address of the kafka bootstrap server
        topic : str
            Name of the kafka topic
        starting_offset: str
            Starting offset configuration, "earliest" by default 
    Returns:
        read_stream: DataStreamReader
    """
    if isinstance(kafka_port, str) or isinstance(kafka_port, int):
   
        print("Reading from: {}:{}".format(kafka_address, kafka_port))
        read_stream = (spark
                    .readStream
                    .format("kafka")
                    .option("group.id", groupid)
                    .option("kafka.bootstrap.servers", "{}:{}".format(kafka_address, kafka_port))
                    .option("failOnDataLoss", False)
                    .option("startingOffsets", starting_offset)
                    .option("subscribe", topic)
                    .load())
    if isinstance(kafka_port, list):
       
        if len(kafka_port) ==2:
            print("Reading from: {}:{},{}:{}".format(kafka_address, kafka_port[0], kafka_address, kafka_port[1]))
            read_stream = (spark
                        .readStream
                        .format("kafka")
                        .option("group.id", groupid)
                        .option("kafka.bootstrap.servers", "{}:{},{}:{}".format(kafka_address, kafka_port[0], kafka_address, kafka_port[1]))
                        .option("failOnDataLoss", False)
                        .option("startingOffsets", starting_offset)
                        .option("subscribe", topic)
                        .load())
        else:
            print("Reading from: {}:{},{}:{},{}:{}".format(kafka_address, kafka_port[0], kafka_address, kafka_port[1], kafka_address, kafka_port[2]))
            read_stream = (spark
                        .readStream
                        .format("kafka")
                        .option("group.id", groupid)
                        .option("kafka.bootstrap.servers", "{}:{},{}:{},{}:{}".format(kafka_address, kafka_port[0], kafka_address, kafka_port[1], kafka_address, kafka_port[2]))
                        .option("failOnDataLoss", False)
                        .option("startingOffsets", starting_offset)
                        .option("subscribe", topic)
                        .load())


    return read_stream


def process_stream(stream, stream_schema, topic):
    """
    Process stream to fetch value from the kafka message, convert ts to timestamp format
    and produce year, month, day, hour, and five-minute interval columns.
    Parameters:
        stream: DataStreamReader
            The data stream reader for your stream
        stream_schema: StructType
            The schema to apply to the data stream
    Returns:
        stream: DataFrame
    """

  
    stream = (stream
              .selectExpr("CAST(value AS STRING)")
              .select(from_json(col("value"), stream_schema).alias("data"))
              .select("data.*")
              )


    stream = (stream
              .withColumn("ts", (col("ts") / 1000).cast("timestamp"))
              .withColumn("year", year("ts"))
              .withColumn("month", month("ts"))
              .withColumn("day", dayofmonth("ts"))
              .withColumn("hour", hour("ts"))
              .withColumn("five_minute_interval",
                          expr("concat(date_format(ts, 'yyyy-MM-dd HH-'), lpad(floor(minute(ts) / 5) * 5, 2, '0'))"))
              )

    if topic in ["listen_events", "page_view_events"]:
        stream = (stream
                .withColumn("song", string_decode("song"))
                .withColumn("artist", string_decode("artist")) 
                )

    return stream

def create_file_write_stream(stream, storage_path, checkpoint_path,
                             trigger="10 seconds", output_mode="append", file_format="parquet"):
    """
    Write the stream back to a file store
    """
    write_stream = (
        stream.writeStream
              .format(file_format)
              .partitionBy("year", "month", "day", "hour", "five_minute_interval")
              .option("path", storage_path)
              .option("checkpointLocation", checkpoint_path)
              .trigger(processingTime=trigger)
              .outputMode(output_mode)
    )
    return write_stream
