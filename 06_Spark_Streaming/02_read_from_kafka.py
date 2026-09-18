# 06_Spark_Streaming/02_read_from_kafka.py
# this code snippet demonstrates how to set up a basic Spark Structured Streaming application that reads data from a Kafka topic named 'test-topic'. The application connects to a Kafka broker running at 'ec2-3-145-208-232.us-east-2.compute.amazonaws.com:9092' and processes the incoming messages in real-time. The messages are expected to be in JSON format, and the application defines a schema to parse the JSON data into a structured DataFrame. The streaming query is configured to print the processed data to the console continuously, allowing for real-time monitoring of the incoming Kafka messages.
# this Spark Structured Streaming application reads data from a Kafka topic named 'test-topic' and processes it in real-time. The application connects to a Kafka broker running at 'ec2-3-145-208-232.us-east-2.compute.amazonaws.com:9092' and expects the incoming messages to be in JSON format. A schema is defined to parse the JSON data into a structured DataFrame, allowing for easy manipulation and analysis of the data. The streaming query is configured to print the processed data to the console continuously, enabling real-time monitoring of the incoming Kafka messages.

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

# Define schema for the incoming data
schema = StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), True)
])

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "ec2-3-145-208-232.us-east-2.compute.amazonaws.com:9092") \
    .option("subscribe", "test-topic") \
    .load()

# Convert the value column from binary to string
value_df = kafka_df.selectExpr("CAST(value AS STRING) as value")

# Parse the JSON string into a DataFrame with the defined schema
json_df = value_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Start the streaming query and print data to console using continuous mode
query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="0 seconds") \
    .option("checkpointLocation", "spark_data") \
    .start()

query.awaitTermination()
