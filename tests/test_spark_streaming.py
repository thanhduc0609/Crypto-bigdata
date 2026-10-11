"""
=============================================================================
Crypto Big Data - Test Spark Structured Streaming with Kafka
=============================================================================
"""

import os
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pyspark.sql.functions import col
from processing.core.spark_session import create_spark_session, load_env_vars

load_env_vars()

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TRADES", os.getenv("KAFKA_TOPIC", "crypto-trades"))

spark = create_spark_session(app_name="CryptoKafkaStreamSmokeTest", include_s3=False)

print("=" * 70)
print("SPARK KAFKA STREAM TEST")
print(f"Spark version : {spark.version}")
print(f"Kafka server  : {KAFKA_SERVER}")
print(f"Kafka topic   : {KAFKA_TOPIC}")
print("=" * 70)

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

output_df = kafka_df.select(
    col("key").cast("string").alias("key"),
    col("value").cast("string").alias("value"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp")
)

query = (
    output_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

print("\nSpark is listening to Kafka... Press CTRL + C to stop.\n")
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping stream...")
finally:
    spark.stop()
