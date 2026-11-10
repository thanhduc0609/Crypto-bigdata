"""Opt-in, bounded Kafka/Spark smoke test. Importing this module starts nothing."""
import argparse
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds', type=int, default=30)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 300:
        parser.error('--seconds must be between 1 and 300')
    from pyspark.sql.functions import col
    from processing.core.spark_session import create_spark_session, load_env_vars
    load_env_vars()
    spark = create_spark_session(app_name='CryptoKafkaStreamSmokeTest', include_s3=False)
    query = None
    try:
        source = spark.readStream.format('kafka').option('kafka.bootstrap.servers', os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')).option('subscribe', os.getenv('KAFKA_TOPIC_TRADES', 'crypto-trades')).option('startingOffsets', 'latest').load()
        rows = source.select(col('key').cast('string'), col('value').cast('string'), 'topic', 'partition', 'offset', 'timestamp')
        query = rows.writeStream.format('console').outputMode('append').option('truncate', 'false').start()
        query.awaitTermination(args.seconds)
        if query.exception():
            raise RuntimeError('Streaming query failed')
        print('Bounded smoke check completed; console output is not a full pipeline audit.')
    finally:
        if query is not None:
            query.stop()
        spark.stop()


if __name__ == '__main__':
    main()
