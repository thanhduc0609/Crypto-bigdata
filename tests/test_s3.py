"""
=============================================================================
Crypto Big Data - Test AWS S3 / MinIO Connection from Spark
=============================================================================
"""

import os
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from processing.core.spark_session import create_spark_session, load_env_vars

load_env_vars()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "")

print("=" * 70)
print("TEST SPARK -> AWS S3 CONNECTION")
print(f"Bucket: {S3_BUCKET}")
print(f"Region: {AWS_REGION}")
print(f"Access Key: {AWS_ACCESS_KEY[:4]}***" if AWS_ACCESS_KEY else "Access Key: (Not set)")
print("=" * 70)

if not S3_BUCKET or not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
    print("[ERROR] Please provide AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and S3_BUCKET_NAME in your .env file.")
    sys.exit(1)

spark = create_spark_session(app_name="SparkS3Test", include_kafka=False)

test_data = [
    ("BTC-USDT", 110000.0, 0.05, "buy", 1727850000000),
    ("ETH-USDT", 4000.0, 1.2, "sell", 1727850001000),
    ("SOL-USDT", 200.0, 15.0, "buy", 1727850002000),
]

df = spark.createDataFrame(
    test_data,
    ["symbol", "price", "size", "side", "timestamp"]
)

print("\nSample DataFrame created:")
df.show()

test_output_path = f"s3a://{S3_BUCKET}/crypto/test_connection"
print(f"Attempting write to: {test_output_path} ...")

try:
    df.write.mode("overwrite").parquet(test_output_path)
    print("\n[SUCCESS] Successfully written Parquet to S3!")
    read_back_df = spark.read.parquet(test_output_path)
    print("\nReading back data from S3:")
    read_back_df.show()
    print("[SUCCESS] S3 Read & Write permissions verified successfully!")
except Exception as e:
    print(f"\n[FAILED] Error writing to S3: {e}")
finally:
    spark.stop()
