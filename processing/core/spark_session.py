"""
=============================================================================
Crypto Big Data - Core Spark Session Builder (Shared for Batch & Streaming)
=============================================================================
"""

import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession

project_root = Path(__file__).resolve().parent.parent.parent


def ensure_java_and_hadoop():
    """Tự động kiểm tra và thiết lập HADOOP_HOME và JAVA_HOME"""
    if "HADOOP_HOME" not in os.environ:
        os.environ["HADOOP_HOME"] = r"C:\hadoop"

    hadoop_home = os.environ.get("HADOOP_HOME", r"C:\hadoop")
    hadoop_bin = os.path.join(hadoop_home, "bin")
    if os.path.exists(hadoop_bin) and hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = f"{hadoop_bin};" + os.environ.get("PATH", "")

    current_java = os.environ.get("JAVA_HOME", "")
    if not current_java or not os.path.exists(current_java):
        candidates = [
            r"C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot",
            r"C:\Program Files\Java\jdk-17",
            r"C:\Program Files\Eclipse Adoptium"
        ]
        for path in candidates:
            if os.path.exists(path):
                os.environ["JAVA_HOME"] = path
                os.environ["PATH"] = f"{path}\\bin;" + os.environ.get("PATH", "")
                break


def load_env_vars():
    """Đọc file .env nếu chưa có trong biến môi trường và mở rộng các biến lồng nhau"""
    env_path = project_root / ".env"
    if not env_path.exists():
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip().strip("'\"")
            if k not in os.environ or os.environ[k].startswith("${"):
                os.environ[k] = v

    # Mở rộng các biến có dạng ${VAR_NAME}
    bucket_name = os.getenv("S3_BUCKET_NAME", "")
    for k, v in list(os.environ.items()):
        if "${S3_BUCKET_NAME}" in v and bucket_name:
            os.environ[k] = v.replace("${S3_BUCKET_NAME}", bucket_name)
        elif "${" in v:
            for sub_k, sub_v in list(os.environ.items()):
                if sub_v and f"${{{sub_k}}}" in v:
                    v = v.replace(f"${{{sub_k}}}", sub_v)
            os.environ[k] = v


def create_spark_session(
    app_name: str = "CryptoBigDataProcessing",
    master: str = "local[*]",
    include_kafka: bool = True,
    include_s3: bool = True,
    include_postgres: bool = True
) -> SparkSession:
    """Tạo Spark Session chuẩn hóa với đầy đủ dependencies cấu hình"""
    ensure_java_and_hadoop()
    load_env_vars()

    packages = []
    if include_kafka:
        packages.append("org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    if include_s3:
        packages.extend([
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ])
    if include_postgres:
        packages.append("org.postgresql:postgresql:42.6.0")

    spark_temp = project_root / "data" / "spark_temp"
    spark_temp.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.local.dir", str(spark_temp))
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
    )

    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))

    # Cấu hình S3A FileSystem
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    aws_region = os.getenv("AWS_REGION", "ap-southeast-1")
    s3_endpoint = os.getenv("S3_ENDPOINT_URL", "")

    if include_s3:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
            .config("spark.hadoop.fs.s3a.fast.upload.active.blocks", "4")
            .config("spark.hadoop.fs.s3a.multipart.size", "64M")
        )
        if aws_access_key and aws_secret_key:
            builder = (
                builder
                .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
                .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
            )
        if s3_endpoint:
            builder = (
                builder
                .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            )
        elif aws_region:
            builder = builder.config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
