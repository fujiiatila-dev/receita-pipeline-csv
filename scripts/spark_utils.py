from pyspark.sql import SparkSession
from delta import *
import os

def get_spark_session(app_name):
    """
    Retorna uma SparkSession configurada para Delta Lake e MinIO (S3).
    A conexão é feita internamente na rede do Docker (http://minio:9000).
    """
    
    # Endpoint interno do Docker
    MINIO_ENDPOINT = "http://minio:9000"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"

    print(f"--- Iniciando Spark Session: {app_name} ---")
    print(f"--- Endpoint S3: {MINIO_ENDPOINT} ---")

    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")

    return configure_spark_with_delta_pip(builder).getOrCreate()