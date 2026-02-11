from pyspark.sql import SparkSession
from generator import Generator
import os
import logging
from log import setup_logging


if __name__ == "__main__":
    setup_logging("generate.log")
    logger = logging.getLogger(__name__)
    try:
        spark = (
            SparkSession.builder
            .appName("Generate")
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
            .getOrCreate()
        )
        logger.info(f"SparkSession created")
        Generator(spark, user_count=1, store_count=5, order_count=100).run()
        spark.stop()
    except Exception as e:
        logger.error(f"Creating SparkSession failed: {e}")
        raise
