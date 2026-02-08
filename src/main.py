from etl_process import ETLProcess
from config import *
from pyspark.sql import SparkSession
import os
from log import setup_logging
import logging

if __name__ == "__main__":
    setup_logging("etl.log")
    logger = logging.getLogger(__name__)
    try:
        spark = (
            SparkSession.builder
            .appName("ETL")
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

        table_paths = {
            order_path: "order",
            user_path: "user",
            store_path: "store"
        }

        sql_path = "/opt/spark/sql/result.sql"

        if os.path.exists(sql_path):
            etl_process = ETLProcess(spark, table_paths)
            etl_process.run(sql_path, result_path)
        else:
            logger.critical(f"{sql_path} file/path not exists")

        spark.stop()
    except Exception as e:
        logger.error(f"Creating SparkSession failed: {e}")
        raise
