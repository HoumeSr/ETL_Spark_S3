from config import result_path, result_sql
from pyspark.sql import SparkSession
import os
import logging

from etl_process import ETLProcess
from log import setup_logging
from tables import store, order, user


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

        store(spark).createTempTable()
        user(spark).createTempTable()
        order(spark).createTempTable()

        if os.path.exists(result_sql):
            etl_process = ETLProcess(spark)
            etl_process.run(result_sql, result_path)
        else:
            logger.critical(f"{result_sql} file/path not exists")

        spark.stop()
    except Exception as e:
        logger.error(f"Creating SparkSession failed: {e}")
        raise
