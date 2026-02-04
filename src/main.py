from etl_process import ETLProcess
from config import *
from pyspark.sql import SparkSession
import os

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ETL") \
        .getOrCreate()
    spark.start()

    table_paths = {
        order_path: "order",
        user_path: "user",
        store_path: "store"
    }
    sql_dir = "sql"
    sql_files = [
        "result.sql"
    ]

    sql_paths = []
    for sql_file in sql_files:
        sql_path = os.path.join(sql_dir, sql_file)
        if os.path.exists(sql_path):
            sql_paths.append(sql_path)
        else:
            # Нужно закинуть в лог
            pass

    etl_process = ETLProcess(spark, table_paths)
    if sql_paths:
        etl_process.run(sql_paths)

    spark.stop()
