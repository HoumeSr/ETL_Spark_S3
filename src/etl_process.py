import logging


class ETLProcess:
    def __init__(self, spark):
        self.spark = spark
        self.logger = logging.getLogger(__name__)

        self.logger.info(
            f"Initializing ETLProcess"
        )

    def write_parquet(self, df, path):
        try:
            df.write.mode('overwrite').parquet(path)
            self.logger.info()
        except Exception as e:
            self.logger.error(f"Write Parquet '{path}' error", e)

    def run(self, sql_path, out_path):
        with open(sql_path, 'r') as sql_file:

            sql = sql_file.read()
            df = self.spark.sql(sql)

            self.write_parquet(df, out_path)
            self.logger.info(f"Parquet {out_path} created. A total of {df.count()} rows were written.")
