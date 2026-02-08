import logging


class ETLProcess:
    def __init__(self, spark, table_paths: dict):
        self.spark = spark
        self.table_paths = table_paths

        self.logger = logging.getLogger(__name__)

        self.logger.info(
            f"Initializing ETLProcess"
        )

    def valide_tables(self):
        result = True
        for table_path in self.table_paths:
            try:
                self.spark.read.parquet(table_path).limit(1).collect()
            except:
                result = False
        return result

    def write_parquet(self, df, path):
        try:
            df.write.mode('overwrite').parquet(path)
            self.logger.info()
        except Exception as e:
            self.logger.error(f"Write Parquet '{path}' error", e)

    def create_TempView(self, table_path, table_name):
        try:
            df = self.spark.read.parquet(table_path)
            df.createTempView(table_name)
            self.logger.info(f"CreateTempView {table_name} success")
        except Exception as e:
            self.logger.info(f"Creating view {table_name} failed", e)
            pass

    def run(self, sql_path, out_path):
        if self.valide_tables():
            for table_path, table_name in self.table_paths.items():
                self.create_TempView(table_path, table_name)
            with open(sql_path, 'r') as sql_file:
                sql = sql_file.read()
                df = self.spark.sql(sql)
                self.write_parquet(
                    df, out_path)
                self.logger.info(f"Parquet {out_path} created")
        else:
            self.logger.error("Parquets were not found")
