class ETLProcess:
    def __init__(self, spark, table_paths: dict):
        self.spark = spark
        self.table_paths = table_paths

    def valide_tables(self):
        result = True
        for table_path in self.table_paths:
            try:
                self.spark.read.parquet(table_path).limit(1).collect()
            except:
                # Нужно закинуть в лог
                result = False
        return result

    def write_parquet(self, df, path):
        try:
            df.write.mode('overwrite').parquet(path)
        except:
            # Нужно закинуть в лог
            pass

    def create_TempView(self, table_path, table_name):
        try:
            df = self.spark.read.parquet(table_path)
            df.createTempView(table_name)
            # Нужно закинуть в лог
        except:
            # Нужно закинуть в лог
            pass

    def run(self, sql_paths=[]):
        if self.valide_tables():
            for table_path, table_name in self.table_paths.items():
                self.create_TempView(table_path, table_name)
            # Здесь будет код для запуска ETL
            for sql_path in sql_paths:
                with open(sql_path, 'r') as sql_file:
                    sql = sql_file.read()
                    df = self.spark.sql(sql)
                    self.write_parquet(
                        df, sql_path.split('/')[-1].split('.')[0])
                    # Нужно закинуть в лог
        else:
            # Нужно закинуть в лог
            pass
