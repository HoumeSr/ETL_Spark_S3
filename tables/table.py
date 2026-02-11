class Table:
    def __init__(self, spark, path: str, columns: list):
        self.spark = spark
        self.path = path
        self.columns = columns

        self.df = self._open_table()

    def _open_table(self):
        self.df = self.spark.read.parquet(self.path)

    def save_table(self, mode):
        self.df.write.mode(mode).parquet(self.path)

    def add_rows(self, rows: list[list]):
        appended_df = self.spark.createDataFrame(rows, self.columns)
        self.df = self.df.union(appended_df)

    def createTempView(self):
        self.df.createTempView(self.path.split('/')[-1])

    def show(self):
        self.df.show()

    def drop(self):
        self.df.drop()
