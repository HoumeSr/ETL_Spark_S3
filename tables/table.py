class Table:
    def __init__(self, spark, path: str, columns: list):
        self.spark = spark
        self.path = path
        self.columns = columns
        self.df = None

    def open_table(self):
        self.df = self.spark.read.parquet(self.path)
        return self.df

    def save_table(self, mode):
        self.df.write.mode(mode).parquet(self.path)

    def add_rows(self, rows):
        appended_df = self.spark.createDataFrame(rows, self.columns)
        if self.df is None:
            self.df = appended_df
        else:
            self.df = self.df.union(appended_df)

    def createTempView(self):
        self.df.createTempView(self.path.split('/')[-1])

    def show(self):
        self.df.show()

    def drop(self):
        self.df.drop()
