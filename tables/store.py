from tables.table import Table
from src.config import store_path


class Store(Table):
    def __init__(self, spark):
        columns = [
            "id",
            "name",
            "city"
        ]
        super().__init__(spark, store_path, columns)
