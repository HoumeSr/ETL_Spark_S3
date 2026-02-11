from tables.table import Table
from src.config import user_path


class User(Table):
    def __init__(self, spark):
        columns = [
            "id",
            "name",
            "city"
        ]
        super().__init__(spark, user_path, columns)
