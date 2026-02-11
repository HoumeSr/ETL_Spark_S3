from tables.table import Table
from src.config import user_path


class User(Table):
    def __init__(self, spark):
        columns = [
            "id",
            "name",
            "phone",
            "created_at"
        ]
        super().__init__(spark, user_path, columns)
