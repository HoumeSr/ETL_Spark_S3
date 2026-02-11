from tables.table import Table
from src.config import user_path


class Order(Table):
    def __init__(self, spark):
        columns = [
            "id",
            "amount",
            "user_id",
            "store_id",
            "status",
            "created_at"
        ]
        super().__init__(spark, user_path, columns)
