import sys
import logging
from random import randint
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from config import order_path, store_path, user_path
sys.path.insert(0, '/opt/spark')
sys.path.insert(0, '/opt/spark/tables')
from tables.user import User
from tables.store import Store
from tables.order import Order


class Generator:
    def __init__(self, spark, user_count=100, store_count=100, order_count=1000):
        self.spark = spark
        self.user_count = user_count
        self.store_count = store_count
        self.order_count = order_count
        self.logger = logging.getLogger(__name__)

        self.logger.info(
            f"Initializing Generator: "
            f"user={user_count}, store={store_count}, order={order_count}"
        )

    def create_store_parquet(self):
        try:
            store_name = ["TopShop", "Product", "IngeniousStoreName",
                          "Apple", "Metro", "Ashan", "Hello"]
            cities = ["Moscow", "Washington", "Berlin", "London",
                      "Paris", "Madrid", "Oslo", "Sofia", "Astana"]

            store_list = []
            for i in range(1, self.store_count + 1):
                store_list.append(  # Условия уникальности
                    (
                        i,  # id
                        store_name[randint(0, len(store_name) - 1)],  # name
                        cities[randint(0, len(cities) - 1)]  # city
                    )
                )
            store_df = Store(self.spark)
            store_df.add_rows(store_list)
            store_df.save_table("overwrite")
            self.logger.info("table 'store' was created")

        except Exception as e:
            self.logger.error(f"Table 'store' creation failed: {e}")
            raise

    def create_user_parquet(self):
        try:
            created_at = [
                datetime(2025, 1, 1, 0, 0, 0),
                datetime(2026, 6, 5, 4, 3, 1),
                datetime(2022, 6, 2, 5, 3, 2),
                datetime(2025, 10, 2, 3, 5, 2)
            ]
            user_list = []
            for i in range(1, self.user_count + 1):
                user_list.append(  # Условия уникальности
                    (
                        i,  # id
                        "Name",  # name
                        "UserPhone",  # city
                        # created_at
                        created_at[randint(0, len(created_at) - 1)]
                    )
                )
            user_df = User(self.spark)
            user_df.add_rows(user_list)
            user_df.save_table("overwrite")
            self.logger.info("table 'user' was created")

        except Exception as e:
            self.logger.error(f"Table 'user' creation failed: {e}")
            raise

    def create_order_parquet(self):
        try:
            created_at = [
                datetime(2025, 1, 1, 0, 0, 0),
                datetime(2026, 6, 5, 4, 3, 1),
                datetime(2022, 6, 2, 5, 3, 2),
                datetime(2025, 10, 2, 3, 5, 2)
            ]
            order_list = []
            for i in range(1, self.order_count + 1):
                order_list.append(
                    (
                        i,  # id
                        randint(0, 1000),  # amount
                        randint(1, self.user_count),  # user_id
                        randint(1, self.store_count),  # store_id
                        "complete",  # status
                        # created_at
                        created_at[randint(0, len(created_at) - 1)]
                    ))
            order_df = Order(self.spark)
            order_df.add_rows(order_list)
            order_df.save_table("overwrite")
            self.logger.info("table 'order' was created")
        except Exception as e:
            self.logger.error(f"Table 'order' creation failed: {e}")
            raise

    def run(self):
        self.create_user_parquet()
        self.create_store_parquet()
        self.create_order_parquet()
