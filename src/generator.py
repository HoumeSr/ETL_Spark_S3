from random import randint
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from config import order_path, store_path, user_path
from datetime import datetime


class Generator:
    def __init__(self, spark, user_count=100, store_count=100, order_count=1000):
        self.spark = spark
        self.user_count = user_count
        self.store_count = store_count
        self.order_count = order_count

    # def write_parquet(self, df, path):
    #     try:
    #         df.write.mode('overwrite').parquet(path)
    #     except:
    #         # Нужно закинуть в лог
    #         pass

    def create_store_parquet(self):
        try:
            store_name = ["TopShop", "Product", "IngeniousStoreName",
                          "Apple", "Metro", "Ashan", "Hello"]
            cities = ["Moscow", "Washington", "Berlin", "London",
                      "Paris", "Madrid", "Oslo", "Sofia", "Astana"]

            store_schema = StructType([
                StructField("id", IntegerType(), nullable=False),
                StructField("name", StringType(), nullable=False),
                StructField("city", StringType(), nullable=False)
            ])
            store_list = []
            for i in range(1, self.store_count + 1):
                store_list.append(  # Условия уникальности
                    (
                        i,  # id
                        store_name[randint(0, len(store_name) - 1)],  # name
                        cities[randint(0, len(cities) - 1)]  # city
                    )
                )
            df_store = self.spark.createDataFrame(
                store_list, schema=store_schema)
            df_store.write.mode('overwrite').parquet(store_path)

        except Exception as e:
            print("create_order_parquet error:", e)
            raise

    def create_user_parquet(self):
        try:
            created_at = [
                datetime(2025, 1, 1, 0, 0, 0),
                datetime(2026, 6, 5, 4, 3, 1),
                datetime(2022, 6, 2, 5, 3, 2),
                datetime(2025, 10, 2, 3, 5, 2)
            ]
            user_schema = StructType([
                StructField("id", IntegerType(), nullable=False),
                StructField("name", StringType(), nullable=False),
                StructField("phone", StringType(), nullable=False),
                StructField("created_at", TimestampType(), nullable=False)
            ])
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
            df_user = self.spark.createDataFrame(
                user_list, schema=user_schema)
            df_user.write.mode('overwrite').parquet(user_path)

        except Exception as e:
            print("create_user_parquet error:", e)
            raise

    def create_order_parquet(self):
        try:
            created_at = [
                datetime(2025, 1, 1, 0, 0, 0),
                datetime(2026, 6, 5, 4, 3, 1),
                datetime(2022, 6, 2, 5, 3, 2),
                datetime(2025, 10, 2, 3, 5, 2)
            ]
            order_schema = StructType([
                StructField("id", IntegerType(), nullable=False),
                StructField("amount", IntegerType(), nullable=False),
                StructField("user_id", IntegerType(), nullable=False),
                StructField("store_id", IntegerType(), nullable=False),
                StructField("status", StringType(), nullable=False),
                StructField("created_at", TimestampType(), nullable=False)
            ])
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
            df_order = self.spark.createDataFrame(
                order_list, schema=order_schema)
            df_order.write.mode('overwrite').parquet(order_path)
        except Exception as e:
            print("create_order_parquet error:", e)
            raise

    def run(self):
        try:
            self.create_user_parquet()
            self.create_store_parquet()
            self.create_order_parquet()
        except Exception as e:
            print("run error:", e)
            raise
