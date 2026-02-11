
import unittest
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, IntegerType, StringType, StructType, TimestampType
from datetime import datetime

sys.path.insert(0, '/opt/spark')
sys.path.insert(0, '/opt/spark/src')
from config import result_sql


class TestTopStores(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .appName("ETL_Test")
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.create_test_data()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @classmethod
    def create_test_data(cls):
        store_data = [
            (1, "Peterochka", "Moscow"),
            (2, "Magnit", "Moscow"),
            (3, "Perecrestok", "Moscow"),
            (4, "Metro", "London"),
            (5, "Ashan", "London"),
            (6, "Lenta", "Berlin"),
            (7, "Verniy", "Berlin")
        ]

        user_data = [
            (1, "Ivan", "+1111111", datetime(2025, 1, 15, 10, 30, 0)),
            (2, "Vacaya", "+2222222", datetime(2025, 3, 20, 14, 45, 0)),
            (3, "Vladimir", "+3333333", datetime(2025, 6, 10, 9, 15, 0)),
            (4, "Gleb", "+4444444", datetime(2024, 12, 31, 23, 59, 59)),
            (5, "Rodion", "+5555555", datetime(2026, 1, 1, 0, 0, 1))
        ]

        order_data = [
            (1, 1000, 1, 1, "complete", datetime(2025, 2, 1)),
            (2, 500, 1, 1, "complete", datetime(2025, 2, 15)),
            (3, 1500, 2, 1, "complete", datetime(2025, 4, 1)),
            (4, 800, 1, 2, "complete", datetime(2025, 3, 1)),
            (5, 1200, 3, 2, "complete", datetime(2025, 7, 1)),
            (6, 3000, 2, 3, "complete", datetime(2025, 5, 1)),
            (7, 2000, 3, 3, "complete", datetime(2025, 8, 1)),
            (8, 700, 1, 4, "complete", datetime(2025, 6, 1)),
            (9, 900, 2, 4, "complete", datetime(2025, 9, 1)),
            (10, 600, 3, 5, "complete", datetime(2025, 10, 1)),
            (11, 400, 1, 5, "complete", datetime(2025, 11, 1)),
            (12, 1300, 2, 6, "complete", datetime(2025, 12, 1)),
            (13, 1100, 3, 7, "complete", datetime(2025, 12, 15)),
            (14, 9999, 4, 1, "complete", datetime(2025, 1, 1)),
            (15, 8888, 5, 2, "complete", datetime(2025, 1, 1)),
        ]

        store_schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("city", StringType(), nullable=False)
        ])

        user_schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("phone", StringType(), nullable=False),
            StructField("created_at", TimestampType(), nullable=False)
        ])

        order_schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("amount", IntegerType(), nullable=False),
            StructField("user_id", IntegerType(), nullable=False),
            StructField("store_id", IntegerType(), nullable=False),
            StructField("status", StringType(), nullable=False),
            StructField("created_at", TimestampType(), nullable=False)
        ])

        cls.store_df = cls.spark.createDataFrame(store_data, store_schema)
        cls.user_df = cls.spark.createDataFrame(user_data, user_schema)
        cls.order_df = cls.spark.createDataFrame(order_data, order_schema)

        cls.store_df.createOrReplaceTempView("store")
        cls.user_df.createOrReplaceTempView("user")
        cls.order_df.createOrReplaceTempView("order")

        expected_data = [
            ("Moscow", "Perecrestok", 5000),
            ("Moscow", "Peterochka", 3000),
            ("Moscow", "Magnit", 2000),
            ("London", "Metro", 1600),
            ("London", "Ashan", 1000),
            ("Berlin", "Lenta", 1300),
            ("Berlin", "Verniy", 1100)
        ]

        expected_schema = StructType([
            StructField("city", StringType(), nullable=False),
            StructField("store_name", StringType(), nullable=False),
            StructField("target_amount", IntegerType(), nullable=False)
        ])

        cls.expected_df = cls.spark.createDataFrame(
            expected_data, expected_schema)

    def test_sql(self):
        self.assertTrue(os.path.exists(result_sql))

        with open(result_sql, 'r') as sql_file:
            sql = sql_file.read()

        result_df = self.spark.sql(sql)

        expected_count = self.expected_df.count()
        actual_count = result_df.count()
        self.assertEqual(actual_count, expected_count)

        expected_columns = ["city", "store_name", "target_amount"]
        actual_columns = result_df.columns

        for column in expected_columns:
            self.assertIn(column, actual_columns)

        result_sorted = result_df.orderBy(
            "city", "target_amount", ascending=False).collect()
        expected_sorted = self.expected_df.orderBy(
            "city", "target_amount", ascending=False).collect()

        for i, (result_row, expected_row) in enumerate(zip(result_sorted, expected_sorted)):
            with self.subTest(row=i):
                self.assertEqual(result_row.city, expected_row.city)
                self.assertEqual(result_row.store_name,
                                 expected_row.store_name)
                self.assertEqual(result_row.target_amount,
                                 expected_row.target_amount)

if __name__ == '__main__':
    unittest.main()