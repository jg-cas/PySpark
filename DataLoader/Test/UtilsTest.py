import unittest
import os
from DataLoader.main.Utils import Common as Utils
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession


class UtilsTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

        cls.spark: SparkSession = SparkSession.builder.appName("UtilsTest").getOrCreate()
        # Arrange
        input_schema = StructType([
            StructField('Test.1', IntegerType(), True),
            StructField('Test.2', IntegerType(), True),
            StructField('Test.1.2', IntegerType(), True),
            StructField('Test.test', StringType(), True)
        ])
        input_data = [
            (1, 2, 3, "test1"),
            (4, 5, 6, "test2"),
            (7, 8, 9, "test3"),
        ]
        cls.input_df = cls.spark.createDataFrame(data=input_data, schema=input_schema)

    def test_clean_cols(self):
        # Arrange
        expected_schema = StructType([
            StructField('Test_1', IntegerType(), True),
            StructField('Test_2', IntegerType(), True),
            StructField('Test_1_2', IntegerType(), True),
            StructField('Test_test', StringType(), True)
        ])
        # Act
        transformed_df: DataFrame = Utils.CommonUtils.clean_cols(self.input_df)

        # Assert
        self.assertEqual(expected_schema, transformed_df.schema)

    def test_count_rows(self):
        # Arrange
        expected_rows = 3

        # Act
        cleaned_df = Utils.CommonUtils.clean_cols(self.input_df)
        actual_rows = Utils.CommonUtils.count_rows(cleaned_df)

        # Assert
        self.assertEqual(expected_rows, actual_rows)

    def test_count_columns(self):
        # Arrange
        expected_cols = 4

        # Act
        cleaned_df = Utils.CommonUtils.clean_cols(self.input_df)
        actual_cols = Utils.CommonUtils.count_columns(cleaned_df)

        # Assert
        self.assertEqual(expected_cols, actual_cols)

    def test_summary_table(self):
        # Arrange
        expected_schema = StructType([
            StructField('summary', StringType(), True),
            StructField('Test_1', StringType(), True)
        ])
        expected_data = [
            ("mean", 4.0),
            ("min", 1),
            ("max", 7)
        ]
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        # Act
        cleaned_df = Utils.CommonUtils.clean_cols(self.input_df)
        transformed_df = Utils.CommonUtils.get_summary_table(cleaned_df, 'Test_1')

        common_fields = lambda fields: (fields.name, fields.dataType, fields.nullable)
        expected_fields = [*map(common_fields, expected_df.schema.fields)]
        transformed_fields = [*map(common_fields, transformed_df.schema.fields)]

        res = set(expected_fields) == set(transformed_fields)

        # Assert
        self.assertTrue(res)
        self.assertEqual(expected_df.collect(), transformed_df.collect())

    def test_frequency_table(self):
        # Arrange
        expected_schema = StructType([
            StructField('Test_test', StringType(), True),
            StructField('count', LongType(), False)
        ])
        expected_data = [
            ("test1", 1),
            ("test2", 1),
            ("test3", 1)
        ]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        # Act
        cleaned_df = Utils.CommonUtils.clean_cols(self.input_df)
        transformed_df = Utils.CommonUtils.get_frequency_table(cleaned_df, 'Test_test')

        common_fields = lambda fields: (fields.name, fields.dataType, fields.nullable)
        expected_fields = [*map(common_fields, expected_df.schema.fields)]
        transformed_fields = [*map(common_fields, transformed_df.schema.fields)]

        res = set(expected_fields) == set(transformed_fields)

        # Assert
        self.assertTrue(res)
        self.assertEqual(expected_df.collect(), transformed_df.collect())

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.sparkContext.stop()


if __name__ == '__main__':
    unittest.main()
