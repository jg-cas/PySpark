import DataLoader.main.Utils.Common as Utils
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql import SparkSession


def execute(spark: SparkSession,
            path: str,
            header: bool = True,
            infer_schema: bool = True,
            delimiter: str = ','):
    raw_df = spark.read \
        .option("header", header) \
        .option("inferSchema", infer_schema) \
        .option("delimiter", delimiter) \
        .csv(path)
    df: DataFrame = Utils.CommonUtils.clean_cols(raw_df)

    print(f"\n\n---------- DataFrame rows : {Utils.CommonUtils.count_rows(df)} ----------")
    print(f"\n---------- DataFrame columns : {Utils.CommonUtils.count_columns(df)} ----------\n")

    for field in df.schema.fields:
        if isinstance(field.dataType, DoubleType):
            summary = Utils.CommonUtils.get_summary_table(df, field.name)
            summary.show()
        elif isinstance(field.dataType, StringType):
            frequency = Utils.CommonUtils.get_frequency_table(df, field.name)
            frequency.show()
