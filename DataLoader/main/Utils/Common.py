from pyspark.sql.functions import DataFrame


# Single Responsibility Principle for testing
class CommonUtils:

    @staticmethod
    def clean_cols(df: DataFrame) -> DataFrame:
        cols = (columns.replace('.', '_') for columns in df.columns)
        result = df.toDF(*cols)
        return result

    @staticmethod
    def count_rows(df: DataFrame) -> int:
        return df.count()

    @staticmethod
    def count_columns(df: DataFrame) -> int:
        return len(df.columns)

    @staticmethod
    def get_summary_table(df: DataFrame, cols: str) -> DataFrame:
        return df.select(cols).summary('mean', 'min', 'max')

    @staticmethod
    def get_frequency_table(df: DataFrame, cols: str) -> DataFrame:
        return df.groupby(cols).count()
