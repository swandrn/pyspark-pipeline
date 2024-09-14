from sys import exit as sysexit
from pyspark.sql import DataFrame

def drop_columns(df: DataFrame, *columns : str) -> DataFrame:
    try:
        if not len(columns) == 0:
            for column in list(columns):
                if not isinstance(column, str):
                    raise TypeError("*columns must only contain strings")
            print(f"Dropping columns {columns}...")
            df = df.drop(*list(columns))
            print("Successfully dropped columns!")
        else:
            print("No column to drop")
        return df
    except Exception as e:
        sysexit(f"error dropping columns: {e}")