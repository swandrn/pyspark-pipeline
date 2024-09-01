from sys import exit as sysexit
from pyspark.sql import DataFrame, SparkSession

def read_csv(spark: SparkSession, csv: str) -> DataFrame:
    try:
        if not csv.endswith('.csv'):
            raise ValueError("only .csv files are allowed")
        print("Attempting to insert CSV into dataframe...")
        df = spark.read.csv(csv, header=True, inferSchema=True)
        print("CSV successfully inserted!")
        return df
    except Exception as e:
        sysexit(f"error reading the file: {e}")