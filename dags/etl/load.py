from sys import exit as sysexit
from pyspark.sql import DataFrame, SparkSession
from sqlalchemy.engine import Engine
import pandas as pd

# Create table and insert dataframe in sqlite
def df_to_sql(df: DataFrame, engine: Engine):
    try:
        print("Converting Spark dataframe to Pandas dataframe...")
        pdf = df.toPandas()
        print("Converted!")
        print("Inserting dataframe into SQLite database...")
        pdf.to_sql(name='cc_records', con=engine.raw_connection(), if_exists='replace')
        print("Dataframe successfully inserted!")
    except Exception as e:
        sysexit(f"error loading dataframe into sqlite: {e}")

def write_df(df: DataFrame | pd.DataFrame, spark: SparkSession, format: str, path: str) -> None:
    try:
        if isinstance(df, pd.DataFrame):
            print("Converting Pandas dataframe to PySpark dataframe...")
            df = spark.createDataFrame(df)
            print("Done converting!")
        print(f"Inserting dataframe to {path}...")
        df.write.format(format).option('header', 'true').mode('append').save(path=path)
        print("Successfully inserted!")
    except Exception as e:
        sysexit(f"error inserting dataframe in {path}: {e}")