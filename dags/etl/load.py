from sys import exit as sysexit
from dags.etl import awsenv
from pyspark.sql import DataFrame, SparkSession
from sqlalchemy.engine import Engine
import pandas as pd
from io import StringIO
import boto3

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

def pandas_df_to_s3(df: pd.DataFrame, bucket: str, key: str):
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, header=True)

        session = boto3.Session(aws_access_key_id=awsenv.ACCESS_KEY_ID,
                                aws_secret_access_key=awsenv.SECRET_ACCESS_KEY,
                                )
        s3 = session.resource('s3',
                            region_name='eu-north-1',
                            )
        s3.Object(bucket, key).put(Body=csv_buffer.getvalue())
    except Exception as e:
        sysexit(f"error writing pandas dataframe to s3: {e}")