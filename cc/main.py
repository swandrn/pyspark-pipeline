import paths
import extract as e
import transform as t
import load as l
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
import os

os.environ["SPARK_CLASSPATH"] = paths.SQLITE_JAR

print("Creating a Spark session...")
spark = SparkSession.builder \
.appName("credit_cards") \
.getOrCreate()
print("Created!")

df = e.read_csv(spark, paths.RAW_CSV)
df.show(10)
df = t.drop_columns(df, "Unnamed: 0", "lat", "long", "city_pop", "merch_lat", "merch_long")

db_name = "credit_cards"
print(f"Attempting to create SQLite database {db_name}...")
engine = create_engine(f"sqlite:///{db_name}.db")
print("SQLite database successfully created!")
l.df_to_sql(df, engine)
print("Done")

spark.stop()