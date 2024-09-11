from cc import paths
from cc import sparkenv
from cc import extract as e
from cc import transform as t
from cc import load as l
from sqlalchemy import create_engine
from pyspark.sql import SparkSession

print("Creating a Spark session...")
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("credit_cards") \
    .config('spark.network.timeout', '36000s') \
    .config('spark.executor.heartbeatInterval', '3600s') \
    .getOrCreate()
print("Created!")

print("Setting AWS configurations...")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", sparkenv.ACCESS_KEY_ID)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", sparkenv.SECRET_ACCESS_KEY)
spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com")
print("Configurations set!")

df = e.read_csv(spark, paths.RAW_CSV)
df = df.limit(1000)

df = t.drop_columns(df, 'Unnamed: 0', 'lat', 'long', 'city_pop', 'merch_lat', 'merch_long')

db_name = 'credit_cards'
print(f"Attempting to create SQLite database {db_name}...")
engine = create_engine(f"sqlite:///{db_name}.db")
print("SQLite database successfully created!")
l.df_to_sql(df, engine)

spark.stop()