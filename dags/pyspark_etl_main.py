from etl import paths
from dags.etl import awsenv
from etl import extract as e
from etl import transform as t
from etl import load as l
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
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", awsenv.ACCESS_KEY_ID)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", awsenv.SECRET_ACCESS_KEY)
spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com")
print("Configurations set!")

df = e.read_csv(spark=spark, csv=paths.RAW_CSV)
df = df.limit(1000)

df = t.drop_columns(df, 'Unnamed: 0', 'lat', 'long', 'city_pop', 'merch_lat', 'merch_long')

l.write_df(df=df, spark=spark, format='csv', path=paths.DEST_CSV)

spark.stop()