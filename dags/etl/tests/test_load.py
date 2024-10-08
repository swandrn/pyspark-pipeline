import unittest
from dags.etl import awsenv
from dags.etl import extract
from dags.etl import transform
from dags.etl import load
from dags.etl import paths
from pathlib import Path
from pyspark.sql import SparkSession
import pandas as pd
from sqlalchemy import create_engine

class TestToSql(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        print("Creating a Spark session...")  
        self.spark = SparkSession.builder \
        .master("local[*]") \
        .appName("credit_cards") \
        .config('spark.local.dir', '/tmp/spark-temp') \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.6,org.apache.hadoop:hadoop-common:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.367') \
        .config('spark.driver.memory', '2g') \
        .config('spark.network.timeout', '36000s') \
        .config('spark.executor.heartbeatInterval', '3600s') \
        .getOrCreate()
        print("Created!")

        self.spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", awsenv.ACCESS_KEY_ID)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", awsenv.SECRET_ACCESS_KEY)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com")

        self.df = extract.read_csv(self.spark, paths.TEST_CSV).limit(20)

    def setUp(self):
        db_name = "test_credit_cards"
        print(f"Attempting to create SQLite database {db_name}...")
        self.engine = create_engine(f"sqlite:///{db_name}.db")
        print("SQLite database successfully created!")

    def test_df_to_sql(self):
        df = self.df
        load.df_to_sql(df, self.engine)
        with self.engine.connect() as conn:
            res_pdf = pd.read_sql("SELECT * FROM cc_records", con=conn.connection)
            res_df = self.spark.createDataFrame(res_pdf)
            res_df = transform.drop_columns(res_df, "index")
            # Compare both dataframes
            pandas_old = df.toPandas()
            pandas_new = res_df.toPandas()
            pandas_new.equals(pandas_old)

    def test_df_to_sql_none(self):
        df = self.df
        with self.assertRaises(SystemExit):
            load.df_to_sql(df, None)

        
    def tearDown(self):
        Path.unlink(r"test_credit_cards.db", missing_ok=True)

    @classmethod
    def tearDownClass(self):
        self.spark.stop()

class TestWrite(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        print("Creating a Spark session...")  
        self.spark = SparkSession.builder \
        .master("local[*]") \
        .appName("credit_cards") \
        .config('spark.local.dir', '/tmp/spark-temp') \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.6,org.apache.hadoop:hadoop-common:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.367') \
        .config('spark.driver.memory', '2g') \
        .config('spark.network.timeout', '36000s') \
        .config('spark.executor.heartbeatInterval', '3600s') \
        .getOrCreate()
        print("Created!")

        self.spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", awsenv.ACCESS_KEY_ID)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", awsenv.SECRET_ACCESS_KEY)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com")

        self.df = extract.read_csv(self.spark, paths.TEST_CSV).limit(20)

    def test_write_df_as_csv(self):
        df = self.df
        load.write_df(df=df, spark=self.spark, format='csv', path=paths.DEST_TEST)
    
    def test_write_df_as_json(self):
        df = self.df
        load.write_df(df=df, spark=self.spark, format='json', path=paths.DEST_TEST)
    
    def test_write_df_as_parquet(self):
        df = self.df
        load.write_df(df=df, spark=self.spark, format='parquet', path=paths.DEST_TEST)

    def test_write_df_pandas(self):
        df = self.df
        df = df.toPandas()
        load.write_df(df=df, spark=self.spark, format='csv', path=paths.DEST_TEST)
    
    @classmethod
    def tearDownClass(self):
        self.spark.stop()

if __name__ == '__main__':
    unittest.main()