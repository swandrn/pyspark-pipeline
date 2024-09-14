import unittest
from dags.etl import sparkenv
from dags.etl import extract
from dags.etl import transform
from dags.etl import paths
from pyspark.sql import SparkSession

class TestExtract(unittest.TestCase):

    @classmethod
    def setUpClass(self) -> None:
        print("Creating a Spark session...")  
        self.spark = SparkSession.builder \
        .master('local[*]') \
        .appName('credit_cards') \
        .config('spark.local.dir', '/tmp/spark-temp') \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.6,org.apache.hadoop:hadoop-common:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.367') \
        .config('spark.driver.memory', '2g') \
        .config('spark.network.timeout', '36000s') \
        .config('spark.executor.heartbeatInterval', '3600s') \
        .getOrCreate()
        print("Created!")

        self.spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", sparkenv.ACCESS_KEY_ID)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", sparkenv.SECRET_ACCESS_KEY)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com")

        self.df = extract.read_csv(self.spark, paths.RAW_CSV).limit(20)

    def test_drop_columns(self):
        df_old = self.df
        df_new = transform.drop_columns(df_old, 'lat')
        self.assertIsNot(df_new, df_old)
        self.assertEqual(len(df_new.columns), 23)
        self.assertNotIn("lat", list(df_new.columns))

    def test_drop_columns_int(self):
        df_old = self.df
        with self.assertRaises(SystemExit):
            transform.drop_columns(df_old, 0)
        
    def test_drop_columns_tuple(self):
        df_old = self.df
        with self.assertRaises(SystemExit):
            transform.drop_columns(df_old, ('lat', 'lon'))

    def test_drop_columns_wrong_column(self):
        df_old = self.df
        df_new = transform.drop_columns(df_old, "foo")
        pandas_old = df_old.toPandas()
        pandas_new = df_new.toPandas()
        pandas_new.equals(pandas_old)


    def test_drop_columns_none(self):
        df_old = self.df
        with self.assertRaises(SystemExit):
            transform.drop_columns(df_old, None)

    def test_drop_columns_noarg(self):
        df_old = self.df
        df_new = transform.drop_columns(df_old)
        pandas_old = df_old.toPandas()
        pandas_new = df_new.toPandas()
        pandas_new.equals(pandas_old)

    def test_drop_columns_nodf(self):
        with self.assertRaises(SystemExit):
            transform.drop_columns(None, 'lat')

    @classmethod
    def tearDownClass(self):
        print("Stopping PySpark...")
        self.spark.stop()
        print("Stopped!")

if __name__ == '__main__':
    unittest.main()