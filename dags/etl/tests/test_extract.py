import unittest
from dags.etl import sparkenv
from dags.etl import extract
from dags.etl import paths
from pyspark.sql import SparkSession, DataFrame


class TestExtract(unittest.TestCase):

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

        self.spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", sparkenv.ACCESS_KEY_ID)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", sparkenv.SECRET_ACCESS_KEY)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com")

    def test_read_csv(self):
        self.assertIsInstance(extract.read_csv(self.spark, paths.RAW_CSV), DataFrame)

    def test_read_csv_url(self):
        with self.assertRaises(SystemExit):
            extract.read_csv(self.spark, "https://www.wikipedia.org/")

    def test_read_csv_img(self):
        with self.assertRaises(SystemExit):
            extract.read_csv(self.spark, paths.IMG_FILE)
            

    def test_read_csv_mp4(self):
        with self.assertRaises(SystemExit):
            extract.read_csv(self.spark, paths.MP4_FILE)

    def test_read_csv_linux_path(self):
        self.assertIsInstance(extract.read_csv(self.spark, paths.RAW_CSV), DataFrame)

    def test_read_csv_int(self):
        with self.assertRaises(SystemExit):
            extract.read_csv(self.spark, 0)

    def test_read_csv_bool(self):
        with self.assertRaises(SystemExit):
            extract.read_csv(self.spark, True)

    def test_read_csv_none(self):
        with self.assertRaises(SystemExit):
            extract.read_csv(self.spark, None)

    def test_read_csv_string(self):
        with self.assertRaises(SystemExit):
            extract.read_csv(self.spark, "foo")

    @classmethod
    def tearDownClass(self):
        self.spark.stop()

if __name__ == '__main__':
    unittest.main()