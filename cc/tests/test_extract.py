import unittest
from cc import extract
from cc import paths
from pyspark.sql import SparkSession, DataFrame


class TestExtract(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        print("Creating a Spark session...")
        self.spark = SparkSession.builder.appName("credit_cards").getOrCreate()
        print("Created!")

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

if __name__ == '__main__':
    unittest.main()