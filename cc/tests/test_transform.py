import unittest
from cc import extract
from cc import transform
from cc import paths
from pyspark.sql import SparkSession

class TestExtract(unittest.TestCase):

    @classmethod
    def setUpClass(self) -> None:
        print("Creating a Spark session...")
        self.spark = SparkSession.builder.appName("credit_cards").getOrCreate()
        print("Created!")
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