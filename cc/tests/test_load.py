import unittest
from cc import extract
from cc import transform
from cc import load
from cc import paths
from pathlib import Path
from pyspark.sql import SparkSession
import pandas as pd
from sqlalchemy import create_engine

class TestExtract(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        print("Creating a Spark session...")
        self.spark = SparkSession.builder.appName("credit_cards").getOrCreate()
        print("Created!")
        self.df = extract.read_csv(self.spark, paths.RAW_CSV).limit(20)

    def setUp(self) -> None:
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

        
    def tearDown(self) -> None:
        Path.unlink(r"test_credit_cards.db", missing_ok=True)

if __name__ == '__main__':
    unittest.main()