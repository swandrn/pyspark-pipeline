from sys import exit as sysexit
from pyspark.sql import DataFrame, SparkSession
import requests

def read_csv(spark: SparkSession, csv: str) -> DataFrame:
    try:
        if not csv.endswith('.csv'):
            raise ValueError("only .csv files are allowed")
        print("Attempting to insert CSV into dataframe...")
        df = spark.read.csv(csv, header=True, inferSchema=True)
        print("CSV successfully inserted!")
        return df
    except Exception as e:
        sysexit(f"error reading the file: {e}")

def call_random_user(url: str, retry: bool = False):
    try:
        resp = requests.get(url)
        if not resp.status_code == 200:
            resp.raise_for_status()
        user_json = resp.json()["results"][0]
        return user_json
    except requests.Timeout as e:
        if retry == True:
            sysexit(f'error fetching {url}: {e}')
        print('a timeout error occured, retrying...')
        call_random_user(url=url, retry=True)
    except Exception as e:
        sysexit(f'error fetching {url}: {e}')