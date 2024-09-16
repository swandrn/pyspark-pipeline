from sys import exit as sysexit
from etl import sparkenv
from pyspark.sql import DataFrame, SparkSession
import requests
import threading
from queue import Queue
import boto3
from botocore.exceptions import ClientError

MAX_WORKERS = 3
consumers = threading.BoundedSemaphore(MAX_WORKERS)

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

def call_random_user(url: str, q: Queue, retry: bool = False):
    try:
        with consumers:
            resp = requests.get(url)
            if not resp.status_code == 200:
                resp.raise_for_status()
            users_json = resp.json()["results"]
            q.put(users_json)
    except requests.Timeout as e:
        if retry == True:
            q.put(f'error fetching {url}: {e}')
        print('a timeout error occured, retrying...')
        call_random_user(url=url, q=q, retry=True)
    except Exception as e:
        q.put(f'error fetching {url}: {e}')

def s3_key_exists(bucket: str, key: str) -> bool:
    session = boto3.Session(aws_access_key_id=sparkenv.ACCESS_KEY_ID,
                            aws_secret_access_key=sparkenv.SECRET_ACCESS_KEY,
                            )
    s3 = session.resource('s3',
                        region_name='eu-north-1',
                        )
    try:
        s3.Object(bucket, key).load()
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            sysexit(f'error fetching {key} in {bucket}: {e}')