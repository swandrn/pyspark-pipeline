from sys import exit as sysexit
from pyspark.sql import DataFrame, SparkSession
import requests
import threading
from queue import Queue

MAX_WORKERS = 5
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
            user_json = resp.json()["results"][0]
            q.put(user_json)
    except requests.Timeout as e:
        if retry == True:
            q.put(f'error fetching {url}: {e}')
        print('a timeout error occured, retrying...')
        call_random_user(url=url, q=q, retry=True)
    except Exception as e:
        q.put(f'error fetching {url}: {e}')

# threads = []
# users_json = []

# for i in range(10):
#     threads.append(threading.Thread(target=call_random_user, args=['https://randomuser.me/api/1.4/?exc=id']))

# for thread in threads:
#     thread.start()
#     users_json.append(q.get())

# for thread in threads:
#     thread.join()