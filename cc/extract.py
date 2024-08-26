from os import path
from sys import exit as sysexit
import pandas as pd

def read_csv(csv):
    try:
        print("Attempting to insert CSV into dataframe...")
        df = pd.read_csv(csv)
        print("CSV successfully inserted!")
        print("Done")
        return df
    except Exception as e:
        sysexit(f"error reading the file: {e}")