from os import path
from sys import exit as sysexit
import pandas as pd

def read_csv(csv):
    try:
        with pd.read_csv(csv) as df:
            return df
    except Exception as e:
        sysexit(f"error reading the file: {e}")