from os import path
from sys import exit as sysexit
import pandas as pd

def read_csv(csv):
    try:
        df = pd.read_csv(csv)
        return df
    except Exception as e:
        sysexit(f"error reading the file: {e}")