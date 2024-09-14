from sys import exit as sysexit
from pyspark.sql import DataFrame
from sqlalchemy.engine import Engine

# Create table and insert dataframe in sqlite
def df_to_sql(df: DataFrame, engine: Engine):
    try:
        print("Converting Spark dataframe to Pandas dataframe...")
        pdf = df.toPandas()
        print("Converted!")
        print("Inserting dataframe into SQLite database...")
        pdf.to_sql(name='cc_records', con=engine.raw_connection(), if_exists='replace')
        print("Dataframe successfully inserted!")
    except Exception as e:
        sysexit(f"error loading dataframe into sqlite: {e}")

def write_df(df: DataFrame, format: str, path: str) -> None:
    try:
        print(f"Inserting dataframe to {path}...")
        df.write.format(format).option('header', 'true').mode('overwrite').save(path=path)
        print("Successfully inserted!")
    except Exception as e:
        sysexit(f"error inserting dataframe in {path}: {e}")