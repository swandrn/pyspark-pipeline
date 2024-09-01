from sys import exit as sysexit
from pyspark.sql import DataFrame

# Create table and insert dataframe in sqlite
def df_to_sql(df: DataFrame, engine):
    try:
        print("Converting Spark dataframe to Pandas dataframe...")
        pdf = df.toPandas()
        print("Converted!")
        print("Inserting dataframe into SQLite database...")
        pdf.to_sql(name='cc_records', con=engine.raw_connection(), if_exists='replace')
        print("Dataframe successfully inserted!")
    except Exception as e:
        sysexit(f"error loading dataframe into sqlite: {e}")