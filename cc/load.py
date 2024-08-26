from sys import exit as sysexit
from sqlalchemy import create_engine

#Create database, table, and insert dataframe
def df_to_sql(df):
    try:
        print("Attempting to create SQLite database...")
        engine = create_engine(f"sqlite:///credit_cards.db")
        print("SQLite database successfully created!")
        print("Inserting dataframe into SQLite database...")
        df.to_sql(name='cc_records', con=engine.raw_connection(), if_exists='replace')
        print("Dataframe successfully inserted!")
    except Exception as e:
        sysexit(f"error loading dataframe into sqlite: {e}")