from sys import exit as sysexit
from sqlalchemy import create_engine

# Create database at root of project dir
def create_db(db_name: str):
    try:
        print(f"Attempting to create SQLite database {db_name}...")
        engine = create_engine(f"sqlite:///{db_name}.db")
        print("SQLite database successfully created!")
        return engine
    except Exception as e:
        sysexit(f"error creating the sqlite database {db_name}: {e}")

# Create table and insert dataframe in sqlite
def df_to_sql(df, engine):
    try:
        print("Inserting dataframe into SQLite database...")
        df.to_sql(name='cc_records', con=engine.raw_connection(), if_exists='replace')
        print("Dataframe successfully inserted!")
    except Exception as e:
        sysexit(f"error loading dataframe into sqlite: {e}")