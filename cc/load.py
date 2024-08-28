from sys import exit as sysexit

# Create table and insert dataframe in sqlite
def df_to_sql(df, engine):
    try:
        print("Inserting dataframe into SQLite database...")
        df.to_sql(name='cc_records', con=engine.raw_connection(), if_exists='replace')
        print("Dataframe successfully inserted!")
    except Exception as e:
        sysexit(f"error loading dataframe into sqlite: {e}")