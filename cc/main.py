import paths
import extract as e
import transform as t
import load as l
from sqlalchemy import create_engine

df = e.read_csv(paths.RAW_CSV)
df = t.drop_columns(df, "Unnamed: 0", "lat", "long", "city_pop", "merch_lat", "merch_long")

db_name = "credit_cards"
print(f"Attempting to create SQLite database {db_name}...")
engine = create_engine(f"sqlite:///{db_name}.db")
print("SQLite database successfully created!")
l.df_to_sql(df, engine)
print("Done")