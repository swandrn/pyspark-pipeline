import paths
import extract as e
import transform as t
import load as l

df = e.read_csv(paths.RAW_CSV)
df = t.drop_columns(df, "Unnamed: 0", "lat", "long", "city_pop", "merch_lat", "merch_long")
engine = l.create_db("credit_cards")
l.df_to_sql(df, engine)
print("Done")