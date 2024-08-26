from sys import exit as sysexit

def drop_columns(df, *columns : str):
    for column in list(columns):
        if not isinstance(column, str):
            sysexit("drop_columns(*columns) only takes string type as argument")
    try:
        print(f"Dropping columns {columns}...")
        df = df.drop(columns=list(columns))
        print("Successfully dropped columns!")
        return df
    except Exception as e:
        sysexit(f"error dropping columns: {e}")