from sys import exit as sysexit
from pyspark.sql import DataFrame
import pandas as pd

def drop_columns(df: DataFrame, *columns : str) -> DataFrame:
    try:
        if not len(columns) == 0:
            for column in list(columns):
                if not isinstance(column, str):
                    raise TypeError("*columns must only contain strings")
            print(f"Dropping columns {columns}...")
            df = df.drop(*list(columns))
            print("Successfully dropped columns!")
        else:
            print("No column to drop")
        return df
    except Exception as e:
        sysexit(f"error dropping columns: {e}")

def user_json_to_df(json) -> pd.DataFrame:
    try:
        json = pd.json_normalize(json, sep="_")
        df = pd.DataFrame.from_dict(json, orient='columns')
        df.rename(columns={'nat': 'nationality',
                        'name_first': 'first_name',
                        'name_last': 'last_name',
                        'location_street_number': 'street_number',
                        'location_street_name': 'street_name',
                        'location_city': 'city',
                        'location_state': 'state',
                        'location_country': 'country',
                        'location_postcode': 'postcode',
                        'location_timezone_offset': 'timezone_offset',
                        'location_timezone_description': 'timezone_description',
                        'location_coordinates_longitude': 'long',
                        'location_coordinates_latitude': 'lat',
                        'login_username': 'username',
                        'login_password': 'password',
                        'dob_date': 'dob',
                        'dob_age': 'age',
                        'registered_date': 'registration_date',
                        'registered_age': 'account_age'},
                        inplace=True),
        return df
    except Exception as e:
        sysexit(f"error converting json to datafram: {e}")