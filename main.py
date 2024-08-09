#!/usr/bin/env python
import os
import requests
from requests.exceptions import HTTPError
import json
import csv
from pathlib import Path
from dotenv import load_dotenv

dotenv_path = Path('.env/.venv')
load_dotenv(dotenv_path=dotenv_path)

# mastodon access token
api_key = os.getenv('API_KEY')

def main():
    try:

        # date to timestamp

        response = requests.get('https://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=radioheadve&api_key=' + api_key + '&from=1722470400&to=1722556799&format=json')
        #print(response.text)

        response.raise_for_status()
        # access JSOn content
        json_response = response.json()

        pages = int(json_response['recenttracks']['@attr']['totalPages'])
        i = 0
        while i < pages:
            print(i)
            i += 1
            get_tracks(start, end, page=i)
            


    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
    # pagination logic for > page
    

def get_tracks(start, end, page):
    
    try:
        # request
        response = requests.get('https://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=radioheadve&api_key=' + api_key + '&from=1722470400&to=1722556799&format=json')
    
        # return string csv format
        
    
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
    
    return True


def warehouse():

    from pyiceberg.catalog.sql import SqlCatalog
    import pyarrow.parquet as pq
    import pyarrow.compute as pc

    warehouse_path = "./warehouse"

    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )

    catalog.create_namespace("default")

    df = pq.read_table("./yellow_tripdata_2023-01.parquet")

    table = catalog.create_table(
        "default.taxi_dataset",
        schema=df.schema
    ) 

    # create a physical table in warehouse folder? or point to file? 
    table.append(df)

    len(table.scan().to_arrow())

    # will this instruction add a new column to parquet file?
    df = df.append_column("tip_per_mile", pc.divide(df["tip_amount"], df["trip_distance"]))

    with table.update_schema() as update_schema:
        update_schema.union_by_name(df.schema)    

    table.overwrite(df)

    # print(table.scan().to_arrow())

    df = table.scan(row_filter="tip_per_mile > 0").to_arrow()

    len(df)
   

def query():

    from pyiceberg.catalog.sql import SqlCatalog
    import pyarrow.parquet as pq
    import pyarrow.compute as pc


    warehouse_path = "./warehouse"

    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )  


    table = catalog.load_table(('default', 'taxi_dataset'))

    #print(table.describe())

    con = table.scan().to_duckdb(table_name="taxi_dataset")


    con.sql(
        "SELECT * FROM taxi_dataset LIMIT 10"
    ).show()
      
    

if __name__ == '__main__':
    main()
    #query()