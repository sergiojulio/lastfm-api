#!/usr/bin/env python
import os
import requests
from pathlib import Path
from dotenv import load_dotenv

dotenv_path = Path('.env/.venv')
load_dotenv(dotenv_path=dotenv_path)

# mastodon access token
api_key = os.getenv('API_KEY')

def main():
    x = requests.get('https://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=radioheadve&api_key=' + api_key + '&format=json')
    print(x.text)

    



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
    

if __name__ == '__main__':
    #main()
    warehouse()