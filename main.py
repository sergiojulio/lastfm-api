#!/usr/bin/env python
import os, sys
import requests
from datetime import datetime, timezone
from requests.exceptions import HTTPError
from pathlib import Path
from dotenv import load_dotenv

dotenv_path = Path('.env/.venv')
load_dotenv(dotenv_path=dotenv_path)

# mastodon access token
api_key = os.getenv('API_KEY')
#enviroment = os.getenv('ENVIROMENT') # dev - prd (butquet)



# Create de database and schematas
def warehouse():

    from pyiceberg.schema import Schema
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, IntegerType, StringType, DateType, TimestampType, LongType
    from pyiceberg.partitioning import PartitionSpec, PartitionField, IdentityTransform, DayTransform

    warehouse_path = "./warehouse"

    # data warehouse

    catalog = SqlCatalog(
        "lastfm",
        **{
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )


     # processing   

    schema = Schema(
        NestedField(field_id=1, name='timestamp', field_type=LongType(), required=False),
        NestedField(field_id=2, name='date_text', field_type=StringType(), required=False),
        NestedField(field_id=3, name='artist', field_type=StringType(), required=False),
        NestedField(field_id=4, name='artist_mbid', field_type=StringType(), required=False),
        NestedField(field_id=5, name='album', field_type=StringType(), required=False),
        NestedField(field_id=6, name='album_mbid', field_type=StringType(), required=False),
        NestedField(field_id=7, name='track', field_type=StringType(), required=False),
        NestedField(field_id=8, name='track_mbid', field_type=StringType(), required=False),
        NestedField(field_id=9, name='date', field_type=DateType(), required=False)
    )

    catalog.create_namespace("silver")

    partition_spec = PartitionSpec(PartitionField(source_id=9, field_id=9, transform=IdentityTransform(), name="date"), spec_id=1)

    catalog.create_table(
        "silver.tracks",
        schema=schema,
        partition_spec=partition_spec
    ) 

    # consumption
    
    schema = Schema(
        NestedField(field_id=1, name='datetime', field_type=TimestampType(), required=False),
        NestedField(field_id=2, name='artist', field_type=StringType(), required=False),
        NestedField(field_id=3, name='album', field_type=StringType(), required=False),
        NestedField(field_id=4, name='track', field_type=StringType(), required=False),
        NestedField(field_id=5, name='date', field_type=DateType(), required=False)
    )

    catalog.create_namespace("gold")

    partition_spec = PartitionSpec(PartitionField(source_id=5, field_id=5, transform=IdentityTransform(), name="date"), spec_id=1)

    catalog.create_table(
        "gold.tracks",
        schema=schema,
        partition_spec=partition_spec
    ) 


def drop_table():

    from pyiceberg.catalog.sql import SqlCatalog
    warehouse_path = "./warehouse"

    catalog = SqlCatalog(
        "lasftfm",
        **{
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )  

    catalog.drop_table("silver.tracks")
    catalog.drop_table("gold.tracks")


def query(schema):

    from pyiceberg.catalog.sql import SqlCatalog

    warehouse_path = "./warehouse"

    catalog = SqlCatalog(
        "lastfm",
        **{
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )  

    table = catalog.load_table((schema, 'tracks'))

    #print(table.describe())

    con = table.scan().to_duckdb(table_name="tracks")

    con.sql(
        "SELECT date, COUNT(*) total FROM tracks GROUP BY date ORDER BY date ASC"
    ).show()
      

#EXTRACT
def extract(date):
    try:

        # date to timestamp
        date_from = date + ' 00:00:00'
        date_to = date + ' 23:59:59'

        dt = datetime.strptime(date_from, "%Y-%m-%d %H:%M:%S")
        date_from = int(dt.replace(tzinfo=timezone.utc).timestamp())

        dt = datetime.strptime(date_to, "%Y-%m-%d %H:%M:%S")
        date_to = int(dt.replace(tzinfo=timezone.utc).timestamp())

        response = requests.get('https://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=radioheadve&api_key=' + api_key + '&from=' + str(date_from) + '&to=' + str(date_to) + '&format=json')

        response.raise_for_status()
        json_response = response.json()

        pages = int(json_response['recenttracks']['@attr']['totalPages'])
        i = 0
        c = pages

        lists = 'timestamp,date_text,artist,artist_mbid,album,album_mbid,track,track_mbid\n'

        while i < pages:
            i += 1
            # return list
            list = get_tracks(start=date_from, end=date_to, page=c)
            c -= 1
            lists = lists + list
            
        with open('./warehouse/raw/tracks-' + date + '.csv', 'w') as f:
            f.write(lists)
            f.close()

    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred 1: {err}')
        # error details
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
    

def get_tracks(start, end, page):
    
    try:
        # request
        response = requests.get('https://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=radioheadve&api_key=' + api_key + '&from=' + str(start) + '&to=' +  str(end) + '&page=' + str(page) + '&format=json')
    
        lists = ''

        json_response = response.json()
        # invert list
        tracks = json_response['recenttracks']['track']
        for track in reversed(tracks):

            if "date" in track:

                list = ('"' + track['date']['uts'] + '",' +
                        '"' + track['date']['#text'] + '",' +
                        '"' + track['artist']['#text'] + '",' +
                        '"' + track['artist']['mbid'] + '",' +
                        '"' + track['album']['#text'] + '",' +
                        '"' + track['album']['mbid'] + '",' +
                        '"' + track['name'] + '",' +
                        '"' + track['mbid'] + '"\n')     
                
                lists = lists + list 

        return lists
    
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred 2: {err}')

        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
    
    return True


#LOAD
def load(date):

    from datetime import date as dt
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.expressions import NotEqualTo   
    import pyarrow as pa
    import pyarrow.csv as pc

    warehouse_path = "./warehouse"

    catalog = SqlCatalog(
        "lastfm",
        **{
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )

    table = catalog.load_table("silver.tracks")

    df = table.scan(
        row_filter = NotEqualTo("date", date)
    ).to_arrow()  

    table.overwrite(df) 

    df = pc.read_csv(warehouse_path + "/raw/tracks-" + date + ".csv")

    year, month, day = map(int, date.split('-'))

    df = df.append_column("date", pa.array([dt(year, month, day)] * len(df), pa.date32()))

    table.append(df)    


#TRANSFORM 
def transformation(date):

    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.expressions import EqualTo, NotEqualTo

    import pyarrow as pa
    import pyarrow.compute as pc

    warehouse_path = "./warehouse"

    catalog = SqlCatalog(
        "lastfm",
        **{
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )  

    silver_table = catalog.load_table(('silver', 'tracks'))

    silver_df = silver_table.scan(
        row_filter = EqualTo("date", date),
        selected_fields = ("timestamp","artist","album","track","date"),
    ).to_arrow()

    col = pc.cast(silver_df["timestamp"], pa.timestamp('s'))

    silver_df = silver_df.drop("timestamp")

    silver_df = silver_df.append_column("datetime", col)

    # gold

    gold_table = catalog.load_table(('gold', 'tracks'))

    df = gold_table.scan(
        row_filter = NotEqualTo("date", date)
    ).to_arrow()  

    gold_table.overwrite(df) 

    with gold_table.update_schema() as update_schema:
        update_schema.union_by_name(silver_df.schema)    

    gold_table.append(silver_df) 


if __name__ == '__main__':

    #warehouse()
    date = "2024-08-04"
    extract(date)
    load(date)
    query('silver')
    transformation(date)
    query('gold')


