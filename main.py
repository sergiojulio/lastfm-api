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


def test(date='2024-08-02'):
    date_from = date + ' 00:00:00'
    date_to = date + ' 23:59:59'

    dt = datetime.strptime(date_from, "%Y-%m-%d %H:%M:%S")

    print(int(dt.replace(tzinfo=timezone.utc).timestamp()))

    print(int(datetime.strptime(date_from, "%Y-%m-%d %H:%M:%S").timestamp()))
    print(int(datetime.strptime(date_to, "%Y-%m-%d %H:%M:%S").timestamp()))   


#EXTRACT
def extract(date='2024-08-03'):
    try:

        # date to timestamp
        date_from = date + ' 00:00:00'
        date_to = date + ' 23:59:59'

        dt = datetime.strptime(date_from, "%Y-%m-%d %H:%M:%S")
        date_from = int(dt.replace(tzinfo=timezone.utc).timestamp())

        dt = datetime.strptime(date_to, "%Y-%m-%d %H:%M:%S")
        date_to = int(dt.replace(tzinfo=timezone.utc).timestamp())


        response = requests.get('https://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=radioheadve&api_key=' + api_key + '&from=' + str(date_from) + '&to=' + str(date_to) + '&format=json')
        #print(response.text)

        response.raise_for_status()
        # access JSOn content
        json_response = response.json()

        # init list headers

        pages = int(json_response['recenttracks']['@attr']['totalPages'])
        i = 0
        c = pages

        lists = 'timestamp,date_text,artist,artist_mbid,album,album_mbid,track,track_mbid\n'

        while i < pages:
            print(i)
            i += 1
            # return list
            list = get_tracks(start=date_from, end=date_to, page=c)
            c -= 1
            lists = lists + list
            
        # close file
        with open('./csv/tracks-' + date + '.csv', 'w') as f:
            f.write(lists)
            f.close()

    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred 1: {err}')

        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
    

def get_tracks(start, end, page):
    
    try:
        # request
        response = requests.get('https://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=radioheadve&api_key=' + api_key + '&from=' + str(start) + '&to=' +  str(end) + '&page=' + str(page) + '&format=json')
    
        # print('https://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=radioheadve&api_key=' + api_key + '&from=' + str(start) + '&to=' +  str(end) + '&page=' + str(page) + '&format=json')

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

# Create de database and schema
def warehouse():

    from pyiceberg.schema import Schema
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, IntegerType, StringType, DateType, TimestampType, LongType
    from pyiceberg.partitioning import PartitionSpec, PartitionField, IdentityTransform, DayTransform

    warehouse_path = "./warehouse"

    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )

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

    catalog.create_namespace("stage")

    partition_spec = PartitionSpec(PartitionField(source_id=9, field_id=9, transform=IdentityTransform(), name="date"), spec_id=1)

    catalog.create_table(
        "stage.tracks",
        schema=schema,
        partition_spec=partition_spec
    ) 


def drop_table():

    from pyiceberg.catalog.sql import SqlCatalog
    warehouse_path = "./warehouse"

    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )  

    catalog.drop_table("default.tracks")


def query():

    from pyiceberg.catalog.sql import SqlCatalog

    warehouse_path = "./warehouse"

    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )  


    table = catalog.load_table(('default', 'tracks'))

    #print(table.describe())

    con = table.scan().to_duckdb(table_name="tracks")


    con.sql(
        "SELECT date, COUNT(*) total FROM tracks GROUP BY date"
    ).show()
      

def create_table():

    from datetime import date

    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, IntegerType, StringType, DateType, TimestampType, LongType
    from pyiceberg.partitioning import PartitionSpec, PartitionField, IdentityTransform, DayTransform
    from pyiceberg.catalog.sql import SqlCatalog

    # pandas
    import pyarrow as pa
    import pyarrow.csv as pc

    warehouse_path = "./warehouse"

    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )
    # timpestamp,date_text,artist,artist_mbid,album,album_mbid,track,track_mbid

    """
    schema = pa.schema([
        ('timpestamp', pa.int32()),
        ('date_text', pa.string()),
        ('artist', pa.string()),
        ('artist_mbid', pa.string()),
        ('album', pa.string()),
        ('album_mbid', pa.string()),
        ('track', pa.string()),
        ('track_mbid', pa.string())
    ])    
    """

    #1722556800
    #999999999
    df = pc.read_csv("./csv/tracks-2024-08-02.csv")

    #df = df.append_column("date", pa.array([1722571] * len(df), pa.date32()))
    df = df.append_column("date", pa.array([date(2024, 8, 2)] * len(df), pa.date32()))

    # table_partitions = pyarrow.compute.partition(arrow_table, partition_columns)

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


    #df = df.partition_to_path([("year", 2024), ("month", 8)], df.schema)
    # schema puede ser pyarrow o pyiceberg
    partition_spec = PartitionSpec(PartitionField(source_id=9, field_id=9, transform=IdentityTransform(), name="date"), spec_id=1)
    # partition_spec = PartitionSpec.builder_for(schema).identity("date").build()

    table = catalog.create_table(
        "default.tracks",
        schema=schema,
        partition_spec=partition_spec
    ) 

    # table.append(df)

    with table.update_schema() as update_schema:
        update_schema.union_by_name(df.schema)    

    table.overwrite(df)


#LOAD
def write_data(date='2024-08-03'):

    from datetime import date
    from pyiceberg.catalog.sql import SqlCatalog

    # pandas
    import pyarrow as pa
    import pyarrow.csv as pc

    warehouse_path = "./warehouse"

    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )

    df = pc.read_csv("./csv/tracks-2024-08-03.csv")

    df = df.append_column("date", pa.array([date(2024, 8, 3)] * len(df), pa.date32()))

    table = catalog.load_table("default.tracks")

    table.overwrite(df)    


#TRANSFORM 
def transformation(date="2024-08-03"):

    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.expressions import EqualTo

    import pyarrow as pa
    import pyarrow.compute as pc

    warehouse_path = "./warehouse"

    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )  

    table = catalog.load_table(('default', 'tracks'))

    df = table.scan(
        row_filter = EqualTo("date", date),
        selected_fields = ("timestamp","date_text","artist","artist_mbid","album","album_mbid","track","track_mbid","date"),
    ).to_arrow()

    #pc.strptime(df.column("simestamp"), format='%Y-%m-%d %H:%M:%S', unit='s')


    #df = df.append_column("date", pa.array([date(2024, 8, 3)] * len(df), pa.date32()))

    col = pc.cast(df["timestamp"], pa.timestamp('s'))

    df = df.append_column("datetime", col)
    # 

    print(df)

    #table.append(df) 

def temp():

    import pyarrow as pa
    import pyarrow.compute as pc

    # Assuming '1722643317' is an epoch time (in seconds)
    array = pa.array([1722643317], pa.int64())

    # Cast the integer array to a timestamp
    timestamp_array = pc.cast(array, pa.timestamp('s'))

    print(timestamp_array)

if __name__ == '__main__':
    transformation()


