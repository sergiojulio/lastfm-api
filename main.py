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


def main(date='2024-08-02'):
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

        lists = ''

        while i < pages:
            print(i)
            i += 1
            # return list
            list = get_tracks(start=date_from, end=date_to, page=c)
            c -= 1
            lists = lists + list
            
        # close file
        with open('./tracks-' + date + '.csv', 'w') as f:
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

                list = ("'" + track['date']['uts'] + "'," +
                        "'" + track['date']['#text'] + "'," +
                        "'" + track['artist']['#text'] + "'," +
                        "'" + track['artist']['mbid'] + "'," +
                        "'" + track['album']['#text'] + "'," +
                        "'" + track['album']['mbid'] + "'," +
                        "'" + track['name'] + "'," +
                        "'" + track['mbid'] + "'\n")     
                
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