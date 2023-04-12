from io import BytesIO
from azure.storage.blob import BlobServiceClient
from datetime import datetime
from dateutil.parser import parse
import pandas as pd
import requests
import re


def upload_data(file_name:str, data):
    # azure variables
    BLOB_STORE_CONN_STR = ''
    CONTAINER_NAME = ''
    BLOB_PATH = file_name

    blob_service_client = BlobServiceClient.from_connection_string(BLOB_STORE_CONN_STR)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_PATH)

    # creating and writing to a parquet data object in memory
    parquet_file = BytesIO()
    data.to_parquet(parquet_file, engine='pyarrow')
    parquet_file.seek(0)

    blob_client.upload_blob(data=parquet_file, overwrite=True)


def artist_streams():
    url = 'https://kworb.net/spotify/artists.html'
    df = pd.read_html(url)[0]

    dt = f"{datetime.now().strftime('%Y-%m-%d')}"
    df.insert(0, 'Date', dt)
    
    file_name = f'{dt}-spotify_top_streamed_artists.parquet'

    upload_data(file_name=file_name, data=df)


def artist_listeners():
    url = 'https://kworb.net/spotify/listeners.html'
    df = pd.read_html(url)[0]

    try:
        r = requests.get(url)
        data = r.text
        date = re.findall(pattern='<br><br>(\n.*)<br><br>',string=data)[0]
        date = date.lstrip().replace('Statistics since ', '').rstrip('.')
        dt = parse(date).strftime('%Y-%m-%d')
    except:
        dt = f"NDP={datetime.now().strftime('%Y-%m-%d')}"

    df.insert(0, 'Date', dt)

    file_name = f'{dt}-spotify_artist_monthly_listeners.parquet'

    upload_data(file_name=file_name, data=df)


def song_streams():
    url = 'https://kworb.net/spotify/songs.html'
    df = pd.read_html(url)[0]

    dt = f"{datetime.now().strftime('%Y-%m-%d')}"
    df.insert(0, 'Date', dt)

    file_name = f'{dt}-spotify_top_song_streams.parquet'

    upload_data(file_name=file_name, data=df)

artist_streams()
artist_listeners()
song_streams()