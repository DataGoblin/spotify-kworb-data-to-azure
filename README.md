# Spotify Data to Azure Blob Storage
Scrapes Spotify data from kworb.net. Uploads as parquet files to Azure Blob Storage

Kworb presents all data on a static webpage which makes it incredibly easy to scrape and puts practically 0 stress on their servers! 

Includes three functions that scrape specific pages.
- https://kworb.net/spotify/artists.html
- https://kworb.net/spotify/listeners.html
- https://kworb.net/spotify/songs.html


Once the pages are scraped the dataframe is sent to an upload function. Which uploads the dataframe from memory into a parquet file that is uploaded to Azure Blob Storage


This repo is incredibly plug-n-play friendly. All you would need to do is fill the following variables in the upload function. 
- BLOB_STORE_CONN_STR = ''
- CONTAINER_NAME = ''

Any further customisation such as file names can be achieved by editing the `file_name` variable in each function. 
