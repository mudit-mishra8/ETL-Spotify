import json
import boto3
from datetime import datetime, timedelta
import urllib.request
import pandas as pd
import os
from io import StringIO


def create_song_df(song_id, song_name,song_duration_ms, song_popularity, song_url, album_id, artist_id):
    data_dict = {
    'song_id': song_id,
    'song_name': song_name,
    'song_duration_ms': song_duration_ms,
    'song_popularity': song_popularity,
    'song_url': song_url,
    'album_id': album_id,
    'artist_id': artist_id
     }
    df_songs = pd.DataFrame(data_dict)
    df_songs['artist_id'] = df_songs['artist_id'].astype(str).str.replace('[', '').str.replace(']', '').str.replace('\'', '')
    return df_songs



def create_album_df(album_id, album_name, album_release_date, album_total_tracks, album_url):
    data_dict = {
    'album_id': album_id,
    'album_name': album_name,
    'album_release_date': album_release_date,
    'album_total_tracks': album_total_tracks,
    'album_url': album_url
     }

    df_album = pd.DataFrame(data_dict) 
    return df_album

    


def create_artist_df(artist_id_single, artist_name_single, artist_url_single):
    data_dict = {
    'artist_id_single': artist_id_single,
    'artist_name_single': artist_name_single,
    'artist_url_single': artist_url_single,
    }

    df_artist = pd.DataFrame(data_dict)
    return df_artist
    
    
    
def lambda_handler(event, context):
    
    s3_client = boto3.client('s3')
    filename = "spotify_extracted_" + str(datetime.now().date()) + ".json"
    print("extracted_data/" + filename)
    s3_response = s3_client.get_object(Bucket='spotify-mudit',  Key="extracted_data/" + filename)
    
    # Extract data from S3 response
    s3_data = s3_response['Body'].read().decode('utf-8')
    data = json.loads(s3_data)
     
    # initializing empty list which serves as columsn of our dataframes
    song_id, song_name, song_duration_ms, song_popularity,song_url, album_id, artist_id =[],[],[],[],[],[],[]
    album_name, album_release_date, album_total_tracks, album_url = [],[],[],[]   
    artist_id_single, artist_name_single, artist_url_single = [] ,[],[]
    
    for i in range(len(data['items'])):
        #songs
        album_id.append(data['items'][i]['track']['album']['id'])
        artist_id_list=[]
        for x in range(len(data['items'][i]['track']['artists'])):
            artist_id_list.append(data['items'][i]['track']['artists'][x]['id'])
        artist_id.append(artist_id_list)
        song_id.append(data['items'][i]['track']['id'])
        song_name.append(data['items'][i]['track']['name'])
        song_duration_ms.append(data['items'][i]['track']['duration_ms'])
        song_popularity.append(data['items'][i]['track']['popularity'])
        song_url.append(data['items'][i]['track']['external_urls']['spotify'])
    
        #album
        album_name.append(data['items'][i]['track']['album']['name'])
        album_release_date.append(data['items'][i]['track']['album']['release_date'])
        album_total_tracks.append(data['items'][i]['track']['album']['total_tracks'])
        album_url.append(data['items'][i]['track']['album']['external_urls']['spotify'])
    
        #artist
        for x in range(len(data['items'][i]['track']['artists'])):
            artist_id_single.append(data['items'][i]['track']['artists'][x]['id'])
            artist_name_single.append(data['items'][i]['track']['artists'][x]['name'])
            artist_url_single.append(data['items'][i]['track']['artists'][x]['external_urls']['spotify'])

        
        
        df_songs = create_song_df(song_id, song_name,song_duration_ms, song_popularity, song_url, album_id, artist_id)
        df_album = create_album_df(album_id, album_name, album_release_date, album_total_tracks, album_url)
        df_artist = create_artist_df(artist_id_single, artist_name_single, artist_url_single)
        
        
        
        filename_songs = "spotify_songs_" + str(datetime.now().date()) + ".csv"
        filename_albums = "spotify_albums_" + str(datetime.now().date()) + ".csv"
        filename_artists = "spotify_artists_" + str(datetime.now().date()) + ".csv"
                                                                                
        # DataFrames to CSV in-memory
        csv_buffer_songs = StringIO()
        csv_buffer_albums = StringIO()
        csv_buffer_artists = StringIO()
        
        df_songs.to_csv(csv_buffer_songs, index=False)
        df_album.to_csv(csv_buffer_albums, index=False)
        df_artist.to_csv(csv_buffer_artists, index=False)
        
        
        s3_client.put_object(
        Bucket="spotify-mudit",
        Key="upload_csv_files/songs/" + filename_songs,
        Body=csv_buffer_songs.getvalue(),
        ContentType='text/csv'
        )  
        
        s3_client.put_object(
        Bucket="spotify-mudit",
        Key="upload_csv_files/albums/" + filename_albums,
        Body=csv_buffer_albums.getvalue(),
        ContentType='text/csv'
        )
        
        s3_client.put_object(
        Bucket="spotify-mudit",
        Key="upload_csv_files/artists/" + filename_artists,
        Body=csv_buffer_artists.getvalue(),
        ContentType='text/csv'
        )
        