import os
import json
import boto3
from datetime import datetime
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

def lambda_handler(event, context):
    
    spotify_client_id = os.environ.get('spotify_client_id')
    spotify_client_secret = os.environ.get('spotify_client_secret')
    
    try:
        # Initialize Spotify client
        client_credentials_manager = SpotifyClientCredentials(client_id=spotify_client_id, client_secret=spotify_client_secret)
        sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
        # Extract playlist data
        playlist_link = 'https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF'
        playlist_uri = playlist_link.split("/")[-1]
        data = sp.playlist_tracks(playlist_uri)
    
    except Exception as e:
        # Handle exceptions that may occur during Spotify API access
        print(f"An error occurred during data extraction: {e}")
        return
    
    # Initialize S3 client
    client = boto3.client('s3')
    filename = "spotify_extracted_" + str(datetime.now().date()) + ".json"
    
    try:
        # Upload the file to S3
        response = client.put_object(
            Bucket="spotify-mudit",
            Key="extracted_data/" + filename,
            Body=json.dumps(data)
        )
        # Check the response
        if 'ETag' in response:
            print("File uploaded successfully.")
        else:
            print("File upload failed.")
    except Exception as e:
        # Handle any exceptions that occurred during the upload
        print(f"An error occurred during file upload: {e}")
