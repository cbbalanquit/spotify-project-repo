import sys
from pathlib import Path
from xmlrpc.client import _datetime
import requests
import json
import pandas as pd
from datetime import datetime, timedelta

class Run_spotify_etl:
    def __init__(self):
        # Initialize value needed for the refresh token
        self.client_id = "90aabbdc77d2445db9d8d8ea4e0a0082"
        self.client_secret = "77a3b0f0389e449d9cb7b2fdb9f70ece"
        self.redirect_uri = "https://spotify_deproject.me"
        self.spotify_token = ""
        self.refresh_token = "AQCff5gPtL2385QdYJwAPaas0bWjiUCe3ezuOh0ZLmdR12_ECUvNeJx2qdEKvMcfC7xRHeVE7WlbsOPajF_ybbeWXn4tCJA7qWBcVmtkywNYQmoMeo9LCt05lcLzlublfRQ"
        self.base_64 = "OTBhYWJiZGM3N2QyNDQ1ZGI5ZDhkOGVhNGUwYTAwODI6NzdhM2IwZjAzODllNDQ5ZDljYjdiMmZkYjlmNzBlY2U="

        # initialize the yesterday variables
        self.yesterday_unix_timestamp = ""
        self.yesterday = ""

        # Initialize values needed for retrieving data from json file
        self.songs_df = pd.DataFrame()

        # Initialize variables for the data_validation part 
        self.status = ""
        self.df = pd.DataFrame()
        self.message = ""

        # Initialize variable for the database connection
        self.DATABASE_LOCATION = "sqlite:///played_tracks.sqlite"

    def data_validation(self):
        string_final = ""
        temp_df = self.songs_df
        
        # Check if dataframe is empty
        if temp_df.empty:
            string = f"No record found for the date {self.yesterday}"
            string_final = string
            return temp_df, "empty", string_final

        # Remove records which does not match the date yesterday
        timestamps = temp_df["played_at"].tolist()
        incorrect_timestamp = 0
        for iteration, timestamp in enumerate(timestamps):
            iter_timestamp = datetime.strptime(timestamp,'%Y-%m-%dT%H:%M:%S.%fZ').replace(hour=0, minute=0, second=0, microsecond=0)
            if  iter_timestamp != self.yesterday:
                incorrect_timestamp += 1
                temp_df = temp_df.drop(axis=0, index = iteration)

        if incorrect_timestamp == len(temp_df):
            string = "All of the retrieved songs have incorrect timestamps"
            string_final += '\n' + string
            return temp_df, "All invalid dates", string_final

        elif incorrect_timestamp > 1 and incorrect_timestamp < len(temp_df):
            string = f"There are {incorrect_timestamp} count(s) of inappropriate timestamps"
            string_final += '\n' + string
        
        else:
            pass  

        # Check primary keys if have duplicates
        if pd.Series(temp_df['played_at']).is_unique:
            pass
        else:
            temp_df = temp_df.drop_duplicates(subset=['played_at'], keep='last', ignore_index=True)
            string = ("Some duplicate data are removed")
            string_final += '\n' + string
        
        #Check for nulls
        if temp_df.isnull().values.any():
            null_count = temp_df["played_at"].isna().sum()

            if null_count == len(temp_df):
                temp_df = temp_df.dropna(subset=['played_at'])
                string = "All of the retrieved songs have null timestamps"
                string_final += '\n' + string
                return temp_df, "All_NA", string_final

            elif null_count > 1 and null_count < len(temp_df):
                temp_df = temp_df.dropna(subset=['played_at'])
                string = f"There are ({null_count}) number of null values in the data extracted"
                string_final += '\n' + string
            
            else:
                pass
        
        return temp_df, "Valid", string_final

    def retrieve_recentlyplayed(self):
        # Initialize the variables needed for the GET request
        recently_played_url = "https://api.spotify.com/v1/me/player/recently-played"
        headers = {
        "Accept" : "application/json",
        "Content-type" : "application/json",
        "Authorization" : "Bearer {token}".format(token = self.spotify_token)
        }

        ## IF SUCCESSFUL, request the data using GET Request API, convert the retrieved data into json and assign to a variable
        ## IF UNSUCCESSFUL, end the job
        print(f"...Connect to Spotify and establish GET Requests for {self.yesterday}...")

        try:
            r = requests.get(f"{recently_played_url}?limit=50&after{self.yesterday_unix_timestamp}", headers = headers)
            
            print(f"GET request from Spotify API is successful: {recently_played_url}")

        except Exception as e:
            print(e)
            print(f"Error in GET request. (request_url: {recently_played_url})")
            sys.exit()
        
        # Convert the response into json format and transfer to a variable
        data = r.json()
        Path(".../spotify_project/output_data/history_json_raw").mkdir(parents=True, exist_ok=True)
        with open(f'.../spotify_project/output_data/history_json_raw/raw_{self.yesterday.date()}.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        # Declare the variables needed as list
        song_name = []
        played_at = []
        song_duration = []
        song_type = []
        is_explicit = []
        is_local = []
        popularity = []
        artist_name = []
        artist_type = []
        album_name = []
        album_type = []
        album_releasedate = []

        # Retrieve all the data from the data extracted in Spotify API
        for song in data["items"]:
            song_name.append(song["track"]["name"])
            played_at.append(song["played_at"])
            song_duration.append(song["track"]["duration_ms"])
            song_type.append(song["track"]["type"])
            is_explicit.append(song["track"]["explicit"])
            is_local.append(song["track"]["is_local"])
            popularity.append(song["track"]["popularity"])
            artist_name.append(song["track"]["artists"][0]["name"])
            artist_type.append(song["track"]["artists"][0]["type"])
            album_name.append(song["track"]["album"]["name"])
            album_type.append(song["track"]["album"]["type"])
            album_releasedate.append(song["track"]["album"]["release_date"])

        # Create a dictionary using the list with data
        song_dict = {
            "song_name" : song_name,
            "played_at" : played_at,
            "song_duration" : song_duration,
            "song_type": song_type,
            "is_explicit": is_explicit,
            "is_local": is_local,
            "popularity": popularity,
            "artist_name": artist_name,
            "artist_type": artist_type,
            "album_name": album_name,
            "album_type": album_type,
            "album_releasedate": album_releasedate
        }

        # Transform dictionary into dataframe
        self.songs_df = pd.DataFrame.from_dict(song_dict)

        # Validate the dataframe
        self.songs_df, status, message = self.data_validation()
        print(status + message)

        # Save the file to the recentlyplayed_csv and recentlyplayed_json folder
        Path(".../spotify_project/output_data/recentlyplayed_csv").mkdir(parents=True, exist_ok=True)
        Path(".../spotify_project/output_data/recentlyplayed_json").mkdir(parents=True, exist_ok=True)
        self.songs_df.to_csv(f'.../spotify_project/output_data/recentlyplayed_csv/{self.yesterday.date()}.csv', index = False)
        self.songs_df.to_json(f'.../spotify_project/output_data/recentlyplayed_json/{self.yesterday.date()}.json', orient = 'index')

        print(f"Data has been successfully retrieved from the API GET requests. \n (request_url: {recently_played_url}, timestamp: {self.yesterday})")

    def call_refresh(self):
        # Call the refresh module and load the function for token refresh. Then, transfer the token refresh in a variable
        print("...Refreshing Token...")

        try:
            # request spotify token using a refresh token
            query = "https://accounts.spotify.com/api/token"
            response = requests.post(query,
                                    data={"grant_type": "refresh_token",
                                        "refresh_token": self.refresh_token},
                                    headers={"Authorization": "Basic " + self.base_64})

            response_json = response.json()
            self.spotify_token = response_json["access_token"]

            print(f"Refresh token has been successfully retrieved.")

        except Exception as e:
            print("Error in generating refresh token. Program abandoned")
            print(e)
            sys.exit()

        # Set the unix timestamp for yesterday
        today = datetime.now()
        self.yesterday = today - timedelta(days = 1)
        self.yesterday = self.yesterday.replace(hour=0,minute=0,second=0,microsecond=0)
        self.yesterday_unix_timestamp = int(self.yesterday.timestamp()) * 1000
        

        # Call the retrieve_recentlyplayed function for the pipeline
        print(f"...Extracting spotify data from '{self.yesterday}'...")

        self.retrieve_recentlyplayed()

def call_spotify_etl():
    # Assign class to a variable and call function
    a = Run_spotify_etl()
    a.call_refresh()


