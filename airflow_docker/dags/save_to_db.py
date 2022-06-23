import pyodbc
import pandas as pd
from datetime import datetime, timedelta

class save_to_db:
    def __init__(self):
    # Set the unix timestamp for yesterday
        today = datetime.now()
        self.yesterday = today - timedelta(days = 1)
        self.yesterday = self.yesterday.replace(hour=0,minute=0,second=0,microsecond=0)
        self.songs_df = pd.DataFrame()

    def save_to_db(self):
        # read data from json file
        self.songs_df = pd.read_json(<input_file_path>, orient = 'index')

        # Establish the connection with the database in the SQL Server
        conn = pyodbc.connect('DRIVER=<SQLSERVER_DRIVER>;SERVER=<SERVER_NAME>;DATABASE=<DB_NAME>;UID=<USER>;PWD=<PASSWORD>;TrustServerCertificate=yes;')

        # Set the cursor
        cursor = conn.cursor()

        # Prepare the query for inserting records from the dataframe
        insert_query = '''
            INSERT INTO db_name.table_name (
                song_name,
                played_at,
                song_duration_ms,
                song_type,
                is_explicit,
                is_local,
                popularity,
                artist_name,
                artist_type,
                album_name,
                album_type,
                album_releasedate)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        '''

        # For each rows of the dataframe, insert to the database
        failed_inserts = 0
        for rows in range(len(self.songs_df)):
            values = []

            for columns in range(len(self.songs_df.columns)):
                value = self.songs_df.iloc[rows, columns]
                if columns == 4 or columns == 5:
                    if value == True:
                        value = '1'
                    elif value == False:
                        value = '0'
                    else:
                        value = ''
                elif columns == 2 or columns == 6:
                    value = str(value)
                
                values.append(value)

            values = tuple(values)

            try:
                cursor.execute(insert_query, values)
            except Exception as e:
                failed_inserts += 1

        print(f'{failed_inserts} unsuccessful inserts from {len(self.songs_df)} records')
        conn.commit()

def call_save_to_db():
    # Assign class to a variable and call function
    a = save_to_db()
    a.save_to_db()
