import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = 'sqlite:///my_played_tracks.sqlite'
USER_ID = '31g6fsahtqvexkewhvshpz2r5eee'
TOKEN = 'BQCIVgaqWPteApBP94Xoyd-E2ZpPOjF0tpGOmLLtzgiVjmSwiQ6XiE24FNGQ36IiJfCvMn-5p4__1Xh-uUe6Ve-AxRga1dQyhMkt6s1f2np7lYAL-Rfuaqqlk8TvkHo7a3sbtBjpveycjkMgJ3ZixMOcIMMuaPCejrm3WguDs3gD'

# data validation function
def is_valid(df: pd.DataFrame) -> bool:
    if df.empty:
        print('No songs downloaded. Finishing execution')
        return False

    # Primary Key check
    if df['track_played_at'].is_unique:
        pass
    else: raise Exception('Primary Key check is violated')

    # Null value check
    if df.isna().values.any():
        raise Exception('Null values found')

    # Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df['track_played_at'].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception('At least one of the returned songs is not dated yesterday')

    return True

if __name__ == '__main__':

    # Extract
    headers = {
        'Accept' : 'application/json',
        'Content-Type' : 'application/json',
        'Authorization' : 'Bearer {token}'.format(token=TOKEN)
    }
    
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
    
    r = requests.get('https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}'\
                     .format(time=yesterday_unix_timestamp), headers = headers)
    data = r.json()
    
    song_dict = {
        'artist_name' : [', '.join([artist['name'] for artist in item['track']['artists']]) for item in data['items']],
        'album_name' : [item['track']['album']['name'] for item in data['items']],
        'album_release_date' : [item['track']['album']['release_date'] for item in data['items']],
        'track_name' : [item['track']['name'] for item in data['items']],
        'track_duration' : [item['track']['duration_ms'] for item in data['items']],
        'track_is_explicit' : [item['track']['explicit'] for item in data['items']],
        'track_popularity' : [item['track']['popularity'] for item in data['items']],
        'track_played_at' : [item['played_at'] for item in data['items']]
    }
    
    song_df = pd.DataFrame(song_dict)
    
    # Validate
    if is_valid(song_df):
        print('Data is valid, proceed to Load stage')
    
    # Load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()
    
    sql_query = '''    
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        artist_name VARCHAR(200),
        album_name VARCHAR(200),
        album_release_date VARCHAR(200),
        track_name VARCHAR(200),
        track_duration VARCHAR(200),
        track_is_explicit VARCHAR(200),
        track_popularity VARCHAR(200),
        track_played_at VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (track_played_at)
    )
    '''
    
    cursor.execute(sql_query)
    print('Opened database successfully')
    
    try:
        song_df.to_sql('my_played_tracks', engine, index=False, if_exists='append')
    except: print('Data already exists in the database')
        
    conn.close()
    print('Close database')