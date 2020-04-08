import glob
import json
import os

import pandas as pd
import psycopg2

from sql_queries import *


def process_song_file(cur, filepath):
    """
        This function takes in the path of a songs file, loads the file and extracts songs information and artist information.
        It then stores this information into the respective songs and artists table
        INPUTS:
        * cur the cursor variable of the database
        * filepath the file path to the song file
    """
    # open song file
    with open(filepath, 'r') as f:
        data = json.load(f)
    df = pd.DataFrame([data])

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = song_data.values[0]
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = artist_data.values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
        This function takes in the path of a log file, loads the file and extracts songplay log information and user information.
        It then stores this information into the respective songplays and user table
        INPUTS:
        * cur the cursor variable of the database
        * filepath the file path to the log file
    """
    with open(filepath, 'r') as f:
        df = pd.read_json(f, lines=True)

    # filter by NextSong action
    df = df[df['page'].str.contains("NextSong")]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_hour = t.dt.hour
    time_second = t.dt.second
    time_data = list(zip(t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday))
    column_labels = ['timestamp', 'hour', 'day', 'weekofyear', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        songid, artistid = results if results else None, None

        # insert songplay record
        songplay_data = (
            pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location,
            row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
        This function takes in the path of a root directory that contains the json files
        and the function that processes each individual file.
        First it extracts a list of all the json files in the given directory.
        Then it iterates over that list and passes each file to the function that processes the file.
        INPUTS:
        * cur the cursor variable of the database
        * conn the connection variable of the database
        * filepath the path of the root directory to process
        * func the function which processes each file
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
        This function is the entry point of our program.
        It connects to the database, processes each directory containing the files and eventually closes the database connection.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()