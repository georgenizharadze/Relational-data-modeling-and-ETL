import os
import glob
from collections import OrderedDict
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """ Read a single song json file, transform and insert data in RDB.

    Read into a pandas df the single object present in the json
    file. Filter the data required for the songs and artists tables
    and insert the data into the relevant table. 

    Arguments:
    cur -- the database cursor object
    filepath -- absolute path to the file
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].\
        values[0].tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location',
                      'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """ Read a single log json file, transform and insert data in RDB. 

    Read into a pandas df multiple objects present in a file. 
    Extract and transform data required to populate 3 tables in 
    the database: users, time and songplays. 

    Arguments:
    cur -- the database cursor object
    filepath -- absolute path to the file
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    t = df['ts']

    # insert time data records
    time_data = (i.tolist() for i in [t, t.dt.hour, t.dt.day, t.dt.week,
                 t.dt.month, t.dt.year, t.dt.weekday])
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year',
                     'weekday')

    time_dict = OrderedDict((col_name, arraylike) for col_name, arraylike in
                            zip(column_labels, time_data))  # To preserve order

    time_df = pd.DataFrame.from_dict(time_dict)

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
        # note that for every row, the fuction has to go to the database 
        # to retrieve values, which is costly in time. 
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid,
                         row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Find all json data recursively in a folder and process them

    Given the path to a folder and a function, recursively retrieve
    the absolute paths to all the files in the folder and its sub-
    folders and process each file by the given function. 
    
    Arguments:
    cur -- the database cursor object
    conn -- the database connection obect
    filepath -- absolute path to a folder
    func -- the function to be used for processing the file, i.e. 
    process_song_file or process_log_file. 
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")  # noqa
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
