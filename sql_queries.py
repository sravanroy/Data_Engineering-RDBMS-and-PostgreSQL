# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS public.songplays;"
user_table_drop = "DROP TABLE IF EXISTS public.users;"
song_table_drop = "DROP TABLE IF EXISTS public.songs;"
artist_table_drop = "DROP TABLE IF EXISTS public.artists;"
time_table_drop = "DROP TABLE IF EXISTS public.time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE public.songplays(
songplay_id SERIAL PRIMARY KEY,
start_time TIMESTAMP NOT NULL,
user_id VARCHAR NOT NULL,
level VARCHAR,
song_id VARCHAR,
artist_id VARCHAR,
session_id VARCHAR,
location VARCHAR,
user_agent VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE public.users(
user_id VARCHAR PRIMARY KEY,
first_name VARCHAR,
last_name VARCHAR,
gender VARCHAR(1),
level VARCHAR
);
""")

song_table_create = ("""CREATE TABLE public.songs (
song_id VARCHAR PRIMARY KEY,
title VARCHAR NOT NULL,
artist_id VARCHAR,
year INT4,
duration FLOAT8
);
""")

artist_table_create = ("""
CREATE TABLE public.artists (
artist_id VARCHAR PRIMARY KEY,
name VARCHAR NOT NULL,
location VARCHAR,
lattitude FLOAT8,
longitude FLOAT8
);
""")

time_table_create = ("""
CREATE TABLE public.time (
start_time TIMESTAMP PRIMARY KEY,
hour INT, 
day INT, 
week INT, 
month INT, 
year INT, 
weekday INT
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO public.songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
INSERT INTO public.users(user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT(user_id) 
DO UPDATE 
SET level = EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO public.songs(song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT
DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO public.artists(artist_id, name, location, lattitude, longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT
DO NOTHING;
""")

time_table_insert = ("""
INSERT INTO public.time(start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT
DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, a.artist_id
FROM public.songs s
INNER JOIN public.artists a
ON s.artist_id  = a.artist_id
WHERE s.title = %s
AND a.name = %s
AND s.duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create,
                        time_table_create]

drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]