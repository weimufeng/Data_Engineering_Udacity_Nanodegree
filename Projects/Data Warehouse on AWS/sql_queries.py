import configparser

# --- CONFIG ---
config = configparser.ConfigParser()
config.read('dwh.cfg')

# AWS
KEY = config.get('AWS','KEY')
SECRET = config.get('AWS','SECRET')

# S3
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# IAM_ROLE
ARN = config.get('IAM_ROLE', 'ARN')
# --- END CONFIG ---

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
artist varchar,
auth varchar,
firstName varchar,
gender char(1),
itemInSession smallint,
lastName varchar,
length float,
level varchar,
location varchar,
method varchar,
page varchar,
registration bigint,
sessionId int,
song varchar,
status smallint,
ts bigint,
userAgent varchar,
userId int
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
num_songs int,
artist_id varchar,
artist_latitude float,
artist_longitude float,
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration float,
year smallint
);
""")

# Fact table
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplays (
songplay_id int identity(0,1) primary key,
start_time timestamp references dim_time (start_time),
user_id int references dim_users (user_id),
level varchar,
song_id varchar references dim_songs (song_id),
artist_id varchar references dim_artists (artist_id),
session_id int, 
location varchar, 
user_agent varchar
);
""")

# Dimension tables
user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_users (
user_id int primary key, 
first_name varchar, 
last_name varchar, 
gender char(1), 
level varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_songs (
song_id varchar primary key, 
title varchar, 
artist_id varchar not null, 
year smallint, 
duration float
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artists (
artist_id varchar primary key,  
name varchar not null, 
location varchar, 
latitude float, 
longitude float
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time (
start_time timestamp primary key, 
hour smallint not null, 
day smallint not null, 
week smallint not null, 
month smallint not null, 
year smallint not null, 
weekday smallint not null
);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {0}
credentials 'aws_iam_role={1}'
compupdate off 
region 'us-west-2'
json {2};
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from {0}
credentials 'aws_iam_role={1}'
compupdate off 
region 'us-west-2'
json 'auto';
""").format(SONG_DATA, ARN)

# Insert FINAL TABLES
songplay_table_insert = ("""
INSERT INTO fact_songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
se.userId,
se.level,
ss.song_id,
ss.artist_id,
se.sessionId,
se.location,
se.userAgent
FROM staging_events se
JOIN staging_songs ss 
ON se.song=ss.title AND se.artist=ss.artist_name
WHERE se.page='NextSong';
""")

user_table_insert = ("""
INSERT INTO dim_users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
userId,
firstName,
lastName,
gender,
level
FROM staging_events se
WHERE se.page='NextSong' AND userId IS NOT NULL
;
""")

song_table_insert = ("""
INSERT INTO dim_songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
song_id,
title,
artist_id,
year,
duration
FROM staging_songs
WHERE song_id IS NOT NULL
;
""")

artist_table_insert = ("""
INSERT INTO dim_artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
artist_id,
artist_name,
artist_location,
artist_latitude,
artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
;
""")

time_table_insert = ("""
INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
start_time,
EXTRACT(hour from start_time),
EXTRACT(day from start_time),
EXTRACT(week from start_time),
EXTRACT(month from start_time),
EXTRACT(year from start_time),
EXTRACT(weekday from start_time)
FROM fact_songplays
;
""")

# QUERY LISTS
# First create dimension tables, then fact table. Because fact table need to reference dimention tables as foreign keys.
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
