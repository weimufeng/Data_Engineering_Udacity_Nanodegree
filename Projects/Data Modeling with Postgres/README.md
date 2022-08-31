# Project: Data Modeling with Postgres

## Overview
In this project, we create a Postgres database and ETL pipeline which models user activity data for a music streaming app called Sparkify for optimizing queries on song play analysis.

## Postgres Database Schema
### Fact table
```
songplays
    songplay_id serial primary key, 
    start_time timestamp not null, 
    user_id int not null, 
    level varchar(150), 
    song_id varchar(150), 
    artist_id varchar(150), 
    session_id int, 
    location varchar(150), 
    user_agent varchar(150)
```

### Dimension table
```
users
    user_id int primary key, 
    first_name varchar(150), 
    last_name varchar(150), 
    gender varchar(150), 
    level varchar(150)

songs
    song_id varchar primary key, 
    title varchar(150) not null, 
    artist_id varchar(150) not null, 
    year int, 
    duration decimal not null

artists
    artist_id varchar primary key, 
    name varchar(150) not null, 
    location varchar(150), 
    latitude float, 
    longitude float

time
    start_time timestamp primary key, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int
```


## ETL Pipeline
### etl.py
ETL pipeline main builder

1. `process_song_file`
    Read JSON files, extract information of songs and artists, save into song_data and artist_data.
2. `process_log_file`
    Read LOG files, extract information of time, users, and songplays, save into time, users, and songplays.
3. `process_data`
    Process and analyze data.

### create_tables.py
Create database, drop and create tables
1. `create_database`
2. `drop_tables`
3. `create_tables`

### sql_queries.py
Helper SQL queries

## Test
### test.ipynb
Include some basic sanity testing will be performed to esnure that the work does NOT contain any commonly found issues.

## Other relevant files
### data
Contains all the JSON and LOG data

### etl.ipynb
Helper notebook for etl.py

