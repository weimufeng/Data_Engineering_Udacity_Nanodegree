import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a SparkSession and returns it. If SparkSession is already created it returns
    the currently running SparkSession.
    
    Inputs:
        None
    Return:
        SparkSession
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """   
    Processes song_data from S3, creates dimension tables "songs" and "artists", and saves tables to to S3.
    
    Inputs:
        spark: SparkSession
        input_data: Path of input data from a S3 bucket 
        output_data: Path of a S3 bucket for saving the tables
    Return:
        N/A
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_fields = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df.select(songs_fields).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_output_path = output_data + 'songs'
    songs_table.write.partitionBy('year', 'artist_id').parquet(path=songs_output_path, mode='overwrite')

    # extract columns to create artists table
    artists_fields = ['artist_id', 'artist_name as name', 'artist_location as location', 'artist_latitude as latitude', 'artist_longitude as longitude']
    artists_table = df.selectExpr(artists_fields).distinct()
    
    # write artists table to parquet files
    artists_output_path = output_data + 'artists'
    artists_table.write.parquet(path=artists_output_path, mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Processes log_data from S3, creates dimension tables "users" and "time",a fact table "songplays" and saves tables to to S3.
    
    Inputs:
        spark: SparkSession
        input_data: Path of input data from a S3 bucket 
        output_data: Path of a S3 bucket for saving the tables
    Returns:
        N/A
    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_fields = ['userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level']
    users_table = df.selectExpr(users_fields).distinct()
    
    # write users table to parquet files
    users_output_path = output_data + 'users'
    users_table.write.parquet(path=users_output_path, mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000, TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn("start_time", get_datetime(df.timestamp))
    
    # extract columns to create time table
    df = df.withColumn("hour", hour("start_time"))
    df = df.withColumn("day", dayofmonth("start_time"))
    df = df.withColumn("week", weekofyear("start_time"))
    df = df.withColumn("month", month("start_time"))
    df = df.withColumn("year", year("start_time"))  
    df = df.withColumn("weekday", dayofweek("start_time"))
    
    time_fields = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_table = df.select(time_fields).distinct()
    
    # write time table to parquet files partitioned by year and month
    time_output_path = output_data + 'time'
    time_table.write.partitionBy('year', 'month').parquet(path=time_output_path, mode='overwrite')

    # read in song and artists data to use for songplays table
    songs_df = spark.read.parquet(output_data + 'songs/*/*/*')
    artists_df = spark.read.parquet(os.path.join(output_data, "artists"))

    # extract columns from joined song and log datasets to create songplays table
    songs_logs = df.join(songs_df, (df.song == songs_df.title))
    artists_songs_logs = songs_logs.join(artists_df, (songs_logs.artist == artists_df.name)).drop(artists_df.location)
    
    df = artists_songs_logs.withColumn('songplay_id', monotonically_increasing_id()) 
    
    songplays_fields = ['songplay_id', 'start_time', 'userId as user_id', 'level', 'song_id', 'artist_id', 'sessionId as session_id', 'location', 'userAgent as user_agent', 'year', 'month']
    songplays_table = df.selectExpr(songplays_fields)

    # write songplays table to parquet files partitioned by year and month
    songplays_output_path = output_data + 'songplays'
    songplays_table.write.partitionBy('year', 'month').parquet(path=songplays_output_path, mode='overwrite')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    # Verify success of creation of songplays table
    df = spark.read.parquet(output_data + 'songplays')
    df.printSchema()
    df.show(5)


if __name__ == "__main__":
    main()
