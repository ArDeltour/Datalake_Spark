import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

from pyspark.sql import functions as F
from pyspark.sql import types as T


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','key')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','secret')

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function loads song data, contained in json files and stored in a S3 bucket 
    (see argument "input data"), to produce two parquet files:
        - songs.parquet
        - artists.parquet
    These parquet files will be saved in a S3 bucket too (see argument "output data").
    Arguments:
        - spark: a spark session 
        - input data: a S3 bucket containing the song json files (here s3a://udacity-dend/)
        - output data: a S3 bucket where the output parquet files will be saved 
                    (here the folder "Output" contained in the S3 bucket called "sparkifyade")
    Returns:
        None
    """   
    
    # get filepath to song data file
    song_data_fp = input_data + 'song_data/*/*/*/*.json'
        
    # read song data file
    song_data = spark.read.json(song_data_fp)

    # extract columns to create songs table
    to_keep_songs_table = list(["song_id","title","artist_id","year","duration"])
    songs_table = song_data.select(*to_keep_songs_table).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year","artist_id").parquet(output_data + 'songs.parquet')

    # extract columns to create artists table
    to_keep_artists_table = list(["artist_id","artist_name as name","artist_location as location", \
                                  "artist_latitude as lattitude", "artist_longitude as longitude"])
    artists_table =  song_data.selectExpr(*to_keep_artists_table).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists.parquet')


def process_log_data(spark, input_data, output_data):
    """
    Description: This function loads log data, contained in json files and stored in a S3 bucket 
    (see argument "input data"), to produce three parquet files:
        - users_table.parquet
        - time_table.parquet
        - songplays_table.parquet
    These parquet files will be saved in a S3 bucket too (see argument "output data").
    Arguments:
        - spark: a spark session 
        - input data: a S3 bucket containing the song json files (here s3a://udacity-dend/)
        - output data: a S3 bucket where the output parquet files will be saved 
                    (here the folder "Output" contained in the S3 bucket called "sparkifyade")
    Returns:
        None
    """   
    
    # get filepath to log data file
    log_data_fp = input_data + 'log-data/*/*/*.json'
        
    # read log data file
    log_data = spark.read.json(log_data_fp)
    log_data = log_data.filter(log_data.page=="NextSong")

    # extract columns for users table    
    to_keep_users_table = list(["userID as user_id","firstName as first_name","lastName as last_name","gender","level"])
    users_table =  log_data.orderBy(["userID", "ts"],ascending=[1,0]) \
    .dropDuplicates(['userID']) \
    .selectExpr(*to_keep_users_table )
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users_table.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 
    log_data = log_data.withColumn("start_time", get_timestamp(log_data.ts))
    
    time_table = log_data.select("start_time")
    time_table = time_table.withColumn("year", F.year(log_data.start_time))
    time_table = time_table.withColumn("month", F.month(log_data.start_time))
    time_table = time_table.withColumn("hour", F.hour(log_data.start_time))
    time_table = time_table.withColumn("day", F.dayofmonth(log_data.start_time))
    time_table = time_table.withColumn("week", F.weekofyear(log_data.start_time))
    time_table = time_table.withColumn("weekday", F.date_format(log_data.start_time,'EEEE')).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'time_table.parquet')

    # read in song data to use for songplays table
    song_data_fp = input_data + 'song_data/*/*/*/*.json'
        
    song_df = spark.read.json(song_data_fp)
    
    # filter by actions for song plays
    song_data_2 = song_df.where(col("title").isNotNull())
    
    # extract columns from joined song and log datasets to create songplays table 
    cond = [log_data.artist == song_data_2.artist_name, log_data.song == song_data_2.title]
    songplays_table = log_data.join(song_data_2,cond, how='left')
    songplays_table = songplays_table.select("*").withColumn("songplay_id", F.monotonically_increasing_id())
    to_keep_songplays_table = list(["songplay_id","start_time", \
                                    "userID as user_id","level","song_id","artist_id", \
                                    "sessionID as session_id","artist_location as location","userAgent as user_agent"])
    songplays_table =  songplays_table.selectExpr(*to_keep_songplays_table)
    songplays_table = songplays_table.withColumn("year", F.year(songplays_table.start_time))
    songplays_table = songplays_table.withColumn("month", F.month(songplays_table.start_time))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year","month") \
    .parquet(output_data + 'songplays_table.parquet')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkifyade/Output/"
        
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
