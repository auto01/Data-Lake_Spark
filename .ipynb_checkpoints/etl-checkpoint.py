import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col
import os
from datetime import datetime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,dayofweek,monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):

    """
    This function extracts data from songs data files
    and load data two different table named : songs , artists

    Parameters:
    
    spark: its a spark session
    input_data: path for input data directory
    output_data: path to store newly created tables with data in given format(ie.Parquet)

    """



    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    
    # read song data file
    df = spark.read.json(song_data)


    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id',
                            'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id') \
                     .parquet(os.path.join(output_data, 'songs/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location',
                              'artist_latitude', 'artist_longitude') \
                      .withColumnRenamed('artist_name', 'name') \
                      .withColumnRenamed('artist_location', 'location') \
                      .withColumnRenamed('artist_latitude', 'latitude') \
                      .withColumnRenamed('artist_longitude', 'longitude') \
                      .dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    
    """
    This function extracts data from logs dataset,songs dataset
    then perform transform and load data three different table named : users , time,songplays .

    In order to fill data into songplays table ,it requires data from both data files (logs,songs)
    so it first join both dataframes based on matching coloumn and then store resultant
    records to songplays.

    Parameters:
    spark: its a spark session
    input_data: path for input data directory
    output_data: path to store newly created tables with data in given format(ie.Parquet)

    """

    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    actions_df = df.filter(df.page == 'NextSong') \
                   .select('ts', 'userId', 'level', 'song', 'artist',
                           'sessionId', 'location', 'userAgent')

    # extract columns for users table    
    users_table = df.select('userId','firstName','lastName','gender','level').dropDuplicates()


    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')



    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    actions_df = actions_df.withColumn('timestamp', get_timestamp(actions_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime=udf(lambda x:str(datetime.fromtimestamp(int(x)/1000)))
    actions_df=actions_df.withColumn('datetime',get_datetime(df.ts))

    
    # extract columns to create time table
    time_table = actions_df.select('datetime')\
    .withColumn('start_time',time_cols.datetime)\
    .withColumn('hour',hour('datetime'))\
    .withColumn('day',dayofmonth('datetime'))\
    .withColumn('week',weekofyear('datetime'))\
    .withColumn('month',month('datetime'))\
    .withColumn('year',year('datetime'))\
    .withColumn('weekday',dayofweek('datetime'))\
    
    time_table=time_table.select('start_time','hour','day','week','month','year','weekday')

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
                    .parquet(os.path.join(output_data,'time/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')
    song_df=song_df.alias('song_df')
    log_df=actions_df.alias('log_df')

    joined_df = log_df.join(song_df, col('log_df.artist') == col('song_df.artist_name'), 'inner')    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = joined_df.select(
        col('log_df.datetime').alias('start_time'),
        col('log_df.userId').alias('user_id'),
        col('log_df.level').alias('level'),
        col('song_df.song_id').alias('song_id'),
        col('song_df.artist_id').alias('artist_id'),
        col('log_df.sessionId').alias('session_id'),
        col('log_df.location').alias('location'), 
        col('log_df.userAgent').alias('user_agent'),
        year('log_df.datetime').alias('year'),
        month('log_df.datetime').alias('month')) \
        .withColumn('songplay_id',monotonically_increasing_id())
        

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(os.path.join(output_path,'songplays/songplays.parquet','overwrite'))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://pk-loaded-data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()



