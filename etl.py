"""
Script that reads song_data and log_data from S3, 
transforms them to create five different tables, 
and writes them to partitioned parquet files in table directories on S3.
"""
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates or gets a Apache spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Load data from S3 song_data directory,
    selects the song and artist table columns
    and extracts the song and artist table in parquet format on S3 output directory.
    Spark is utilized to read,transform and write the data.
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id','title','artist_id',
                            'year','duration') \
                    .dropDuplicates()
    songs_table.createOrReplaceTempView("songs")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id") \
                    .mode("overwrite") \
                    .parquet(os.path.join(output_data, 'song_parquet'))

    # extract columns to create artists table
    artists_table = df.select('artist_id','artist_name','artist_location', \
                            'artist_latitude','artist_longitude' ) \
                    .withColumnRenamed("artist_name","name") \
                    .withColumnRenamed("artist_location","location") \
                    .withColumnRenamed("artist_latitude","latitude") \
                    .withColumnRenamed("artist_longitude","longitude") \
                    .dropDuplicates()
    
    artists_table.createOrReplaceTempView("artists")
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite") \
                    .parquet(os.path.join(output_data,'artist_parquet'))
   

def process_log_data(spark, input_data, output_data):
    """
    Load data from S3 log_data directory and extracts columns for
    users and time table,
    loads both log_data and song_data and extracts columns for
    songplays table,
    and extracts data from users, time and songplays into parquet format
    and load on S3.
    Spark is utilized to read, transform and write the data.
    
    """
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_actions = df.filter(df.page == "NextSong") \
                    .select('ts','userId','level','song','artist' \
                            ,'sessionId','location','userAgent','length')
    
    # extract columns for users table    
    users_table = df.select('userId','firstName','lastName' \
                           ,'gender','level') \
                    .withColumnRenamed('userId','user_id') \
                    .withColumnRenamed('firstName','first_name') \
                    .withColumnRenamed('lastName','last_name') \
                    .dropDuplicates()
    
    users_table.createOrReplaceTempView("users")
    
    # write users table to parquet files
    users_table.write.mode("overwrite") \
                .parquet(os.path.join(output_data,'user_parquet'))

    # create timestamp column from original timestamp column
    #convert the ts in ms to seconds 
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df_actions = df_actions.withColumn('timestamp',get_timestamp(df_actions.ts)) 
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x)/1000)))
    df_actions = df_actions.withColumn('datetime',get_datetime(df_actions.ts))
    
    df_actions.createOrReplaceTempView("logs")
    
    # extract columns to create time table
    time_table = df_actions.select('datetime') \
                            .withColumn('hour', hour('datetime')) \
                            .withColumn('day',dayofmonth('datetime')) \
                            .withColumn('week',weekofyear('datetime')) \
                            .withColumn('month',month('datetime')) \
                            .withColumn('year',year('datetime')) \
                            .withColumn('weekday', dayofweek('datetime')) \
                            .withColumnRenamed('datetime','start_time') \
                            .dropDuplicates()
    
    time_table.createOrReplaceTempView("time")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month") \
                    .mode("overwrite") \
                    .parquet(os.path.join(output_data,'time_parquet'))

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song_data/*/*/*/*.json'))
    
    song_df.createOrReplaceTempView("song_df")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT t.start_time,
            l.userId AS user_id,
            l.level as level,
            sd.song_id AS song_id,
            sd.artist_id AS artist_id,
            l.sessionId AS session_id,
            l.location AS location,
            l.userAgent AS user_agent,
            t.month AS month,
            t.year AS year
        FROM logs l
        JOIN time t
        ON l.datetime = t.start_time
        JOIN song_df sd
            ON (l.artist = sd.artist_name
            AND l.song = sd.title
            AND ABS(l.length - sd.duration) <=1)
        WHERE l.artist IS NOT NULL
        AND l.song IS NOT NULL
        AND l.length IS NOT NULL    
    """).withColumn('songplay_id',monotonically_increasing_id())
    
    songplays_table.createOrReplaceTempView('songplays')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month") \
                    .mode("overwrite") \
                    .parquet(os.path.join(output_data,"songplays_parquet"))


def main():
    """
    Creates a spark session, extracts song_data and log_data on S3, 
    processes the data using Spark, 
    and loads the data back in S3 as a set of dimensional tables.
    Output will be in parquet format
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://hk-udacitydl-projdata/"
    
    #input_data = "/home/workspace/data/"
    #output_data = "/home/workspace/data/"
        
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
