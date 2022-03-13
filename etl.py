import configparser
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, row_number
from pyspark.sql.types import TimestampType, StringType, IntegerType
from pyspark.sql.window import *

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create Spark session
    Return:
        spark: Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process song data
    Args:
        spark: Spark session
        input_data: s3 location to read data
        output_data: s3 location to write data

    Steps:
        1. Read song data from S3
        2. Create song table
        3. Create global temporary view from song table
        4. Write song table to s3 (partitioned by year and artist)
        5. Create artist table
        6. Write artist table to s3
        7. Create global temporary view from artist table

    Raises:
        ValueError: If song data is not found
    """
    # get filepath to song data file
    song_data = "{}song_data/*/*/*/*.json".format(input_data)

    # read song data file
    df = spark \
        .read \
        .json(path=song_data)

    # extract columns to create songs table
    songs_table = df \
        .select(["song_id", "title", "artist_id", "year", "duration"]) \
        .dropDuplicates()

    # Create global temp view for songs table for later use when find song id
    songs_table.createGlobalTempView("songs")

    # write songs table to parquet files partitioned by year and artist, because after partitionedBy command,
    # those partitioned columns will be removed when write to s3, so we need to clone them before doing partition.
    songs_table \
        .withColumn("_year", songs_table.year) \
        .withColumn("_artist_id", songs_table.artist_id) \
        .write.partitionBy("_year", "_artist_id").parquet("{}/songs".format(output_data))

    # extract columns to create artists table and rename them, also we have to get the latest updated time for artist
    # because their location can be changed by year.
    artists_table = df \
        .select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude", "year"]) \
        .withColumnRenamed("artist_name", "name") \
        .withColumnRenamed("artist_location", "location") \
        .withColumnRenamed("artist_latitude", "latitude") \
        .withColumnRenamed("artist_longitude", "longitude") \
        .dropDuplicates() \
        .withColumn("row_number", row_number().over(Window.partitionBy("artist_id").orderBy(col("year").desc()))) \
        .filter(col("row_number") == 1) \
        .drop("row_number", "year")

    # write artists table to parquet files
    artists_table.write.parquet("{}/artists".format(output_data))

    # Create global temp view for artists table for later use when find artist id
    artists_table.createGlobalTempView("artists")


def process_log_data(spark, input_data, output_data):
    """Process log data
    Args:
        spark: Spark session
        input_data: s3 location to read data
        output_data: s3 location to write data

    Steps:
        1. Read log data from S3
        2. Filter log dataframe by page = 'NextSong'
        3. Create user table
        4. Write user table to s3
        5. Add timestamp column to log dataframe
        6. Add datetime column to log dataframe
        7. Create udf functions to extract information about hour, day, month, year, week, weekday in timestamp column
        8. Create time table from timestamp and datetime column in log dataframe using created udf functions.
        9. Write time table to s3 and partition by year and month
        10. Get song_id, artist_id, and duration from song and artist table
        11. Create songplays table from log dataframe and song_id, artist_id, and duration
        12. Write songplays table to s3 and partition by year and month

    Raises:
        ValueError: If log data is not found
    """
    # get filepath to log data file
    log_data = "{}log_data/*/*/*.json".format(input_data)

    # read log data file
    df = spark \
        .read \
        .json(path=log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df \
        .select(["userId", "firstName", "lastName", "gender", "level", "ts"]) \
        .withColumnRenamed("userId", "user_id") \
        .withColumnRenamed("fistName", "first_name") \
        .withColumnRenamed("lastName", "last_name") \
        .dropDuplicates() \
        .withColumn("row_number", row_number().over(Window.partitionBy("user_id").orderBy(col("ts").desc()))) \
        .filter(col("row_number") == 1) \
        .drop("row_number", "ts")

    # write users table to parquet files
    users_table.write.parquet("{}/users".format(output_data))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts / 1000), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S'), StringType())
    df = df.withColumn("datetime", get_datetime(df.ts))

    # extract columns to create time table
    get_hour = udf(lambda ts: int(ts.hour), IntegerType())
    get_day = udf(lambda ts: int(ts.day), IntegerType())
    get_week = udf(lambda ts: int(ts.isocalendar()[1]), IntegerType())
    get_month = udf(lambda ts: int(ts.month), IntegerType())
    get_year = udf(lambda ts: int(ts.year), IntegerType())
    get_weekday = udf(lambda ts: int(ts.weekday()), IntegerType())

    time_table = df \
        .withColumn("hour", get_hour(df.timestamp)) \
        .withColumn("day", get_day(df.timestamp)) \
        .withColumn("week", get_week(df.timestamp)) \
        .withColumn("month", get_month(df.timestamp)) \
        .withColumn("year", get_year(df.timestamp)) \
        .withColumn("weekday", get_weekday(df.timestamp)) \
        .select(["datetime", "hour", "day", "week", "month", "year", "weekday"]) \
        .dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table \
        .withColumn("_year", time_table.year) \
        .withColumn("_month", time_table.month) \
        .write.partitionBy("_year", "_month").parquet("{}/time".format(output_data))

    # read in song data to use for songplays table
    song_df = spark.sql("""
        select distinct s.song_id, s.artist_id, s.title, a.name, s.duration
        from global_temp.songs s inner join global_temp.artists a
        on (s.artist_id = a.artist_id)
    """)

    # extract columns from joined song and log datasets to create songplays table, use row_number() with no partition
    # to add incremental id to each row
    songplays_table = df.alias("logs") \
        .join(song_df.alias("song"), [song_df.name == df.artist,
                                      song_df.title == df.song], "inner") \
        .join(time_table.alias("time"), [df.datetime == time_table.datetime], "inner") \
        .select(["logs.datetime", "logs.userId", "logs.level", "song.song_id",
                 "song.artist_id", "logs.sessionId", "logs.location", "logs.userAgent", "time.month", "time.year",
                 "logs.ts"]) \
        .dropDuplicates() \
        .withColumn("songplay_id", row_number().over(Window.partitionBy().orderBy(df.ts))) \
        .withColumnRenamed("datetime", "start_time") \
        .withColumnRenamed("userId", "user_id") \
        .withColumnRenamed("userAgent", "user_agent") \
        .withColumnRenamed("sessionId", "session_id") \
        .drop("ts")

    # write songplays table to parquet files partitioned by year and month, use the same clone techinque
    # as songs table to partition data when writing to s3
    songplays_table \
        .withColumn("_year", songplays_table.year) \
        .withColumn("_month", songplays_table.month) \
        .write.partitionBy("_year", "_month").parquet("{}/songplays".format(output_data))


def main():
    """Main function to run ETL pipeline
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://minhha4-de-udacity-datalake"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
