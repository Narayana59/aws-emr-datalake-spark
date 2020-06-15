import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format 
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


"""
    This procedure creates spark session
"""
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark
"""
    This procedure processes a song file in spark session.
    It extracts the song information from a public S3 json files 
    and writes into another s3 bucket as parquet file after the transformation. 
    
    INPUTS: 
    * spark the spark session variable
    * input_data has the path of the s3 bucket where song json files reside
    * output_data has the path of s3 buckter where parquet files to be written after transformation
"""

def process_song_data(spark, input_data, output_data):
    song_data = input_data+'song_data/*/*/*/*.json'
    df = spark.read.json(song_data)
    songs_table=df['song_id','title','artist_id','year','duration']
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')
    artists_table =df['artist_id','artist_name','artist_location','artist_latitude','artist_longitude'] 
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')
"""
    This procedure processes a log file in spark session.
    It extracts the log information from a public S3 json files 
    and writes into another s3 bucket as parquet file after the transformation. 
    
    INPUTS: 
    * spark the spark session variable
    * input_data has the path of the s3 bucket where log json files reside
    * output_data has the path of s3 buckter where parquet files to be written after transformation
"""
    
    
def process_log_data(spark, input_data, output_data):
    log_data =input_data+'log_data/*/*/*.json'
    df = spark.read.json(log_data)
    df = df.where(df["page"]=="NextSong") 
    
    users_table = df['userId','firstName','lastName','gender','level']
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')
    
    get_timestamp = udf(lambda x:  datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn("datetime", get_datetime(df.ts))
    df = df.withColumn('hour', hour ('timestamp'))
    df = df.withColumn('day', dayofmonth ('datetime'))
    df = df.withColumn('week', weekofyear ('datetime'))
    df = df.withColumn('month', month ('datetime'))
    df = df.withColumn('year', year ('datetime'))
    df = df.withColumn('weekday', date_format('datetime', 'E'))
    time_table = df['timestamp','hour','day','week','month','year','weekday']
    time_table=time_table.dropna()
    time_table.write .partitionBy('year', 'month').parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')
    
    song_data = input_data+'song_data/*/*/*/*.json'
    song_df=spark.read.json(song_data)
    song_df=song_df['artist_name','duration','title','song_id','artist_id']
    df = df.join(song_df, (song_df.artist_name == df.artist)&(song_df.duration == df.length) & (song_df.title == df.song), "left")
    df=df.dropna()
    df = df.withColumn('songplay_id', monotonically_increasing_id())
    songplays_table =df['songplay_id','timestamp','userId','level','song_id','artist_id','sessionId','userAgent','year','month']
    songplays_table=songplays_table.dropna()
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')

    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-bucket-project1/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
