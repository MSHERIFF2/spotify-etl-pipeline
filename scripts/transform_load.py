from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def run_transform_load():
    spark = SparkSession.builder.appName("SpotifyETL").getOrCreate()
    
    # Load
    df = spark.read.option("header", "true").csv("data/raw/raw_spotify_data.csv")

    # Transform: Create Star Schema
    dim_artists = df.select("artist_id", "artist_name", "artist_country").distinct()
    dim_songs = df.select("song_id", "song_title", "song_genre").distinct()
    fact_streams = df.select("stream_id", "date", "artist_id", "song_id", "device_type")

    # Load to Parquet
    dim_artists.write.mode("overwrite").parquet("data/gold/dim_artists")
    dim_songs.write.mode("overwrite").parquet("data/gold/dim_songs")
    fact_streams.write.mode("overwrite").parquet("data/gold/fact_streams")
    
    print("✅ Transformation Complete: Star Schema saved as Parquet.")
    spark.stop()

if __name__ == "__main__":
    run_transform_load()