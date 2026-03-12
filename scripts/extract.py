import csv
import random
import os
from datetime import datetime, timedelta

def run_extraction():
    os.makedirs('data/raw', exist_ok=True)
    output_path = "data/raw/raw_spotify_data.csv"
    
    artists = [(101, "The Weeknd", "Canada"), (103, "Burna Boy", "Nigeria"), (104, "BTS", "Korea")]
    songs = [(1, "Blinding Lights", "Pop"), (4, "Last Last", "Afrobeats"), (5, "Dynamite", "K-Pop")]

    with open(output_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["stream_id", "date", "artist_id", "artist_name", "artist_country", 
                         "song_id", "song_title", "song_genre", "device_type"])

        for i in range(1000):
            artist = random.choice(artists)
            song = random.choice(songs)
            writer.writerow([i, datetime.now().strftime("%Y-%m-%d"), artist[0], artist[1], 
                             artist[2], song[0], song[1], song[2], random.choice(["Mobile", "Web"])])
    print(f"✅ Data extracted to {output_path}")

if __name__ == "__main__":
    run_extraction()